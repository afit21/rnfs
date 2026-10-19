"""Regenerate the pinned Python wheels the Flatpak manifest installs (no network access during a Flatpak build).

    python3 -I packaging/flatpak/generate-sources.py

Writes, next to this script:
  python3-deps.json   a flatpak-builder module installing rnfs's runtime dependencies (rns and its own)
                      from PyPI wheels, for x86_64 and aarch64 (only-arches picks the right compiled wheels)
  build-sources.json  the setuptools wheel pip needs to build rnfs itself, offline

The requirements come from pyproject.toml (project.dependencies, plus the optional dependencies except
development extras) at the exact versions in packaging/requirements.lock, and every wheel's sha256 must be
one the lock lists, so the output only changes when the lock (or packaging/build-backend.lock, for
build-sources.json) does. The tests
workflow re-runs this and fails if the result differs from what's committed. Needs pip >= 23 (for
`--dry-run --report`) and network access.
"""
import json
import os
import subprocess
import sys
import tempfile
import tomllib

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
# The runtime's Python (org.freedesktop.Platform 25.08: CPython 3.13). Compiled wheels (cffi) are fetched for
# its neighbours too, so the build still works if the runtime's Python differs; pip installs the one that fits
RUNTIME_PYTHONS = ["3.12", "3.13", "3.14"]
ARCHES = {"x86_64": "x86_64", "aarch64": "aarch64"}  # flatpak arch: manylinux machine name
LOCK = os.path.join(REPO, "packaging", "requirements.lock")
# The build backend: the setuptools every other build uses (its hash is checked here and by flatpak-builder)
BACKEND_LOCK = os.path.join(REPO, "packaging", "build-backend.lock")
DEVELOPMENT_EXTRAS = {"dev", "test", "tests", "docs", "lint"}  # extras not bundled


def requirements() -> list[str]:
    """rnfs's runtime requirements from pyproject.toml, optional ones included."""
    with open(os.path.join(REPO, "pyproject.toml"), "rb") as f:
        project = tomllib.load(f)["project"]
    reqs = list(project.get("dependencies", []))
    for name, extra in project.get("optional-dependencies", {}).items():
        if name not in DEVELOPMENT_EXTRAS:
            reqs += [r for r in extra if r not in reqs]
    return reqs


def read_lock(path: str = LOCK) -> dict[str, tuple[str, set[str]]]:
    """{name: (version, {sha256, ...})} from requirements.lock (or another hash lock)."""
    with open(path, encoding="utf-8") as f:
        text = f.read().replace("\\\n", " ")
    lock = {}
    for line in text.splitlines():
        words = line.split("#", 1)[0].split()
        if not words:
            continue
        name, _, version = words[0].partition("==")
        hashes = {w.split(":", 1)[1] for w in words[1:] if w.startswith("--hash=sha256:")}
        if not version or not hashes:
            raise SystemExit(f"generate-sources.py: {path}: not a pinned, hashed requirement: {words[0]}")
        lock[name.lower()] = (version, hashes)
    return lock


def check_locked(wheels: list[dict], lock: dict[str, tuple[str, set[str]]]) -> None:
    """Exit unless every wheel is a version and file the lock (requirements.lock, ...) pins."""
    for wheel in wheels:
        name = wheel["name"].lower()
        if name not in lock:
            raise SystemExit(f"generate-sources.py: {name} isn't in the lock; run packaging/update-lock.py")
        if wheel["sha256"] not in lock[name][1]:
            raise SystemExit(f"generate-sources.py: {wheel['url']} isn't a file the lock pins for {name}")


def resolve(reqs: list[str], machine: str | None, python: str, pins: list[str] = ()) -> list[dict]:
    """[{name, url, sha256}] of the wheels pip picks for reqs (constrained to pins) on this Python and machine."""
    platforms = []
    if machine:
        for tag in ("manylinux_2_28", "manylinux_2_17", "manylinux2014"):
            platforms += ["--platform", f"{tag}_{machine}"]
    with tempfile.TemporaryDirectory() as tmp:
        constraints = os.path.join(tmp, "constraints.txt")
        with open(constraints, "w", encoding="utf-8") as f:
            f.write("".join(pin + "\n" for pin in pins))
        report = os.path.join(tmp, "report.json")
        # -I and cwd=tmp: pip must not import modules from the caller's directory
        subprocess.run(
            [sys.executable, "-I", "-m", "pip", "install", "--quiet", "--disable-pip-version-check", "--dry-run",
             "--ignore-installed", "--report", report, "--target", os.path.join(tmp, "target"),
             "--only-binary=:all:", "--implementation", "cp", "--python-version", python,
             "-c", constraints, *platforms, *reqs],
            check=True, cwd=tmp)
        with open(report, encoding="utf-8") as f:
            items = json.load(f)["install"]
    return [{"name": item["metadata"]["name"], "url": item["download_info"]["url"],
             "sha256": item["download_info"]["archive_info"]["hashes"]["sha256"]} for item in items]


def main() -> None:
    """Write python3-deps.json and build-sources.json."""
    reqs = requirements()
    lock = read_lock()
    pins = [f"{name}=={version}" for name, (version, _) in sorted(lock.items())]
    sources: dict[str, dict] = {}  # url: source
    for arch, machine in ARCHES.items():
        for python in RUNTIME_PYTHONS:
            wheels = resolve(reqs, machine, python, pins)
            check_locked(wheels, lock)
            for wheel in wheels:
                source = sources.setdefault(wheel["url"], {"type": "file", "url": wheel["url"],
                                                           "sha256": wheel["sha256"], "only-arches": []})
                if arch not in source["only-arches"]:
                    source["only-arches"].append(arch)
    for source in sources.values():
        if sorted(source["only-arches"]) == sorted(ARCHES):
            del source["only-arches"]  # pure Python: every arch
    module = {
        "name": "python3-deps",
        "buildsystem": "simple",
        "build-commands": [
            'pip3 install --verbose --no-index --find-links="file://${PWD}" --prefix=${FLATPAK_DEST} '
            '--no-build-isolation ' + " ".join(f'"{r}"' for r in reqs),
        ],
        "sources": sorted(sources.values(), key=lambda s: s["url"].rsplit("/", 1)[-1]),
    }
    backend = read_lock(BACKEND_LOCK)
    build_wheels = resolve([f"{name}=={version}" for name, (version, _) in sorted(backend.items())], None,
                           RUNTIME_PYTHONS[0])
    check_locked(build_wheels, backend)
    build_sources = [{"type": "file", "url": w["url"], "sha256": w["sha256"]} for w in build_wheels]
    for name, data in (("python3-deps.json", module), ("build-sources.json", build_sources)):
        with open(os.path.join(HERE, name), "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        print(f"wrote {name}")
    print("requirements:", ", ".join(reqs))


if __name__ == "__main__":
    main()
