#!/bin/sh
# Build a .deb of rnfs: packaging/build-deb.sh [--arch amd64|arm64] [--wheel PATH] [--out DIR]
#
# The package installs:
#   /opt/rnfs/lib/              rnfs and its dependencies (rns, lxmf, pyserial, cryptography, cffi)
#   /opt/rnfs/bin/rnfs          launcher: the system python3 in isolated mode (-I) with /opt/rnfs/lib first
#   /usr/bin/rnfs               symlink to it
#   /usr/lib/systemd/user/rnfs.service   systemd user unit (systemctl --user enable --now rnfs)
#
# Why not a venv: a venv is tied to the exact python3.X it was built with (lib/python3.X, bin/python ->
# python3.X), so a package built on one distro would break on another, or after a Python upgrade. Instead
# the dependencies are installed with `pip install --target` from wheels for CPython 3.11 and manylinux_2_28
# (glibc >= 2.28): everything is pure Python except cryptography (an abi3 wheel, fine on any CPython >= 3.11)
# and cffi, whose compiled backend is bundled once per supported Python (3.11 to 3.14; the files are named per
# version, so they coexist). Because of those compiled wheels the package is per architecture (amd64, arm64),
# not "all". It's built with wheels downloaded for the target platform, so an arm64 package can be built on
# an amd64 machine. The dependencies are the exact versions and files pinned in packaging/requirements.lock
# (pip --require-hashes).
#
# Needs: python3 with pip (or a venv's python, via $PYTHON), dpkg-deb, network access to PyPI.
set -eu
umask 022

REPO=$(cd "$(dirname "$0")/.." && pwd)
LOCK="$REPO/packaging/requirements.lock"
PYTHON=${PYTHON:-python3}
ARCH=$(dpkg --print-architecture 2>/dev/null || echo amd64)
WHEEL=""
OUT="$REPO/dist"
MAINTAINER=${DEB_MAINTAINER:-"rnfs developers <rnfs@users.noreply.github.com>"}
EXTRA_PYTHONS="3.12 3.13 3.14"  # besides 3.11, the Pythons whose cffi backend is bundled

while [ $# -gt 0 ]; do
    case "$1" in
        --arch) ARCH=$2; shift 2 ;;
        --wheel) WHEEL=$2; shift 2 ;;
        --out) OUT=$2; shift 2 ;;
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) echo "build-deb.sh: unknown option $1" >&2; exit 2 ;;
    esac
done

case "$ARCH" in
    amd64) MACHINE=x86_64 ;;
    arm64) MACHINE=aarch64 ;;
    *) echo "build-deb.sh: unsupported architecture $ARCH (amd64 or arm64)" >&2; exit 2 ;;
esac

VERSION=$(sed -n 's/^__version__ = ["'\'']\([^"'\'']*\)["'\''].*/\1/p' "$REPO/rnfs/__init__.py")
[ -n "$VERSION" ] || { echo "build-deb.sh: can't read __version__ from rnfs/__init__.py" >&2; exit 1; }

WORK=$(mktemp -d "${TMPDIR:-/tmp}/rnfs-deb.XXXXXX")
trap 'rm -rf "$WORK"' EXIT INT TERM
# Run pip (-I) from the work directory, so modules in the caller's directory or PYTHONPATH aren't imported
case "$WHEEL" in /*|"") ;; *) WHEEL="$(pwd)/$WHEEL" ;; esac
case "$PYTHON" in /*) ;; */*) PYTHON="$(pwd)/$PYTHON" ;; esac
case "$OUT" in /*) ;; *) OUT="$(pwd)/$OUT" ;; esac
cd "$WORK"

if [ -z "$WHEEL" ]; then
    echo "Building the rnfs wheel"
    # From a copy, so the build leaves no build/ or *.egg-info in the source tree
    mkdir -p "$WORK/src"
    cp -R "$REPO/pyproject.toml" "$REPO/README.md" "$REPO/MANIFEST.in" "$REPO/rnfs" "$WORK/src/"
    for license in "$REPO"/LICENSE* "$REPO"/LICENCE* "$REPO"/COPYING*; do
        if [ -f "$license" ]; then cp "$license" "$WORK/src/"; fi
    done
    find "$WORK/src" -name __pycache__ -type d -prune -exec rm -rf {} +
    "$PYTHON" -I -m pip wheel --quiet --no-deps --disable-pip-version-check --wheel-dir "$WORK/wheel" "$WORK/src"
    WHEEL=$(ls "$WORK"/wheel/rnfs-*.whl)
fi

PLATFORMS="--platform manylinux_2_28_$MACHINE --platform manylinux_2_17_$MACHINE --platform manylinux2014_$MACHINE"
ROOT="$WORK/root"
LIB="$ROOT/opt/rnfs/lib"

echo "Installing rnfs $VERSION and its dependencies (from packaging/requirements.lock) for $ARCH into /opt/rnfs/lib"
# The lock plus the rnfs wheel with its own hash: pip's hash-checking mode then installs nothing unpinned
cp "$LOCK" "$WORK/requirements.txt"
echo "rnfs @ file://$WHEEL --hash=sha256:$(sha256sum "$WHEEL" | cut -d' ' -f1)" >> "$WORK/requirements.txt"
# shellcheck disable=SC2086
"$PYTHON" -I -m pip install --quiet --no-compile --disable-pip-version-check --target "$LIB" \
    --only-binary=:all: --implementation cp --python-version 3.11 $PLATFORMS --require-hashes -r "$WORK/requirements.txt"

# Just cffi's lines from the lock, for downloading its wheels for the other Pythons
awk '/^cffi==/ { p = 1 } p { print; if ($0 !~ /\\$/) p = 0 }' "$LOCK" > "$WORK/cffi.lock"
grep -q '^cffi==' "$WORK/cffi.lock" || { echo "build-deb.sh: cffi isn't in $LOCK" >&2; exit 1; }

for version in $EXTRA_PYTHONS; do
    nodot=$(echo "$version" | tr -d .)
    # shellcheck disable=SC2086
    if "$PYTHON" -I -m pip download --quiet --no-deps --disable-pip-version-check --dest "$WORK/cffi$nodot" \
            --only-binary=:all: --implementation cp --python-version "$version" $PLATFORMS \
            --require-hashes -r "$WORK/cffi.lock" 2>/dev/null; then
        "$PYTHON" -I - "$WORK/cffi$nodot" "$LIB" <<'EOF'
import glob, sys, zipfile
wheel, = glob.glob(sys.argv[1] + "/cffi-*.whl")
with zipfile.ZipFile(wheel) as z:
    for name in z.namelist():
        if name.startswith("_cffi_backend."):
            z.extract(name, sys.argv[2])
            print("  added", name)
EOF
    else
        echo "  no cffi wheel for Python $version on $ARCH; skipped"
    fi
done

# Console scripts pip made for the build machine's Python (rnsd, lxmd, ...): not part of this package
rm -rf "${LIB:?}/bin"
find "$LIB" -name __pycache__ -type d -prune -exec rm -rf {} +

mkdir -p "$ROOT/opt/rnfs/bin" "$ROOT/usr/bin" "$ROOT/usr/lib/systemd/user" "$ROOT/usr/share/doc/rnfs" "$ROOT/DEBIAN"
cat > "$ROOT/opt/rnfs/bin/rnfs" <<'EOF'
#!/usr/bin/python3 -I
"""rnfs, from the bundled libraries in /opt/rnfs/lib (isolated from user and system site-packages)."""
import os
import sys

# ../lib next to this script's real location (/usr/bin/rnfs is a symlink to it)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "lib"))
from rnfs.cli import main  # noqa: E402

sys.exit(main())
EOF
chmod 0755 "$ROOT/opt/rnfs/bin/rnfs"
ln -s /opt/rnfs/bin/rnfs "$ROOT/usr/bin/rnfs"

sed -e 's|^ExecStart=.*|ExecStart=/usr/bin/rnfs daemon|' \
    -e '/^# ExecStart assumes/d' \
    -e '/^# double quotes, and write a literal %/d' \
    -e 's|^#   cp contrib/rnfs.service ~/.config/systemd/user/$|#   (installed by the rnfs package as /usr/lib/systemd/user/rnfs.service)|' \
    -e '/^#   mkdir -p ~\/.config\/systemd\/user$/d' \
    "$REPO/contrib/rnfs.service" > "$ROOT/usr/lib/systemd/user/rnfs.service"
chmod 0644 "$ROOT/usr/lib/systemd/user/rnfs.service"

{
    echo "Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/"
    echo "Upstream-Name: rnfs"
    echo "Source: https://github.com/afit21/rnfs"
    echo
    echo "Files: *"
    echo "Comment: rnfs (its license text follows, if the source tree has one). The bundled third-party"
    echo " packages in /opt/rnfs/lib (RNS, LXMF, pyserial, cryptography, cffi, pycparser) carry their own licenses in their *.dist-info directories there."
    for license in "$REPO"/LICENSE "$REPO"/LICENSE.* "$REPO"/LICENCE "$REPO"/COPYING; do
        if [ -f "$license" ]; then
            echo "License: other"
            sed 's/^$/./; s/^/ /' "$license"
            break
        fi
    done
} > "$ROOT/usr/share/doc/rnfs/copyright"

cat > "$ROOT/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
if [ "$1" = configure ]; then
    # Byte-compile for the installed python3, so starting rnfs doesn't recompile on every run
    python3 -m compileall -q -j 0 /opt/rnfs/lib >/dev/null 2>&1 || true
fi
exit 0
EOF
cat > "$ROOT/DEBIAN/prerm" <<'EOF'
#!/bin/sh
set -e
# Remove the bytecode postinst made; dpkg removes everything else
find /opt/rnfs/lib -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
exit 0
EOF
chmod 0755 "$ROOT/DEBIAN/postinst" "$ROOT/DEBIAN/prerm"

INSTALLED_SIZE=$(du -sk --exclude=DEBIAN "$ROOT" | cut -f1)
cat > "$ROOT/DEBIAN/control" <<EOF
Package: rnfs
Version: $VERSION
Architecture: $ARCH
Maintainer: $MAINTAINER
Installed-Size: $INSTALLED_SIZE
Depends: python3 (>= 3.11), libc6 (>= 2.28)
Section: net
Priority: optional
Homepage: https://github.com/afit21/rnfs
Description: content-addressed file sharing over Reticulum
 rnfs shares files over Reticulum (RNS) networks, including LoRa and other
 low-bandwidth links, with the functional features of IPFS: content
 addressing, a DHT for finding providers, pinning, mutable names and an
 HTTP gateway.
 .
 Reticulum, LXMF and their Python dependencies are bundled in /opt/rnfs.
 A systemd user unit is included: systemctl --user enable --now rnfs
EOF

mkdir -p "$OUT"
DEB="$OUT/rnfs_${VERSION}_${ARCH}.deb"
dpkg-deb --root-owner-group --build "$ROOT" "$DEB" >/dev/null
echo "Built $DEB"
