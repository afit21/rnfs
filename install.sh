#!/bin/sh
# Install, update or uninstall rnfs's latest release (or its main branch) from GitHub, in its own Python
# virtual environment.
#
#   curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --service
#   sh install.sh [--system] [--service] [--main] [--update | --uninstall]
#
# Per user (the default):   venv ~/.local/share/rnfs/venv, command ~/.local/bin/rnfs
# --system (or as root):    venv /opt/rnfs/venv,           command /usr/local/bin/rnfs
#
#   --service     also install the systemd user unit and start it (system mode: install it for all users,
#                 each then runs `systemctl --user enable --now rnfs`)
#   --main        install the main branch, which may be newer than the latest release, instead of the release
#   --update      re-install from the latest release, or the main branch with --main (running the installer
#                 again does the same)
#   --uninstall   remove the venv, the command and the unit (keeps your data in ~/.rnfs)
#   --python P    use this Python (3.11 or newer) for the venv
#   --force       replace an existing rnfs command or systemd unit that this installer didn't make
#                 (e.g. a pipx one)
#
# Environment: RNFS_SOURCE  what to install instead of the latest release (overrides --main too): a local
#                           source directory, a .zip/.tar.gz/.whl path or URL, or another pip requirement
#              RNFS_PYTHON  same as --python
#              RNFS_ALLOW_SOURCE_BUILDS=1  build dependencies PyPI has no wheel of for this platform (e.g.
#                           cffi on 32-bit ARM) from their pinned, hash-checked source; the tools that build
#                           them are then fetched unpinned, and a C compiler and Python headers are needed
#
# Dependencies (rns, lxmf, cryptography, ...) are installed as wheels at the versions and sha256 hashes
# pinned in the source's packaging/requirements.lock (pip --require-hashes --only-binary=:all:), and rnfs
# itself is built with the setuptools pinned in packaging/build-backend.lock, in a separate build venv
# without pip's build isolation (which would fetch the newest setuptools). A source without those files (a
# .whl, a pip requirement) gets the newest versions instead, with a warning.
# Needs Python 3.11+ with venv and pip (no git). Your rnfs data (~/.rnfs) and Reticulum config (~/.reticulum)
# are never touched.
set -eu

REPO_URL="https://github.com/afit21/rnfs"
# Without RNFS_SOURCE, this script (as served from the main branch) installs the latest release's source tree,
# which may be older than the script. It must keep working with the layout of every release from 0.1.1 on:
# packaging/requirements.lock and packaging/build-backend.lock, which fetch_source looks for.
SOURCE=${RNFS_SOURCE:-}  # empty: chosen by choose_source once Python is found
MAIN=no
PYTHON=${RNFS_PYTHON:-}
PYTHON_GIVEN=no
MODE=user
ACTION=install
SERVICE=no
FORCE=no

say() { printf '%s\n' "$*"; }
warn() { printf 'rnfs installer: warning: %s\n' "$*" >&2; }
die() { printf 'rnfs installer: %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --system) MODE=system ;;
        --service) SERVICE=yes ;;
        --update) ACTION=install ;;
        --main) MAIN=yes ;;
        --uninstall) ACTION=uninstall ;;
        --force) FORCE=yes ;;
        --python) [ $# -ge 2 ] || die "--python needs a path"; PYTHON=$2; shift ;;
        --python=*) PYTHON=${1#--python=} ;;
        -h|--help)
            if [ -f "$0" ]; then sed -n '2,/^set -eu$/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'
            else say "See https://github.com/afit21/rnfs/blob/main/install.sh"; fi
            exit 0 ;;
        *) die "unknown option $1 (try --help)" ;;
    esac
    shift
done

abs_path() {  # abs_path PATH: PATH (which exists) as an absolute path
    if [ -d "$1" ]; then
        (CDPATH='' cd -P -- "$1" && pwd -P)
    else
        dir=$(CDPATH='' cd -P -- "$(dirname -- "$1")" && pwd -P) || return 1
        printf '%s/%s\n' "${dir%/}" "$(basename -- "$1")"
    fi
}

# Make the caller's relative paths absolute, then leave the caller's directory: Python puts the current
# directory first on sys.path for `python -m`/`-c`, so a venv.py or pip/ there would run with our rights (root,
# with --system). Every Python call below also uses -I, which leaves it (and PYTHON* variables) out.
if [ -e "$SOURCE" ]; then SOURCE=$(abs_path "$SOURCE") || die "can't resolve $SOURCE"; fi
case "$PYTHON" in
    /*|"") ;;
    */*) if [ -e "$PYTHON" ]; then PYTHON=$(abs_path "$PYTHON") || die "can't resolve $PYTHON"; fi ;;
esac
case "${TMPDIR:-}" in
    /*|"") ;;
    *) if [ -d "$TMPDIR" ]; then TMPDIR=$(abs_path "$TMPDIR"); else unset TMPDIR; fi ;;
esac
case "${HOME:-/}" in /*) ;; *) die "HOME isn't an absolute path: $HOME" ;; esac
# The XDG base directory spec says to ignore relative values
case "${XDG_DATA_HOME:-/}" in /*) ;; *) unset XDG_DATA_HOME ;; esac
case "${XDG_CONFIG_HOME:-/}" in /*) ;; *) unset XDG_CONFIG_HOME ;; esac
cd /

if [ "$MODE" = user ] && [ "$(id -u)" = 0 ]; then
    MODE=system
fi

if [ "$MODE" = system ]; then
    [ "$(id -u)" = 0 ] || die "--system needs root: run it with sudo (e.g. curl ... | sudo sh -s -- --system)"
    # Everything made here must be readable by every user (root's umask may be 077)
    umask 022
    PREFIX=/opt/rnfs
    VENV=$PREFIX/venv
    BIN_DIR=/usr/local/bin
    UNIT_DIR=/etc/systemd/user
    # pip won't use the invoking user's cache as root (sudo -E) and warns about it; root's own isn't needed
    PIP_OPTS="--disable-pip-version-check --no-cache-dir"
else
    [ -n "${HOME:-}" ] || die "HOME isn't set"
    PREFIX=${XDG_DATA_HOME:-$HOME/.local/share}/rnfs
    VENV=$PREFIX/venv
    BIN_DIR=$HOME/.local/bin
    UNIT_DIR=${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user
fi
LAUNCHER=$BIN_DIR/rnfs
UNIT=$UNIT_DIR/rnfs.service
PIP_OPTS=${PIP_OPTS:-"--disable-pip-version-check"}

# Python and venv checks

python_ok() {  # python_ok CMD: CMD is Python 3.11 or newer
    [ "$("$1" -I -c 'import sys; print(sys.version_info >= (3, 11))' 2>/dev/null)" = True ]
}

python_version() { "$1" -I -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null; }

find_python() {
    if [ -n "$PYTHON" ]; then
        PYTHON_GIVEN=yes
        command -v "$PYTHON" >/dev/null 2>&1 || die "Python not found: $PYTHON"
        python_ok "$PYTHON" || die "$PYTHON is older than Python 3.11, which rnfs needs"
        return
    fi
    for candidate in python3 python3.14 python3.13 python3.12 python3.11; do
        if command -v "$candidate" >/dev/null 2>&1 && python_ok "$candidate"; then
            PYTHON=$candidate
            return
        fi
    done
    if command -v python3 >/dev/null 2>&1; then
        found=$(python3 -I -c 'import platform; print(platform.python_version())' 2>/dev/null || echo unknown)
        die "rnfs needs Python 3.11 or newer, and python3 is $found.
Install a newer Python, then run this again (with --python /path/to/python3.X if it isn't python3):
  Debian/Ubuntu:  sudo apt install python3.12 python3.12-venv   (or the newest python3.X your release has)
  Fedora:         sudo dnf install python3.12
  Arch:           sudo pacman -S python"
    fi
    die "Python 3 not found. Install Python 3.11 or newer with venv, then run this again:
  Debian/Ubuntu:  sudo apt install python3 python3-venv
  Fedora:         sudo dnf install python3
  Arch:           sudo pacman -S python
  openSUSE:       sudo zypper install python3"
}

check_venv() {  # Debian and Ubuntu ship venv's pip bootstrap (ensurepip) as a separate package
    probe=$(mktemp -d "${TMPDIR:-/tmp}/rnfs-venv-check.XXXXXX")
    if ! "$PYTHON" -I -m venv "$probe/venv" >/dev/null 2>&1; then
        rm -rf "$probe"
        version=$(python_version "$PYTHON")
        die "$PYTHON can't create virtual environments (venv or ensurepip is missing). Install it, then run this again:
  Debian/Ubuntu:  sudo apt install python3-venv   (or python$version-venv)
  Fedora:         sudo dnf install python3        (venv is included; python3-pip if pip is missing)
  Arch:           sudo pacman -S python"
    fi
    rm -rf "$probe"
}

# shellcheck disable=SC2086  # PIP_OPTS is a list of options
pip_in() { py=$1; shift; "$py" -I -m pip $PIP_OPTS "$@"; }  # pip_in PYTHON ARGS...: that Python's pip
vpip() { pip_in "$VENV/bin/python" "$@"; }

venv_ok() {  # the venv exists, works, and uses the Python asked for with --python (if any)
    [ -x "$VENV/bin/python" ] && python_ok "$VENV/bin/python" &&
        { [ "$PYTHON_GIVEN" = no ] || [ "$(python_version "$VENV/bin/python")" = "$(python_version "$PYTHON")" ]; }
}

# The launcher and the systemd unit

ours() {  # ours PATH: PATH is a symlink this installer made (pointing into our venv)
    [ -L "$1" ] && [ "$(readlink "$1")" = "$VENV/bin/rnfs" ]
}

check_launcher() {  # fail early if the command's path is taken by something else (unless --force)
    if { [ -e "$LAUNCHER" ] || [ -L "$LAUNCHER" ]; } && ! ours "$LAUNCHER" && [ "$FORCE" = no ]; then
        die "$LAUNCHER already exists and wasn't installed by this script (pipx? a package?).
Remove it (e.g. pipx uninstall rnfs) or run this again with --force to replace it."
    fi
}

UNIT_MARK="installed by rnfs's install.sh"  # in the unit's first line (installs before this check have it too)

unit_ours() {  # the unit is a file this installer wrote
    [ -f "$UNIT" ] && ! [ -L "$UNIT" ] && grep -qF "$UNIT_MARK" "$UNIT"
}

check_unit() {  # fail early if --service would replace a unit that this installer didn't write (unless --force)
    if [ "$SERVICE" = yes ] && { [ -e "$UNIT" ] || [ -L "$UNIT" ]; } && ! unit_ours && [ "$FORCE" = no ]; then
        die "$UNIT already exists and wasn't written by this script.
Remove it, or run this again with --force to replace it."
    fi
}

install_launcher() {
    mkdir -p "$BIN_DIR"
    if ! ours "$LAUNCHER"; then rm -f "$LAUNCHER"; fi
    ln -sf "$VENV/bin/rnfs" "$LAUNCHER"
}

systemd_quote() {  # systemd_quote PATH: PATH as ExecStart='s command word: in double quotes, with % doubled
    # systemd expands %-specifiers in it (but not $VARIABLES: those only in the arguments)
    printf '"%s"' "$(printf '%s' "$1" | sed -e 's/%/%%/g')"
}

write_unit() {
    # systemd refuses a command path with quotes, backslashes or control characters in it
    case "$LAUNCHER" in
        *[\\\"\'[:cntrl:]]*) die "systemd can't run a command whose path contains a quote, backslash or control character: $LAUNCHER" ;;
    esac
    mkdir -p "$UNIT_DIR"
    if ! unit_ours; then rm -f "$UNIT"; fi  # (--force) a symlink or someone else's unit: replace, don't write through
    cat > "$UNIT" <<EOF
# systemd user unit for the rnfs daemon, $UNIT_MARK.
#   systemctl --user enable --now rnfs
#   journalctl --user -u rnfs -f          # logs
# To keep it running while you're logged out: loginctl enable-linger \$USER

[Unit]
Description=rnfs: content-addressed file sharing over Reticulum
# Start after rnsd if it also runs as a user service, so rnfs attaches to it rather than starting its own instance
After=rnsd.service
# Give up after 5 failed starts in 10 minutes (e.g. the command is missing) instead of retrying forever
StartLimitIntervalSec=600
StartLimitBurst=5

[Service]
Type=simple
ExecStart=$(systemd_quote "$LAUNCHER") daemon
Restart=on-failure
RestartSec=10
# The daemon shuts down cleanly on SIGTERM, saving provider records and RNS state
KillSignal=SIGTERM
TimeoutStopSec=30
# Sandboxing that works in a user unit too. Serial devices (RNode, KISS) are unaffected; Bluetooth RNodes are
# reached through BlueZ's D-Bus (AF_UNIX). An interface program run by a PipeInterface inherits these limits:
# if it needs another socket family (e.g. AF_PACKET), add it with systemctl --user edit rnfs.
NoNewPrivileges=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK
LockPersonality=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes

[Install]
WantedBy=default.target
EOF
}

latest_release() {  # the tag of the latest release on GitHub: where releases/latest redirects (no API, no rate limit)
    "$PYTHON" -I - "$REPO_URL" <<'EOF'
import re, sys, urllib.parse, urllib.request

repo = sys.argv[1]
try:
    with urllib.request.urlopen(repo + "/releases/latest", timeout=60) as response:
        final = response.geturl()
except OSError as e:
    sys.exit(f"couldn't reach {repo}/releases/latest: {e}")
prefix = repo + "/releases/tag/"
tag = urllib.parse.unquote(final[len(prefix):]) if final.startswith(prefix) else ""
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", tag):
    sys.exit(f"{repo}/releases/latest doesn't lead to a release (it led to {final})")
print(tag)
EOF
}

choose_source() {  # without RNFS_SOURCE: the latest release's source archive, or the main branch's with --main
    [ -z "$SOURCE" ] || return 0
    if [ "$MAIN" = yes ]; then
        SOURCE=$REPO_URL/archive/refs/heads/main.zip
        say "Installing the main branch, which may be newer than the latest release"
    else
        tag=$(latest_release) || die "couldn't find rnfs's latest release on GitHub. Check your connection, or install
the main branch instead with --main."
        SOURCE=$REPO_URL/archive/refs/tags/$tag.zip
        say "Installing rnfs $tag, the latest release"
    fi
}

# The source: a local tree, or an archive downloaded and unpacked here (by Python, so no curl/unzip needed),
# so its packaging/requirements.lock and packaging/build-backend.lock can be used. A wheel or other pip requirement goes to pip as it is.

unpack() {  # unpack SOURCE DIR: download (if a URL) and unpack the .zip/.tar.* SOURCE in DIR; print its top directory
    "$VENV/bin/python" -I - "$1" "$2" <<'EOF'
import os, shutil, sys, tarfile, urllib.request, zipfile

source, work = sys.argv[1:3]
archive = source
if source.startswith(("http://", "https://")):
    archive = os.path.join(work, "download")
    with urllib.request.urlopen(source, timeout=120) as response, open(archive, "wb") as f:
        shutil.copyfileobj(response, f)


def check(name):
    """Exit if an archive member's name would land outside the extraction directory."""
    parts = name.replace("\\", "/").split("/")
    if name.startswith(("/", "\\")) or ".." in parts or ":" in parts[0]:
        sys.exit(f"unsafe path in the archive: {name!r}")


dest = os.path.join(work, "unpacked")
os.mkdir(dest)
if zipfile.is_zipfile(archive):
    with zipfile.ZipFile(archive) as z:
        for name in z.namelist():
            check(name)
        z.extractall(dest)
else:
    with tarfile.open(archive) as t:
        members = t.getmembers()
        for m in members:
            check(m.name)
        members = [m for m in members if m.isfile() or m.isdir()]  # no links or devices
        extra = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
        t.extractall(dest, members=members, **extra)
entries = os.listdir(dest)  # GitHub archives and sdists have one top directory (rnfs-main/, rnfs-X.Y.Z/)
only = os.path.join(dest, entries[0]) if len(entries) == 1 else dest
print(only if os.path.isdir(only) else dest)
EOF
}

fetch_source() {  # fetch_source WORKDIR: set build_source (what pip builds), lock and backend_lock (or "")
    build_source=$SOURCE
    lock=""
    backend_lock=""
    case "${SOURCE%%[?#]*}" in
        *.zip|*.tar.gz|*.tgz|*.tar.bz2|*.tar.xz) is_archive=yes ;;
        *) is_archive=no ;;
    esac
    if [ -d "$SOURCE" ]; then
        # Build a local source tree from a copy, so the build leaves no build/ or *.egg-info in it
        [ -f "$SOURCE/pyproject.toml" ] && [ -d "$SOURCE/rnfs" ] || die "$SOURCE isn't an rnfs source tree"
        build_source=$1/src
        mkdir -p "$build_source"
        for item in "$SOURCE"/pyproject.toml "$SOURCE"/README* "$SOURCE"/MANIFEST.in "$SOURCE"/LICEN[CS]E* \
                    "$SOURCE"/COPYING* "$SOURCE"/rnfs; do
            if [ -e "$item" ]; then cp -R "$item" "$build_source/"; fi
        done
        for item in "$SOURCE"/packaging/requirements.lock "$SOURCE"/packaging/build-backend.lock; do
            if [ -f "$item" ]; then
                mkdir -p "$build_source/packaging"
                cp "$item" "$build_source/packaging/"
            fi
        done
    elif [ "$is_archive" = yes ]; then
        build_source=$(unpack "$SOURCE" "$1") || die "couldn't download or unpack $SOURCE"
        [ -f "$build_source/pyproject.toml" ] && [ -d "$build_source/rnfs" ] || die "$SOURCE isn't an rnfs source archive"
    fi
    if [ -f "$build_source/packaging/requirements.lock" ]; then lock=$build_source/packaging/requirements.lock; fi
    if [ -f "$build_source/packaging/build-backend.lock" ]; then backend_lock=$build_source/packaging/build-backend.lock; fi
}

build_wheel() {  # build_wheel WORKDIR: build (or fetch) the rnfs wheel from build_source into WORKDIR
    if [ -n "$backend_lock" ]; then
        # With the pinned, hash-checked setuptools, in a venv of its own (so it stays out of rnfs's venv), and
        # without build isolation, which would fetch the newest setuptools from PyPI
        "$PYTHON" -I -m venv "$1/build-venv" || die "couldn't create a build venv with $PYTHON"
        pip_in "$1/build-venv/bin/python" install --quiet --require-hashes --only-binary=:all: -r "$backend_lock" ||
            die "pip couldn't install the build backend from $backend_lock"
        pip_in "$1/build-venv/bin/python" wheel --quiet --no-deps --no-build-isolation --wheel-dir "$1" \
            "$build_source" || die "couldn't build rnfs from $SOURCE"
    else
        case "$build_source" in
            *.whl) ;;
            *) warn "$SOURCE has no packaging/build-backend.lock, so pip may build it with the newest setuptools (unpinned)" ;;
        esac
        vpip wheel --quiet --no-deps --wheel-dir "$1" "$build_source" || die "couldn't build rnfs from $SOURCE"
    fi
}

install_locked() {  # install_locked LOCK WORKDIR: rnfs's dependencies, as LOCK pins them (versions and hashes)
    # Wheels only: building an sdist would fetch its build tools (setuptools, ...) unpinned
    only_binary=:all:
    if [ "${RNFS_ALLOW_SOURCE_BUILDS:-}" = 1 ]; then
        warn "RNFS_ALLOW_SOURCE_BUILDS=1: dependencies without a wheel for this platform are built from source, with unpinned build tools"
        only_binary=:none:
    fi
    if ! vpip install --quiet --require-hashes --only-binary="$only_binary" -r "$1" 2>"$2/pip-errors"; then
        cat "$2/pip-errors" >&2
        if grep -q 'No matching distribution' "$2/pip-errors"; then
            die "PyPI has no prebuilt wheel ($(uname -m), Python $(python_version "$VENV/bin/python")) of one of rnfs's
dependencies (above). To build it from its pinned, hash-checked source instead (its build tools are then
fetched unpinned, and it needs a C compiler and Python headers, e.g. apt install build-essential
python3-dev libffi-dev), run this again with RNFS_ALLOW_SOURCE_BUILDS=1 set:
  curl -fsSL .../install.sh | RNFS_ALLOW_SOURCE_BUILDS=1 sh             (or: | sudo RNFS_ALLOW_SOURCE_BUILDS=1 sh -s -- --system)"
        fi
        die "pip couldn't install rnfs's dependencies from $1"
    fi
    cat "$2/pip-errors" >&2  # warnings
}

have_user_systemd() { command -v systemctl >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; }

# Actions

do_install() {
    find_python
    check_launcher
    check_unit
    choose_source
    updating=no
    if venv_ok; then
        say "Updating rnfs in $VENV"
        updating=yes
    else
        check_venv
        if [ -e "$VENV" ]; then
            say "Re-creating the venv in $VENV"
            rm -rf "$VENV"
        fi
        say "Creating a venv in $VENV with $("$PYTHON" -I -c 'import sys; print(sys.executable, sys.version.split()[0])')"
        mkdir -p "$PREFIX"
        "$PYTHON" -I -m venv "$VENV"
    fi
    # pip isn't upgraded from PyPI (that would be an unpinned download); the venv's own pip does --require-hashes

    # Build the wheel once, then install it: the dependencies at the versions the source's lock pins, and rnfs
    # itself with a forced reinstall, since the main branch (or a local source) can change without its version
    # number changing
    wheels=$(mktemp -d "${TMPDIR:-/tmp}/rnfs-install.XXXXXX")
    trap 'rm -rf "$wheels"' EXIT INT TERM
    say "Fetching and building rnfs from $SOURCE"
    fetch_source "$wheels"
    build_wheel "$wheels"
    wheel=""
    for found in "$wheels"/rnfs-*.whl; do
        if [ -f "$found" ]; then wheel=$found; break; fi
    done
    [ -n "$wheel" ] || die "building $SOURCE didn't produce an rnfs wheel"
    if [ -n "$lock" ]; then
        install_locked "$lock" "$wheels"
    else
        warn "$SOURCE has no packaging/requirements.lock, so rnfs's dependencies are installed unpinned (newest versions)"
        vpip install --quiet --upgrade "$wheel" || die "pip couldn't install rnfs's dependencies"
    fi
    vpip install --quiet --force-reinstall --no-deps "$wheel" || die "pip couldn't install rnfs"
    # rnfs went in with --no-deps: check the lock (or an older venv's leftovers) left nothing missing or conflicting
    check=$(vpip check 2>&1) || die "rnfs's dependencies in $VENV are incomplete or conflicting:
$check"
    rm -rf "$wheels"
    trap - EXIT INT TERM

    install_launcher
    version=$("$LAUNCHER" --version 2>/dev/null) || die "the installed rnfs doesn't run: $LAUNCHER --version failed"
    say "Installed $version as $LAUNCHER"

    if [ "$SERVICE" = yes ] || unit_ours; then
        write_unit
        if [ "$MODE" = system ]; then
            say "Installed the systemd user unit $UNIT; each user can start it with: systemctl --user enable --now rnfs"
            if [ "$updating" = yes ]; then
                say "Users already running rnfs keep the old version until they restart it (with the service:"
                say "  systemctl --user daemon-reload && systemctl --user restart rnfs)"
            fi
        elif have_user_systemd; then
            systemctl --user daemon-reload
            if [ "$SERVICE" = yes ]; then
                systemctl --user enable --now rnfs.service
                say "Started the rnfs service (systemctl --user status rnfs; logs: journalctl --user -u rnfs -f)"
            else
                systemctl --user try-restart rnfs.service || true
                say "Updated the systemd user unit (restarted rnfs if it was running)"
            fi
        else
            say "Installed $UNIT, but there's no systemd user session here; start it later with:"
            say "  systemctl --user daemon-reload && systemctl --user enable --now rnfs"
        fi
    elif [ "$MODE" = system ] && [ "$updating" = yes ]; then
        say "Users already running rnfs keep the old version until they restart it"
    fi

    case ":${PATH:-}:" in
        *":$BIN_DIR:"*) say "Run: rnfs daemon   (and rnfs --help)" ;;
        *) warn "$BIN_DIR isn't on your PATH. Add it, e.g. in ~/.profile or ~/.bashrc:
  export PATH=\"$BIN_DIR:\$PATH\"
or run rnfs as $LAUNCHER" ;;
    esac
}

remove_enable_links() {  # system mode: the links `systemctl --user enable` (or --global) made to our unit
    # Only symlinks named rnfs.service that point at exactly $UNIT, in each user's default ~/.config (one
    # set in XDG_CONFIG_HOME isn't known here). find doesn't follow symlinks, and -delete unlinks the entry
    # it checked, so nothing but such a link is removed.
    { echo /etc/systemd/user/default.target.wants
      { getent passwd 2>/dev/null || cat /etc/passwd; } | cut -d: -f6 | sort -u | while IFS= read -r home; do
          case "$home" in /*) printf '%s\n' "${home%/}/.config/systemd/user/default.target.wants" ;; esac
      done
    } | while IFS= read -r wants; do
        if [ -d "$wants" ]; then
            find "$wants" -maxdepth 1 -name rnfs.service -type l -lname "$UNIT" -print -delete 2>/dev/null |
                while IFS= read -r link; do say "Removed $link"; done
        fi
    done
}

do_uninstall() {
    if unit_ours; then
        if [ "$MODE" = user ] && have_user_systemd; then
            systemctl --user disable --now rnfs.service >/dev/null 2>&1 || true
        fi
        rm -f "$UNIT"
        if [ "$MODE" = user ] && have_user_systemd; then systemctl --user daemon-reload || true; fi
        say "Removed $UNIT"
        if [ "$MODE" = system ]; then
            remove_enable_links
            say "Users running the rnfs service keep it running until they log out or run: systemctl --user stop rnfs"
        fi
    elif [ -e "$UNIT" ] || [ -L "$UNIT" ]; then
        warn "left $UNIT alone: this installer didn't write it"
    fi
    if ours "$LAUNCHER"; then
        rm -f "$LAUNCHER"
        say "Removed $LAUNCHER"
    elif [ -e "$LAUNCHER" ]; then
        warn "left $LAUNCHER alone: this installer didn't make it"
    fi
    if [ -d "$VENV" ]; then
        rm -rf "$VENV"
        rmdir "$PREFIX" 2>/dev/null || true
        say "Removed $VENV"
    else
        say "rnfs isn't installed in $VENV"
    fi
    say "Your rnfs data in ~/.rnfs (and Reticulum's config in ~/.reticulum) were kept; delete them yourself if you want."
}

if [ "$MODE" = system ] && [ -e /var/lib/dpkg/info/rnfs.list ]; then
    die "the rnfs .deb package is installed (it owns /opt/rnfs and /usr/bin/rnfs). Update or remove it with apt
instead (sudo apt remove rnfs), or use this installer per user, without root."
fi

case "$ACTION" in
    install) do_install ;;
    uninstall) do_uninstall ;;
esac
