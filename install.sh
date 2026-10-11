#!/bin/sh
# Install, update or uninstall rnfs from the main branch on GitHub, in its own Python virtual environment.
#
#   curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --service
#   sh install.sh [--system] [--service] [--update | --uninstall]
#
# Per user (the default):   venv ~/.local/share/rnfs/venv, command ~/.local/bin/rnfs
# --system (or as root):    venv /opt/rnfs/venv,           command /usr/local/bin/rnfs
#
#   --service     also install the systemd user unit and start it (system mode: install it for all users,
#                 each then runs `systemctl --user enable --now rnfs`)
#   --update      re-install from the latest main branch (running the installer again does the same)
#   --uninstall   remove the venv, the command and the unit (keeps your data in ~/.rnfs)
#   --python P    use this Python (3.11 or newer) for the venv
#   --force       replace an existing rnfs command that this installer didn't make (e.g. a pipx one)
#
# Environment: RNFS_SOURCE  what to install instead of the main branch: a local source directory, a
#                           .zip/.tar.gz/.whl path or URL, or another pip requirement
#              RNFS_PYTHON  same as --python
#
# Dependencies (rns, lxmf, cryptography, ...) are installed at the versions and sha256 hashes pinned in the
# source's packaging/requirements.lock (pip --require-hashes); a source without one (a .whl, a pip
# requirement) gets the newest versions instead, with a warning.
# Needs Python 3.11+ with venv and pip (no git). Your rnfs data (~/.rnfs) and Reticulum config (~/.reticulum)
# are never touched.
set -eu

DEFAULT_SOURCE="https://github.com/afit21/rnfs/archive/refs/heads/main.zip"
SOURCE=${RNFS_SOURCE:-$DEFAULT_SOURCE}
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
        --uninstall) ACTION=uninstall ;;
        --force) FORCE=yes ;;
        --python) [ $# -ge 2 ] || die "--python needs a path"; PYTHON=$2; shift ;;
        --python=*) PYTHON=${1#--python=} ;;
        -h|--help)
            if [ -f "$0" ]; then sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
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
    PREFIX=/opt/rnfs
    VENV=$PREFIX/venv
    BIN_DIR=/usr/local/bin
    UNIT_DIR=/etc/systemd/user
else
    [ -n "${HOME:-}" ] || die "HOME isn't set"
    PREFIX=${XDG_DATA_HOME:-$HOME/.local/share}/rnfs
    VENV=$PREFIX/venv
    BIN_DIR=$HOME/.local/bin
    UNIT_DIR=${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user
fi
LAUNCHER=$BIN_DIR/rnfs
UNIT=$UNIT_DIR/rnfs.service

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

vpip() { "$VENV/bin/python" -I -m pip --disable-pip-version-check "$@"; }

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
    cat > "$UNIT" <<EOF
# systemd user unit for the rnfs daemon, installed by rnfs's install.sh.
#   systemctl --user enable --now rnfs
#   journalctl --user -u rnfs -f          # logs
# To keep it running while you're logged out: loginctl enable-linger \$USER

[Unit]
Description=rnfs: content-addressed file sharing over Reticulum
# Start after rnsd if it also runs as a user service, so rnfs attaches to it rather than starting its own instance
After=rnsd.service

[Service]
Type=simple
ExecStart=$(systemd_quote "$LAUNCHER") daemon
Restart=on-failure
RestartSec=10
# The daemon shuts down cleanly on SIGTERM, saving provider records and RNS state
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=default.target
EOF
}

# The source: a local tree, or an archive downloaded and unpacked here (by Python, so no curl/unzip needed),
# so its packaging/requirements.lock can be used. A wheel or other pip requirement goes to pip as it is.

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

fetch_source() {  # fetch_source WORKDIR: set build_source (what pip builds) and lock (the lock file, or "")
    build_source=$SOURCE
    lock=""
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
        if [ -f "$SOURCE/packaging/requirements.lock" ]; then
            mkdir -p "$build_source/packaging"
            cp "$SOURCE/packaging/requirements.lock" "$build_source/packaging/"
        fi
    elif [ "$is_archive" = yes ]; then
        build_source=$(unpack "$SOURCE" "$1") || die "couldn't download or unpack $SOURCE"
        [ -f "$build_source/pyproject.toml" ] && [ -d "$build_source/rnfs" ] || die "$SOURCE isn't an rnfs source archive"
    fi
    if [ -f "$build_source/packaging/requirements.lock" ]; then lock=$build_source/packaging/requirements.lock; fi
}

have_user_systemd() { command -v systemctl >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; }

# Actions

do_install() {
    find_python
    check_launcher
    if venv_ok; then
        say "Updating rnfs in $VENV"
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
    # itself with a forced reinstall, since the main branch can change without its version number changing
    wheels=$(mktemp -d "${TMPDIR:-/tmp}/rnfs-install.XXXXXX")
    trap 'rm -rf "$wheels"' EXIT INT TERM
    say "Fetching and building rnfs from $SOURCE"
    fetch_source "$wheels"
    vpip wheel --quiet --no-deps --wheel-dir "$wheels" "$build_source" || die "couldn't build rnfs from $SOURCE"
    wheel=""
    for found in "$wheels"/rnfs-*.whl; do
        if [ -f "$found" ]; then wheel=$found; break; fi
    done
    [ -n "$wheel" ] || die "building $SOURCE didn't produce an rnfs wheel"
    if [ -n "$lock" ]; then
        vpip install --quiet --require-hashes -r "$lock" || die "pip couldn't install rnfs's dependencies from $lock"
    else
        warn "$SOURCE has no packaging/requirements.lock, so rnfs's dependencies are installed unpinned (newest versions)"
        vpip install --quiet --upgrade "$wheel" || die "pip couldn't install rnfs's dependencies"
    fi
    vpip install --quiet --force-reinstall --no-deps "$wheel" || die "pip couldn't install rnfs"
    rm -rf "$wheels"
    trap - EXIT INT TERM

    install_launcher
    version=$("$LAUNCHER" --version 2>/dev/null) || die "the installed rnfs doesn't run: $LAUNCHER --version failed"
    say "Installed $version as $LAUNCHER"

    if [ "$SERVICE" = yes ] || [ -f "$UNIT" ]; then
        write_unit
        if [ "$MODE" = system ]; then
            say "Installed the systemd user unit $UNIT; each user can start it with: systemctl --user enable --now rnfs"
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
    fi

    case ":${PATH:-}:" in
        *":$BIN_DIR:"*) say "Run: rnfs daemon   (and rnfs --help)" ;;
        *) warn "$BIN_DIR isn't on your PATH. Add it, e.g. in ~/.profile or ~/.bashrc:
  export PATH=\"$BIN_DIR:\$PATH\"
or run rnfs as $LAUNCHER" ;;
    esac
}

do_uninstall() {
    if [ -f "$UNIT" ]; then
        if [ "$MODE" = user ] && have_user_systemd; then
            systemctl --user disable --now rnfs.service >/dev/null 2>&1 || true
        fi
        rm -f "$UNIT"
        if [ "$MODE" = user ] && have_user_systemd; then systemctl --user daemon-reload || true; fi
        say "Removed $UNIT"
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
