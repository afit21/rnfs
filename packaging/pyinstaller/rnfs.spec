# PyInstaller spec for a single-file rnfs executable (console app).
#
#   pip install --require-hashes --only-binary=:all: -r packaging/build-tools.lock      # PyInstaller, setuptools
#   pip install --require-hashes --only-binary=:all: -r packaging/requirements.lock
#   pip install --no-deps --no-build-isolation .
#                                        # rnfs and its dependencies (rns, lxmf, ...) must be installed
#   pyinstaller --clean --noconfirm packaging/pyinstaller/rnfs.spec
#   dist/rnfs(.exe) --version
#
# Reticulum finds its interface modules by globbing its own package directory for *.py files
# (RNS/__init__.py, RNS/Interfaces/__init__.py) and then does `from RNS.Interfaces import *`. In a frozen
# app the modules live in the PYZ archive, so the glob would find nothing and no interface would load. So
# every RNS (and LXMF) submodule is bundled as a hidden import AND its .py sources are bundled as data
# files, which makes the globs see the same file list as in a normal install.
import os
import re
import sys

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

REPO = os.path.abspath(os.path.join(SPECPATH, "..", ".."))


def rnfs_version() -> str:
    """rnfs.__version__, read from the source so the spec doesn't import the package."""
    with open(os.path.join(REPO, "rnfs", "__init__.py"), encoding="utf-8") as f:
        return re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', f.read(), re.M).group(1)


def required(package: str) -> str:
    """package, after checking it's installed: collect_submodules would silently bundle nothing otherwise."""
    try:
        __import__(package)
    except ImportError:
        sys.exit(f"rnfs.spec: {package} isn't installed; install rnfs's dependencies first "
                 "(pip install --require-hashes -r packaging/requirements.lock)")
    return package


VERSION = rnfs_version()

hiddenimports = (
    # rnfs.features loads feature modules by name with importlib, and rnfs.statedb its migration's importers
    collect_submodules("rnfs")
    + ["sqlite3"]  # the state database; imported inside a function, so named here (PyInstaller adds _sqlite3)
    + collect_submodules(required("RNS"))
    + collect_submodules(required("LXMF"))
    + collect_submodules("serial")  # pyserial: Serial, KISS and RNode interfaces
    + collect_submodules("cryptography")
)
datas = (
    collect_data_files("RNS", include_py_files=True)
    + collect_data_files("LXMF", include_py_files=True)
)

# Windows version resource from rnfs.__version__ (numeric part, padded to four fields)
version_file = None
if sys.platform == "win32":
    from PyInstaller.utils.win32.versioninfo import (FixedFileInfo, StringFileInfo, StringStruct, StringTable,
                                                     VarFileInfo, VarStruct, VSVersionInfo)

    numbers = [int(n) for n in re.findall(r"\d+", VERSION)[:4]]
    numbers += [0] * (4 - len(numbers))
    version_file = VSVersionInfo(
        ffi=FixedFileInfo(filevers=tuple(numbers), prodvers=tuple(numbers)),
        kids=[
            StringFileInfo([StringTable("040904B0", [
                StringStruct("CompanyName", "rnfs"),
                StringStruct("FileDescription", "rnfs: content-addressed file sharing over Reticulum"),
                StringStruct("FileVersion", VERSION),
                StringStruct("InternalName", "rnfs"),
                StringStruct("OriginalFilename", "rnfs.exe"),
                StringStruct("ProductName", "rnfs"),
                StringStruct("ProductVersion", VERSION),
            ])]),
            VarFileInfo([VarStruct("Translation", [0x0409, 1200])]),
        ],
    )

a = Analysis(
    [os.path.join(SPECPATH, "rnfs_entry.py")],
    pathex=[],  # rnfs is imported from the installed package (pip install .)
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    # Not used by the daemon; keeps the exe smaller
    excludes=["tkinter", "test", "unittest.test", "pydoc_data"],
    noarchive=False,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="rnfs",
    debug=False,
    strip=False,
    upx=False,
    console=True,
    runtime_tmpdir=None,
    version=version_file,
)
