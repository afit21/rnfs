"""PyInstaller entry point for the one-file rnfs executable (same as the `rnfs` console script)."""
import multiprocessing

from rnfs.cli import main

if __name__ == "__main__":
    # A frozen Windows exe needs this so that a child process something in it starts with multiprocessing runs
    # the worker instead of a second copy of the CLI (LXMF's stamp generator did, until rnfs 0.2.0 left LXMF out)
    multiprocessing.freeze_support()
    main()
