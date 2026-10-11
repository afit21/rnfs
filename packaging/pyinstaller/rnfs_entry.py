"""PyInstaller entry point for the one-file rnfs executable (same as the `rnfs` console script)."""
import multiprocessing

from rnfs.cli import main

if __name__ == "__main__":
    # LXMF's stamp generator uses multiprocessing; a frozen Windows exe needs this so child processes
    # run the worker instead of a second copy of the CLI
    multiprocessing.freeze_support()
    main()
