"""Smoke-test an rnfs command (a frozen exe, a .deb's launcher, a flatpak wrapper, ...).

    python3 packaging/smoke_test.py RNFS_COMMAND [ARG ...]
    python3 packaging/smoke_test.py dist/rnfs.exe
    python3 packaging/smoke_test.py flatpak run io.github.afit21.rnfs

Runs `--version` and `--help`, then starts `daemon` with a temporary --dir and a temporary Reticulum config
(no shared instance, so a running rnsd isn't touched; one TCP server interface on 127.0.0.1 and nothing that
reaches the network), on free ports, polls `status` until the daemon answers, checks the TCP interface is
listening (which proves RNS found its dynamically loaded interface modules, the part a frozen build can
miss), stops the daemon the way Ctrl+C would and checks it exits. Exits non-zero on any failure.
Uses only the standard library, so any Python 3 can run it.
"""
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

WINDOWS = sys.platform == "win32"
RETICULUM_CONFIG = """\
[reticulum]
  enable_transport = No
  share_instance = No

[logging]
  loglevel = 4

[interfaces]
  [[Smoke Test TCP]]
    type = TCPServerInterface
    enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {port}
"""
START_TIMEOUT = 90  # seconds; a one-file exe unpacks itself first, which is slow on CI runners
STOP_TIMEOUT = 45


def free_port() -> int:
    """A local TCP port that is free now."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run(command: list[str], *args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run command + args, print what it printed, and fail if it fails (when check)."""
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=120)
    print(f"$ {' '.join(command + list(args))}  -> exit {result.returncode}")
    if check:
        print(result.stdout.rstrip())
        if result.returncode != 0:
            print(result.stderr.rstrip(), file=sys.stderr)
            sys.exit(f"smoke test: {' '.join(args)} failed")
    return result


def main():
    command = sys.argv[1:]
    if not command:
        sys.exit(__doc__)
    if os.path.isfile(command[0]):
        command[0] = os.path.abspath(command[0])  # e.g. dist/rnfs.exe

    version = run(command, "--version").stdout
    if not version.startswith("rnfs "):
        sys.exit(f"smoke test: unexpected --version output {version!r}")
    help_text = run(command, "--help").stdout
    if "daemon" not in help_text:
        sys.exit("smoke test: --help doesn't list the daemon command")

    with tempfile.TemporaryDirectory(prefix="rnfs-smoke-") as tmp:
        rnfs_dir = os.path.join(tmp, "rnfs")
        rns_dir = os.path.join(tmp, "reticulum")
        os.makedirs(rns_dir)
        with open(os.path.join(rns_dir, "config"), "w", encoding="utf-8") as f:
            interface_port = free_port()
            f.write(RETICULUM_CONFIG.format(port=interface_port))
        ports = ["--dir", rnfs_dir, "--api-port", str(free_port())]
        log_path = os.path.join(tmp, "daemon.log")
        with open(log_path, "w", encoding="utf-8") as log:
            daemon = subprocess.Popen(
                command + ["daemon", *ports, "--gateway-port", str(free_port()), "--rnsconfig", rns_dir],
                stdout=log, stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if WINDOWS else 0)
        try:
            deadline = time.monotonic() + START_TIMEOUT
            while True:
                if daemon.poll() is not None:
                    sys.exit(f"smoke test: daemon exited early with {daemon.returncode}")
                status = run(command, "status", *ports, check=False)
                if status.returncode == 0:
                    print(status.stdout.rstrip())
                    break
                if time.monotonic() > deadline:
                    sys.exit(f"smoke test: daemon didn't answer `status` in {START_TIMEOUT} s:\n{status.stderr}")
                time.sleep(1)
            try:
                socket.create_connection(("127.0.0.1", interface_port), timeout=10).close()
            except OSError as e:
                sys.exit(f"smoke test: RNS's TCP interface isn't listening on {interface_port} ({e}): "
                         "were RNS's interface modules bundled?")
            print(f"RNS TCP interface listening on 127.0.0.1:{interface_port}")
        finally:
            if daemon.poll() is None:
                daemon.send_signal(signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT)
                try:
                    daemon.wait(STOP_TIMEOUT)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait()
                    print(f"smoke test: daemon didn't stop within {STOP_TIMEOUT} s", file=sys.stderr)
                    stopped_cleanly = False
                else:
                    stopped_cleanly = True
            else:
                stopped_cleanly = False
            with open(log_path, encoding="utf-8", errors="replace") as log:
                print("--- daemon log ---\n" + log.read().rstrip() + "\n------------------")
        if not stopped_cleanly:
            sys.exit("smoke test: daemon didn't stop cleanly")
    print("smoke test passed")


if __name__ == "__main__":
    main()
