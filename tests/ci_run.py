"""Run tests as CI does, then run each one that failed once more, in a fresh process.

    python -m tests.ci_run                          # as `python -m unittest discover tests`
    python -m tests.ci_run tests.test_e2e ...       # as `python -m unittest tests.test_e2e ...`

A test that fails and then passes alone is timing-sensitive on a slow shared runner, not broken: the run passes,
and the test is named in a warning (a GitHub Actions annotation and a line in the job summary) so it gets fixed.
A test that fails twice fails the run, as does a run with more than MAX_RERUN failures (something systematic,
which a second run would only slow down), a module that can't be imported, or an unexpected success.
"""

import os
import re
import subprocess
import sys
import unittest

MAX_RERUN = 5
TESTS = os.path.dirname(os.path.abspath(__file__))
# A class or module fixture that failed is reported under this name rather than a test's id
FIXTURE = re.compile(r"^(?:setUpClass|tearDownClass|setUpModule|tearDownModule) \((.+)\)$")


def rerun_name(test) -> str | None:
    """The name to run test again by, or None if running it again can't help (it didn't import). A failed subtest
    is run again by its test's name: its own id carries its parameters, which no loader takes."""
    test = getattr(test, "test_case", test)  # a unittest.case._SubTest's test
    name = test.id()
    if name.startswith("unittest.loader."):
        return None
    fixture = FIXTURE.match(name)
    return fixture.group(1) if fixture else name


def note(message: str):
    """A warning on the run's page and in its summary (a plain line outside GitHub Actions)."""
    print(f"::warning title=Flaky test::{message}" if os.environ.get("GITHUB_ACTIONS") else f"warning: {message}",
          flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write(f"- {message}\n")


def main(names: list[str]) -> int:
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromNames(names) if names else loader.discover(TESTS)
    result = unittest.TextTestRunner().run(suite)
    if result.wasSuccessful():
        return 0
    names_again = {rerun_name(test) for test, _ in result.failures + result.errors}
    if result.unexpectedSuccesses or None in names_again:
        return 1
    again = sorted(names_again)
    if len(again) > MAX_RERUN:
        print(f"{len(again)} tests failed: not running them again", flush=True)
        return 1
    print(f"\nRunning the {len(again)} that failed again, alone:", *again, sep="\n  ", flush=True)
    # discover imports the modules under tests/ as top-level names (test_api, security.test_l15_...), so the
    # second run needs tests/ on the path as well as the repository
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [TESTS, os.environ.get("PYTHONPATH")]))}
    if subprocess.run([sys.executable, "-m", "unittest", *again], env=env).returncode != 0:
        return 1
    for name in again:
        note(f"{name} failed, then passed when run again: it depends on timing or on another test")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
