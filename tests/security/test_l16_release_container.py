"""L16 (fix): the release workflow's Debian 12 smoke test ran `docker run -v "$PWD:/w" debian:12` (a read-write mount,
an image not pinned by digest, run as root) right before the .deb it could change was uploaded, checksummed and
attested. Now the checkout is mounted read-only, the image is pinned by digest and the smoke test runs as nobody.

Static checks of .github/workflows/*.yml, as tests/test_packaging.py makes. The workflow step itself was run
locally with Docker against a .deb built by packaging/build-deb.sh (see the report); CI didn't run it.
"""
import os
import re
import unittest

from tests.test_packaging import WORKFLOWS, read

DIGEST = re.compile(r"@sha256:[0-9a-f]{64}\b")


def docker_runs(text: str) -> list[str]:
    """Every `docker run` command in a workflow, its continuation lines joined."""
    joined = re.sub(r"\\\n\s*", " ", text)
    return [line.strip() for line in joined.splitlines() if "docker run" in line and not line.lstrip().startswith("#")]


class ReleaseContainerTest(unittest.TestCase):
    """Containers the workflows run can't change what they build."""

    def test_docker_runs_are_read_only_and_pinned(self):
        found = 0
        for name in os.listdir(WORKFLOWS):
            for command in docker_runs(read(os.path.join(WORKFLOWS, name))):
                found += 1
                with self.subTest(name=name, command=command):
                    mounts = re.findall(r"(?:-v|--volume)[ =]\"?([^\" ]+)", command)
                    self.assertTrue(mounts)
                    for mount in mounts:
                        self.assertRegex(mount, r":ro$", "mounted writable")
                    self.assertNotIn("--mount", command)
                    self.assertRegex(command, DIGEST, "image not pinned by digest")
                    self.assertIn("--security-opt no-new-privileges", command)
        self.assertGreaterEqual(found, 1)

    def test_debian_smoke_test_runs_as_nobody(self):
        text = read(os.path.join(WORKFLOWS, "release.yml"))
        step = text[text.index("- name: Install and smoke test (Debian 12"):]
        step = step[:step.index("- uses:")]
        self.assertIn("cp dist/rnfs_*_amd64.deb /tmp/", step)  # installed from a copy: the mount is read-only
        self.assertIn("apt-get install -y -qq /tmp/rnfs_*_amd64.deb", step)
        self.assertRegex(step, r"setpriv --reuid=nobody --regid=nogroup --clear-groups \\\s+python3 "
                               r"packaging/smoke_test.py")

    def test_containers_are_pinned_by_digest(self):
        for name in os.listdir(WORKFLOWS):
            for image in re.findall(r"(?m)^\s*image:\s*(\S+)", read(os.path.join(WORKFLOWS, name))):
                with self.subTest(name=name, image=image):
                    self.assertRegex(image, DIGEST)


if __name__ == "__main__":
    unittest.main()
