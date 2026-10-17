"""tests/security/README.md, the index from audit ID to test module, code and fix commit, stays in step with the
security tests: every tests/security/test_*.py is listed, and every test module a row names exists."""
import os
import re
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SECURITY = os.path.join(REPO, "tests", "security")
INDEX = os.path.join(SECURITY, "README.md")

# A row of one of the index's tables: "| ID | Test modules | Code guarded | Fix commits |" (not the header or rule).
ROW = re.compile(r"(?m)^\| (?!ID \||---)(.+?) \| (.+?) \| (.+?) \| (.+?) \|$")
# A test module in tests/security, named bare, and one elsewhere in tests/, named by its path.
SECURITY_TEST = re.compile(r"(?<![\w/])(test_\w+\.py)")
OTHER_TEST = re.compile(r"\btests/(test_\w+\.py)")


def index_rows() -> list[tuple[str, str]]:
    """Each row of the index as (ID, test modules column)."""
    with open(INDEX, encoding="utf-8") as f:
        return [(match.group(1), match.group(2)) for match in ROW.finditer(f.read())]


class SecurityIndexTest(unittest.TestCase):
    """The index lists every security test and names only ones that exist."""

    def test_every_security_test_is_indexed(self):
        """Each tests/security/test_*.py appears in some row's test modules."""
        listed = {name for _, tests in index_rows() for name in SECURITY_TEST.findall(tests)}
        present = {name for name in os.listdir(SECURITY) if re.fullmatch(r"test_\w+\.py", name)}
        self.assertEqual(present - listed, set(), "add these to tests/security/README.md")

    def test_every_indexed_test_exists(self):
        """Every test module a row names exists: bare names in tests/security, tests/... paths in tests/."""
        rows = index_rows()
        self.assertGreater(len(rows), 50)
        for audit_id, tests in rows:
            for name in SECURITY_TEST.findall(tests):
                self.assertTrue(os.path.isfile(os.path.join(SECURITY, name)), f"{audit_id}: {name}")
            for name in OTHER_TEST.findall(tests):
                self.assertTrue(os.path.isfile(os.path.join(REPO, "tests", name)), f"{audit_id}: tests/{name}")

    def test_ids_are_unique(self):
        """One row per ID, so a reader looking one up finds all of it in one place."""
        ids = [audit_id for audit_id, _ in index_rows() if not audit_id.startswith("none")]
        self.assertEqual(sorted(set(ids)), sorted(ids))


if __name__ == "__main__":
    unittest.main()
