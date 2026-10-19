"""Rules the tests themselves keep, checked statically so a test that can't fail, or fails only off Linux, is caught
on the machine it was written on (CONTRIBUTING.md, "Tests on every OS")."""
import ast
import os
import unittest

TESTS = os.path.dirname(os.path.abspath(__file__))


def test_sources():
    """(path relative to tests/, parsed module) of every Python file under tests/."""
    for top, _dirs, files in os.walk(TESTS):
        for name in sorted(files):
            if name.endswith(".py"):
                path = os.path.join(top, name)
                with open(path, encoding="utf-8") as f:
                    yield os.path.relpath(path, TESTS), ast.parse(f.read(), path)


class SuiteRulesTest(unittest.TestCase):
    def test_a_bare_wait_for_names_what_it_waits_for(self):
        """support.wait_for returns its condition's falsy value on a timeout unless given what=, so a call whose
        value nothing checks passes however long the wait failed: it must say what it waits for, and so raise."""
        silent = []
        for path, tree in test_sources():
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)):
                    continue
                func = node.value.func
                # support.wait_for, imported or not; not a method of the same name (threading.Condition's)
                name = func.id if isinstance(func, ast.Name) else (
                    func.attr if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
                    and func.value.id == "support" else None)
                if name == "wait_for" and len(node.value.args) < 3 and not any(
                        keyword.arg == "what" for keyword in node.value.keywords):
                    silent.append(f"{path}:{node.lineno}")
        self.assertEqual(silent, [], "wait_for(...) whose result is unused needs what=")


if __name__ == "__main__":
    unittest.main()
