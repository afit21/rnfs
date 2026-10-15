"""H2-R (fix), two Low residuals of the display-mu-file page:

- Private mode: the page showed any complete operator pin (source user or folder) to anyone who knew its CID, so
  on a node in private mode, which serves its content to members only, any NomadNet visitor could read its
  unsealed pinned files up to MAX_SIZE. Now, in private mode (as `rnfs status` says; an answer without it counts
  as private), only allowlisted CIDs are shown.
- Rate limit: RATE_LIMIT counted all visitors together, so one visitor's requests locked everyone else out for
  RATE_WINDOW. NomadNet passes a page the visitor's identity hash (remote_identity) when they identified on the
  link, and a link_id that is new with every link. Now each identified visitor has VISITOR_LIMIT of their own,
  those who didn't identify share RATE_LIMIT (nothing tells them apart), and TOTAL_LIMIT still bounds everyone.
  Identities cost nothing to make, so this stops an ordinary visitor using up the others' requests, not a
  determined one (who can still reach TOTAL_LIMIT with several identities).

The page's real script runs against an offline victim node through the fake rnfs CLI of _pages_helpers, as in
test_h2_display_page_pins; NomadNet's environment is stood in for by remote_identity in os.environ.
"""
import io
import os
import unittest
from unittest import mock

from tests.security import _pages_helpers as helpers
from tests.security.test_h2_display_page_pins import CONTENT, MICRON, PageTestCase


def identity(n: int) -> str:
    """A visitor's identity hash as NomadNet passes it (32 hex digits)."""
    return f"{n:032x}"


class PrivateModeTest(PageTestCase):
    """The victim's own pins, in and out of private mode."""

    def setUp(self):
        super().setUp()
        self.notes = self.victim.add_stream(io.BytesIO(b"members-only notes\n"))["cid"]

    def test_operator_pin_is_not_shown_in_private_mode(self):
        self.victim.config.private = True
        output, calls = self.view(self.notes)
        self.assertNotIn("members-only notes", output, "a private node's pin was shown to a visitor")
        self.assertIn("isn't shared on this node", output)
        self.assertEqual([call[0] for call in calls], ["pin", "status"])  # nothing read

    def test_allowlisted_cid_is_shown_in_private_mode(self):
        self.victim.config.private = True
        page = self.victim.add_stream(io.BytesIO(MICRON))["cid"]
        helpers.allow(self.data_dir, page)
        output, calls = self.view(page)
        self.assertEqual(output, MICRON.decode())
        self.assertNotIn(["status"], calls)  # the allowlist is the operator's choice either way

    def test_operator_pin_is_shown_outside_private_mode(self):
        """Unchanged (a guard): a public node's own pins are still shown, as plain text."""
        output, _ = self.view(self.notes)
        self.assertIn("members-only notes", output)

    def test_status_without_private_counts_as_private(self):
        """Fails closed if `rnfs status` doesn't say (an older or odd daemon)."""
        run = helpers.fake_rnfs(self.victim, [])

        def without_private(argv, *args, **kwargs):
            result = run(argv, *args, **kwargs)
            if argv[1:] == ["status"]:
                result.stdout = b"".join(line for line in result.stdout.splitlines(True)
                                         if not line.startswith(b"private:"))
            return result

        output = helpers.exec_display_page(without_private, self.notes, self.data_dir)
        self.assertNotIn("members-only notes", output)
        self.assertIn("isn't shared on this node", output)


class VisitorRateLimitTest(PageTestCase):
    """Rate limits per identified visitor, for the others together, and for everyone."""

    def setUp(self):
        super().setUp()
        self.cid = self.provide(CONTENT)  # not shared: a view only lists pins

    def view_as(self, who: str | None) -> bool:
        """One view of the page by visitor who (None: one who didn't identify); True if it was served."""
        environment = {"remote_identity": who} if who is not None else {}
        with mock.patch.dict(os.environ, environment):
            if who is None:
                os.environ.pop("remote_identity", None)
            output, calls = self.view(self.cid)
        served = bool(calls)
        self.assertEqual(served, "too many requests" not in output)
        return served

    def test_one_identified_visitor_cannot_lock_out_the_others(self):
        """30 views (the old shared limit) from one identified visitor leave room for everyone else."""
        for _ in range(30):
            self.view_as(identity(1))
        self.assertTrue(self.view_as(None), "an anonymous visitor was locked out by one identified visitor")
        self.assertTrue(self.view_as(identity(2)), "a second visitor was locked out by the first")

    def test_anonymous_visitors_cannot_lock_out_identified_ones(self):
        for _ in range(self.page("RATE_LIMIT")):
            self.assertTrue(self.view_as(None))
        self.assertFalse(self.view_as(None))  # those who don't identify share their limit
        self.assertTrue(self.view_as(identity(1)), "an identified visitor was locked out by anonymous ones")

    def test_each_identified_visitor_has_a_limit(self):
        """A guard on the new limit: VISITOR_LIMIT views, then refused, whatever the others did."""
        served = sum(self.view_as(identity(1)) for _ in range(self.page("VISITOR_LIMIT") + 5))
        self.assertEqual(served, self.page("VISITOR_LIMIT"))

    def test_all_visitors_together_are_still_bounded(self):
        """A guard: new identities, which cost nothing, still meet TOTAL_LIMIT."""
        limit, total = self.page("VISITOR_LIMIT"), self.page("TOTAL_LIMIT")
        served = sum(self.view_as(identity(n)) for n in range(total // limit + 2) for _ in range(limit))
        self.assertEqual(served, total)

    def test_malformed_identity_counts_as_anonymous(self):
        for _ in range(self.page("RATE_LIMIT")):
            self.view_as(None)
        self.assertFalse(self.view_as("not-an-identity"))
        self.assertFalse(self.view_as(identity(1) + "00"))


if __name__ == "__main__":
    unittest.main()
