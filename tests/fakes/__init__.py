"""Test doubles shared by several test modules (not test modules themselves): FakeClient for PeerClient
(tests/fakes/client.py) and the quiet-log fixture (tests/fakes/log.py). tests/test_fake_contract.py checks the fakes
against the real classes they stand in for, so a change there can't leave tests passing against a stale fake."""
