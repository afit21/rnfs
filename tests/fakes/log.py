"""The quiet-log fixture: a test module whose code under test logs through RNS silences it while its tests run."""
import RNS


def quiet_log() -> tuple:
    """setUpModule and tearDownModule functions for a test module (`setUpModule, tearDownModule = quiet_log()`):
    RNS logs only critical messages while the module's tests run, and its loglevel is restored after them."""
    saved = []

    def setUpModule():
        """Silence RNS's log below critical."""
        saved.append(RNS.loglevel)
        RNS.loglevel = RNS.LOG_CRITICAL

    def tearDownModule():
        """Restore RNS's loglevel."""
        RNS.loglevel = saved.pop()

    return setUpModule, tearDownModule
