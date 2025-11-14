import logging
import pytest

from application.factory import create_app


@pytest.fixture(scope="session")
def app():
    app = create_app("config.TestConfig")
    return app


@pytest.fixture(scope="session")
def client(app):
    with app.test_client() as client:
        with app.app_context():
            yield client


@pytest.fixture
def caplog_debug(caplog):
    """
    Fixture to capture all log levels including DEBUG.
    Use this instead of caplog when you need to test debug logs.
    """
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture
def log_checker():
    """
    Fixture that provides a utility to check if specific log messages were generated.

    Usage:
        def test_something(log_checker):
            # your code that logs
            log_checker.assert_log_contains("expected message", level="INFO")
    """

    class LogChecker:
        def __init__(self):
            self.handler = logging.handlers.MemoryHandler(capacity=1000)
            self.records = []

        def setup(self, logger_name=None):
            """Attach handler to logger to capture logs"""
            if logger_name:
                logger = logging.getLogger(logger_name)
            else:
                logger = logging.getLogger()
            logger.addHandler(self.handler)
            self.handler.setTarget(None)
            return self

        def assert_log_contains(self, message, level=None):
            """Assert that a log message containing 'message' was logged"""
            self.handler.flush()
            for record in self.handler.buffer:
                if message in record.getMessage():
                    if level is None or record.levelname == level:
                        return True
            raise AssertionError(
                f"Log message containing '{message}' "
                f"{'at level ' + level if level else ''} not found"
            )

        def get_logs(self, level=None):
            """Get all captured log records, optionally filtered by level"""
            self.handler.flush()
            if level:
                return [r for r in self.handler.buffer if r.levelname == level.upper()]
            return list(self.handler.buffer)

        def clear(self):
            """Clear captured logs"""
            self.handler.buffer.clear()

    return LogChecker()
