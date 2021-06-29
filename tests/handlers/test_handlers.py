import pytest
from pydbt.handlers import alert
from pydbt.handlers import monitor


def test_create_alert():
    try:
        alert.init()
    except AttributeError:
        pytest.fail("AttributeError should not be raised on alerting handler")


def test_create_monitor():
    try:
        monitor.init()
    except AttributeError:
        pytest.fail("AttributeError should not be raised on alerting handler")
