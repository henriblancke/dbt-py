import datetime
from dateutil.tz import tzutc
from dbt.contracts.results import TimingInfo

from pydbt.utils import tools


def test_rename_keys():
    src = {"foo": 0}
    mapper = {"foo": "bar"}
    res = tools.rename_keys(src, mapper)

    assert list(res.keys())[0] == "bar"


def test_omit_keys():
    src = {"foo": 0, "bar": 1}
    res = tools.omit_keys(src, ["bar"])

    assert len(res.keys()) == 1
    assert list(res.keys()) == ["foo"]


def test_freshness_age_to_unit():
    res = tools.freshness_age_to_unit(7200, "hour")
    assert res == 2

    res = tools.freshness_age_to_unit(120, "minute")
    assert res == 2.0

    res = tools.freshness_age_to_unit(120, "second")
    assert res == 120.00


def test_get_execution_time():
    timing_fixture = [
        TimingInfo(
            name="compile",
            started_at=datetime.datetime(
                2021, 4, 7, 17, 16, 32, 987621, tzinfo=tzutc()
            ),
            completed_at=datetime.datetime(
                2021, 4, 7, 17, 16, 33, 10372, tzinfo=tzutc()
            ),
        ),
        TimingInfo(
            name="execute",
            started_at=datetime.datetime(2021, 4, 7, 17, 16, 33, 10483, tzinfo=tzutc()),
            completed_at=datetime.datetime(
                2021, 4, 7, 17, 16, 34, 123089, tzinfo=tzutc()
            ),
        ),
    ]

    res = tools.get_execution_datetime(timing_fixture)
    assert res == 1617815794.0


def test_get_full_db_id():
    res = tools.get_full_db_id("db", "schema", "table")
    assert res == "DB.SCHEMA.TABLE"

    res = tools.get_full_db_id("db", "schema", "table", capitalize=False)
    assert res == "db.schema.table"

    res = tools.get_full_db_id("db", "SCHEMA", "table", capitalize=False)
    assert res == "db.SCHEMA.table"
