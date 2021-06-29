import pytest
import datetime
from dateutil.tz import tzutc

from tests.fixture_loader import ResultMock

from pydbt.parsers.formatter import Formatter
from pydbt.types import Freshness, Message, Reporting

from dbt.contracts.results import TimingInfo
from dbt.contracts.graph.unparsed import TimePeriod


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            ResultMock.load_freshness_fixture,
            Freshness(
                unit=TimePeriod.hour,
                age=0.0,
                loader="pipelinewise",
                threshold=12,
            ),
        ),
        (
            ResultMock.load_freshness_fixture_warn,
            Freshness(
                unit=TimePeriod.hour,
                age=4.0,
                loader="pipelinewise",
                threshold=4,
            ),
        ),
        (
            ResultMock.load_freshness_fixture_fail,
            Freshness(
                unit=TimePeriod.day,
                age=608.0,
                loader="pipelinewise",
                threshold=10,
            ),
        ),
    ],
)
def test_get_freshness(input, expected):
    result = Formatter._get_freshness(input)
    assert result == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            ResultMock.load_model_fixture,
            Reporting(
                rows=5,
                execution_time=7.394317150115967,
                timing=[
                    TimingInfo(
                        name="compile",
                        started_at=datetime.datetime(
                            2021, 4, 7, 18, 18, 48, 185887, tzinfo=tzutc()
                        ),
                        completed_at=datetime.datetime(
                            2021, 4, 7, 18, 18, 49, 77401, tzinfo=tzutc()
                        ),
                    ),
                    TimingInfo(
                        name="execute",
                        started_at=datetime.datetime(
                            2021, 4, 7, 18, 18, 49, 77503, tzinfo=tzutc()
                        ),
                        completed_at=datetime.datetime(
                            2021, 4, 7, 18, 18, 55, 131509, tzinfo=tzutc()
                        ),
                    ),
                ],
                freshness=None,
            ),
        ),
        (
            ResultMock.load_test_fixture,
            Reporting(
                rows=None,
                execution_time=1.7002599239349365,
                timing=[
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
                        started_at=datetime.datetime(
                            2021, 4, 7, 17, 16, 33, 10483, tzinfo=tzutc()
                        ),
                        completed_at=datetime.datetime(
                            2021, 4, 7, 17, 16, 34, 123089, tzinfo=tzutc()
                        ),
                    ),
                ],
                freshness=None,
            ),
        ),
        (
            ResultMock.load_model_fixture_fail,
            Reporting(
                rows=None,
                execution_time=1.8677847385406494,
                timing=[],
                freshness=None,
            ),
        ),
    ],
)
def test_get_reporting(input, expected):
    result = Formatter._get_reporting(input)
    assert result == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            ResultMock.load_freshness_fixture,
            Message(
                level=11,
                title="Source freshness up to date.",
                error=None,
                message="Source `DATA_LAKE_DEV.CORE.PACKAGE_SEGMENT` is up to date",
                context={
                    "database": "data_lake_dev",
                    "schema": "core",
                    "path": "models/sources.yml",
                    "name": "package_segment",
                    "resource_type": "source",
                    "package_name": "transformations",
                    "filename": "models/sources.yml",
                    "abs_path": "/root/project/models/sources.yml",
                    "unit": TimePeriod.hour,
                    "age": 0.0,
                    "loader": "pipelinewise",
                    "threshold": 12,
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=2.3677098751068115,
                    timing=[
                        TimingInfo(
                            name="compile",
                            started_at=datetime.datetime(
                                2021, 4, 7, 18, 22, 6, 240889, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 18, 22, 6, 240894, tzinfo=tzutc()
                            ),
                        ),
                        TimingInfo(
                            name="execute",
                            started_at=datetime.datetime(
                                2021, 4, 7, 18, 22, 6, 240967, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 18, 22, 8, 608043, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=Freshness(
                        unit=TimePeriod.hour,
                        age=0.0,
                        loader="pipelinewise",
                        threshold=12,
                    ),
                ),
            ),
        )
    ],
)
def test_get_source_components(input, expected):
    result = Formatter._get_source_components(input)
    assert result == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            ResultMock.load_model_fixture,
            Message(
                level=11,
                title="Incremental model success.",
                error="SUCCESS 5",
                message="*[SUCCESS]* model `WAREHOUSE_LOCAL.HBL_BOOKING_ODS.DBX_BOOKING` that depends on `SOURCE.TRANSFORMATIONS.BOOKING.DBX_BOOKING`",
                context={
                    "database": "warehouse_local",
                    "schema": "hbl_booking_ods",
                    "path": "booking/dbx_booking.sql",
                    "name": "dbx_booking",
                    "resource_type": "model",
                    "package_name": "transformations",
                    "tags": "daily, sunrise, morning, noon, afternoon, evening, pii",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/run/transformations/models/booking/dbx_booking.sql",
                    "materialized": "incremental",
                },
                reporting=Reporting(
                    rows=5,
                    execution_time=7.394317150115967,
                    timing=[
                        TimingInfo(
                            name="compile",
                            started_at=datetime.datetime(
                                2021, 4, 7, 18, 18, 48, 185887, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 18, 18, 49, 77401, tzinfo=tzutc()
                            ),
                        ),
                        TimingInfo(
                            name="execute",
                            started_at=datetime.datetime(
                                2021, 4, 7, 18, 18, 49, 77503, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 18, 18, 55, 131509, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=None,
                ),
            ),
        ),
        (
            ResultMock.load_model_fixture_fail,
            Message(
                level=14,
                title="Incremental model error.",
                error="Database Error in model dbx_booking (models/booking/dbx_booking.sql)\n  001003 (42000): SQL compilation error:\n  syntax error line 27 at position 0 unexpected 'FROM'.\n  compiled SQL at target/compiled/transformations/models/booking/dbx_booking.sql",
                message="*[ERROR]* model `WAREHOUSE_LOCAL.HBL_BOOKING_ODS.DBX_BOOKING` that depends on `SOURCE.TRANSFORMATIONS.BOOKING.DBX_BOOKING`",
                context={
                    "database": "warehouse_local",
                    "schema": "hbl_booking_ods",
                    "path": "booking/dbx_booking.sql",
                    "name": "dbx_booking",
                    "resource_type": "model",
                    "package_name": "transformations",
                    "tags": "daily, sunrise, morning, noon, afternoon, evening, pii",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/compiled/transformations/models/booking/dbx_booking.sql",
                    "materialized": "incremental",
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=1.8677847385406494,
                    timing=[],
                    freshness=None,
                ),
            ),
        ),
    ],
)
def test_get_model_components(input, expected):
    result = Formatter._get_model_components(input)
    assert result == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            ResultMock.load_test_fixture,
            Message(
                level=11,
                title="[PASS] Test not_null_airport_id",
                error=None,
                message="*[PASS]* Test not_null_airport_id in `WAREHOUSE_LOCAL`",
                context={
                    "database": "warehouse_local",
                    "schema": "hbl_none",
                    "path": "schema_test/not_null_airport_id.sql",
                    "name": "not_null_airport_id",
                    "resource_type": "test",
                    "package_name": "transformations",
                    "tags": "schema, daily, sunrise, morning, noon, afternoon, evening",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/compiled/transformations/models/location/schema.yml/schema_test/not_null_airport_id.sql",
                    "materialized": "test",
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=1.7002599239349365,
                    timing=[
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
                            started_at=datetime.datetime(
                                2021, 4, 7, 17, 16, 33, 10483, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 17, 16, 34, 123089, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=None,
                ),
            ),
        ),
        (
            ResultMock.load_test_fixture_fail,
            Message(
                level=14,
                title="[FAIL] Test not_null_package_package_id",
                error="Got 1 results, expected 0",
                message="*[FAIL]* Test not_null_package_package_id in `WAREHOUSE_LOCAL`",
                context={
                    "database": "warehouse_local",
                    "schema": "hbl_none",
                    "path": "schema_test/not_null_package_package_id.sql",
                    "name": "not_null_package_package_id",
                    "resource_type": "test",
                    "package_name": "transformations",
                    "tags": "schema, daily, sunrise, morning, noon, afternoon, evening",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/compiled/transformations/models/core/schema.yml/schema_test/not_null_package_package_id.sql",
                    "materialized": "test",
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=1.898899793624878,
                    timing=[
                        TimingInfo(
                            name="compile",
                            started_at=datetime.datetime(
                                2021, 4, 7, 17, 52, 26, 891117, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 17, 52, 26, 897454, tzinfo=tzutc()
                            ),
                        ),
                        TimingInfo(
                            name="execute",
                            started_at=datetime.datetime(
                                2021, 4, 7, 17, 52, 26, 897677, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 17, 52, 28, 319413, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=None,
                ),
            ),
        ),
        (
            ResultMock.load_test_fixture_warn,
            Message(
                level=13,
                title="[WARN] Test not_null_package_package_id",
                error="Got 1 results, expected 0",
                message="*[WARN]* Test not_null_package_package_id in `WAREHOUSE_LOCAL`",
                context={
                    "database": "warehouse_local",
                    "schema": "hbl_none",
                    "path": "schema_test/not_null_package_package_id.sql",
                    "name": "not_null_package_package_id",
                    "resource_type": "test",
                    "package_name": "transformations",
                    "tags": "schema, daily, sunrise, morning, noon, afternoon, evening",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/compiled/transformations/models/core/schema.yml/schema_test/not_null_package_package_id.sql",
                    "materialized": "test",
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=1.898899793624878,
                    timing=[
                        TimingInfo(
                            name="compile",
                            started_at=datetime.datetime(
                                2021, 4, 7, 17, 52, 26, 891117, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 17, 52, 26, 897454, tzinfo=tzutc()
                            ),
                        ),
                        TimingInfo(
                            name="execute",
                            started_at=datetime.datetime(
                                2021, 4, 7, 17, 52, 26, 897677, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 17, 52, 28, 319413, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=None,
                ),
            ),
        ),
    ],
)
def test_get_test_components(input, expected):
    result = Formatter._get_test_components(input)
    assert result == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            ResultMock.load_snaphot_fixture,
            Message(
                level=11,
                title="Snapshot completed",
                error=None,
                message="*[SUCCESS]* snapshot with target `WAREHOUSE_LOCAL.PERSON_DERIVED_ODS.PERSON_COMPANY_SNAPSHOT`",
                context={
                    "database": "warehouse_local",
                    "schema": "person_derived_ods",
                    "path": "person_company_snapshot.sql",
                    "name": "person_company_snapshot",
                    "resource_type": "snapshot",
                    "package_name": "transformations",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/run/transformations/person_company_snapshot.sql",
                    "materialized": "snapshot",
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=23.130106925964355,
                    timing=[
                        TimingInfo(
                            name="compile",
                            started_at=datetime.datetime(
                                2019, 12, 12, 19, 15, 30, 347444, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2019, 12, 12, 19, 15, 32, 180207, tzinfo=tzutc()
                            ),
                        ),
                        TimingInfo(
                            name="execute",
                            started_at=datetime.datetime(
                                2019, 12, 12, 19, 15, 32, 180269, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2019, 12, 12, 19, 15, 53, 477112, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=None,
                ),
            ),
        )
    ],
)
def test_get_snapshot_components(input, expected):
    result = Formatter._get_snapshot_components(input)
    assert result == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            ResultMock.load_model_fixture,
            Message(
                level=11,
                title="Incremental model success.",
                error="SUCCESS 5",
                message="*[SUCCESS]* model `WAREHOUSE_LOCAL.HBL_BOOKING_ODS.DBX_BOOKING` that depends on `SOURCE.TRANSFORMATIONS.BOOKING.DBX_BOOKING`",
                context={
                    "database": "warehouse_local",
                    "schema": "hbl_booking_ods",
                    "path": "booking/dbx_booking.sql",
                    "name": "dbx_booking",
                    "resource_type": "model",
                    "package_name": "transformations",
                    "tags": "daily, sunrise, morning, noon, afternoon, evening, pii",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/run/transformations/models/booking/dbx_booking.sql",
                    "materialized": "incremental",
                },
                reporting=Reporting(
                    rows=5,
                    execution_time=7.394317150115967,
                    timing=[
                        TimingInfo(
                            name="compile",
                            started_at=datetime.datetime(
                                2021, 4, 7, 18, 18, 48, 185887, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 18, 18, 49, 77401, tzinfo=tzutc()
                            ),
                        ),
                        TimingInfo(
                            name="execute",
                            started_at=datetime.datetime(
                                2021, 4, 7, 18, 18, 49, 77503, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 18, 18, 55, 131509, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=None,
                ),
            ),
        ),
        (
            ResultMock.load_test_fixture,
            Message(
                level=11,
                title="[PASS] Test not_null_airport_id",
                error=None,
                message="*[PASS]* Test not_null_airport_id in `WAREHOUSE_LOCAL`",
                context={
                    "database": "warehouse_local",
                    "schema": "hbl_none",
                    "path": "schema_test/not_null_airport_id.sql",
                    "name": "not_null_airport_id",
                    "resource_type": "test",
                    "package_name": "transformations",
                    "tags": "schema, daily, sunrise, morning, noon, afternoon, evening",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/compiled/transformations/models/location/schema.yml/schema_test/not_null_airport_id.sql",
                    "materialized": "test",
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=1.7002599239349365,
                    timing=[
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
                            started_at=datetime.datetime(
                                2021, 4, 7, 17, 16, 33, 10483, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 17, 16, 34, 123089, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=None,
                ),
            ),
        ),
        (
            ResultMock.load_freshness_fixture,
            Message(
                level=11,
                title="Source freshness up to date.",
                error=None,
                message="Source `DATA_LAKE_DEV.CORE.PACKAGE_SEGMENT` is up to date",
                context={
                    "database": "data_lake_dev",
                    "schema": "core",
                    "path": "models/sources.yml",
                    "name": "package_segment",
                    "resource_type": "source",
                    "package_name": "transformations",
                    "filename": "models/sources.yml",
                    "abs_path": "/root/project/models/sources.yml",
                    "unit": TimePeriod.hour,
                    "age": 0.0,
                    "loader": "pipelinewise",
                    "threshold": 12,
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=2.3677098751068115,
                    timing=[
                        TimingInfo(
                            name="compile",
                            started_at=datetime.datetime(
                                2021, 4, 7, 18, 22, 6, 240889, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 18, 22, 6, 240894, tzinfo=tzutc()
                            ),
                        ),
                        TimingInfo(
                            name="execute",
                            started_at=datetime.datetime(
                                2021, 4, 7, 18, 22, 6, 240967, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2021, 4, 7, 18, 22, 8, 608043, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=Freshness(
                        unit=TimePeriod.hour,
                        age=0.0,
                        loader="pipelinewise",
                        threshold=12,
                    ),
                ),
            ),
        ),
        (
            ResultMock.load_snaphot_fixture,
            Message(
                level=11,
                title="Snapshot completed",
                error=None,
                message="*[SUCCESS]* snapshot with target `WAREHOUSE_LOCAL.PERSON_DERIVED_ODS.PERSON_COMPANY_SNAPSHOT`",
                context={
                    "database": "warehouse_local",
                    "schema": "person_derived_ods",
                    "path": "person_company_snapshot.sql",
                    "name": "person_company_snapshot",
                    "resource_type": "snapshot",
                    "package_name": "transformations",
                    "filename": "/root/project",
                    "abs_path": "/root/project/target/run/transformations/person_company_snapshot.sql",
                    "materialized": "snapshot",
                },
                reporting=Reporting(
                    rows=None,
                    execution_time=23.130106925964355,
                    timing=[
                        TimingInfo(
                            name="compile",
                            started_at=datetime.datetime(
                                2019, 12, 12, 19, 15, 30, 347444, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2019, 12, 12, 19, 15, 32, 180207, tzinfo=tzutc()
                            ),
                        ),
                        TimingInfo(
                            name="execute",
                            started_at=datetime.datetime(
                                2019, 12, 12, 19, 15, 32, 180269, tzinfo=tzutc()
                            ),
                            completed_at=datetime.datetime(
                                2019, 12, 12, 19, 15, 53, 477112, tzinfo=tzutc()
                            ),
                        ),
                    ],
                    freshness=None,
                ),
            ),
        ),
    ],
)
def test_format(input, expected):
    result = Formatter.format(input)
    assert result == expected
