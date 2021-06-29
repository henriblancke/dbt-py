import pytest

from tests.fixture_loader import ResultMock

from pydbt.parsers import node


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            ResultMock.load_model_fixture,
            {
                "abs_path": "/root/project/target/run/transformations/models/booking/dbx_booking.sql",
                "database": "warehouse_local",
                "filename": "/root/project",
                "materialized": "incremental",
                "name": "dbx_booking",
                "package_name": "transformations",
                "path": "booking/dbx_booking.sql",
                "resource_type": "model",
                "schema": "hbl_booking_ods",
                "tags": "daily, sunrise, morning, noon, afternoon, evening, pii",
            },
        ),
        (
            ResultMock.load_test_fixture,
            {
                "abs_path": "/root/project/target/compiled/transformations/models/location/schema.yml/schema_test/not_null_airport_id.sql",
                "database": "warehouse_local",
                "filename": "/root/project",
                "materialized": "test",
                "name": "not_null_airport_id",
                "package_name": "transformations",
                "path": "schema_test/not_null_airport_id.sql",
                "resource_type": "test",
                "schema": "hbl_none",
                "tags": "schema, daily, sunrise, morning, noon, afternoon, evening",
            },
        ),
        (
            ResultMock.load_snaphot_fixture,
            {
                "abs_path": "/root/project/target/run/transformations/person_company_snapshot.sql",
                "database": "warehouse_local",
                "filename": "/root/project",
                "materialized": "snapshot",
                "name": "person_company_snapshot",
                "package_name": "transformations",
                "path": "person_company_snapshot.sql",
                "resource_type": "snapshot",
                "schema": "person_derived_ods",
            },
        ),
        (
            ResultMock.load_freshness_fixture,
            {
                "abs_path": "/root/project/models/sources.yml",
                "database": "data_lake_dev",
                "filename": "models/sources.yml",
                "name": "package_segment",
                "package_name": "transformations",
                "path": "models/sources.yml",
                "resource_type": "source",
                "schema": "core",
            },
        ),
    ],
)
def test_parse_node(input, expected):
    result = node.parse_node(input)
    assert result == expected
