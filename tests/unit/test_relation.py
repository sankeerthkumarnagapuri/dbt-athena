import pytest

from dbt.adapters.athena.relation import AthenaRelation, TableType, get_table_type

from .constants import DATA_CATALOG_NAME, DATABASE_NAME

TABLE_NAME = "test_table"


class TestRelation:
    def test__get_relation_type_table(self):
        assert get_table_type({"Name": "name", "TableType": "table"}) == TableType.TABLE

    def test__get_relation_type_with_no_type(self):
        with pytest.raises(ValueError):
            get_table_type({"Name": "name"})

    def test__get_relation_type_view(self):
        assert get_table_type({"Name": "name", "TableType": "VIRTUAL_VIEW"}) == TableType.VIEW

    def test__get_relation_type_iceberg(self):
        assert (
            get_table_type({"Name": "name", "TableType": "EXTERNAL_TABLE", "Parameters": {"table_type": "ICEBERG"}})
            == TableType.ICEBERG
        )


class TestAthenaRelation:
    def test_render_hive_uses_hive_style_quotation(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        assert relation.render_hive() == f"`{DATA_CATALOG_NAME}`.`{DATABASE_NAME}`.`{TABLE_NAME}`"

    def test_render_hive_resets_quote_character_after_call(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        relation.render_hive()
        assert relation.render() == f'"{DATA_CATALOG_NAME}"."{DATABASE_NAME}"."{TABLE_NAME}"'

    def test_render_pure_resets_quote_character_after_call(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        assert relation.render_pure() == f"{DATA_CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}"
