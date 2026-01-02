from quackflow.schema import Bool, Float, Int, Long, Schema, String, Timestamp


class TestSchemaDefinition:
    def test_simple_schema(self):
        class User(Schema):
            id = Int()
            name = String()

        fields = User.fields()

        assert len(fields) == 2
        assert fields["id"].duckdb_type == "INTEGER"
        assert fields["name"].duckdb_type == "VARCHAR"

    def test_nullable_field(self):
        class Event(Schema):
            id = Int()
            metadata = String(nullable=True)

        fields = Event.fields()

        assert fields["id"].nullable is False
        assert fields["metadata"].nullable is True

    def test_default_value(self):
        class Config(Schema):
            retries = Int(default=3)
            tag = String(default="default")

        fields = Config.fields()

        assert fields["retries"].default == 3
        assert fields["tag"].default == "default"

    def test_all_basic_types(self):
        class AllTypes(Schema):
            a = String()
            b = Int()
            c = Long()
            d = Float()
            e = Bool()
            f = Timestamp()

        fields = AllTypes.fields()

        assert fields["a"].duckdb_type == "VARCHAR"
        assert fields["b"].duckdb_type == "INTEGER"
        assert fields["c"].duckdb_type == "BIGINT"
        assert fields["d"].duckdb_type == "DOUBLE"
        assert fields["e"].duckdb_type == "BOOLEAN"
        assert fields["f"].duckdb_type == "TIMESTAMP"


class TestDDLGeneration:
    def test_create_table_ddl(self):
        class User(Schema):
            id = Int()
            name = String()
            score = Float(nullable=True)

        ddl = User.create_table_ddl("users")

        assert ddl == "CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR NOT NULL, score DOUBLE)"
