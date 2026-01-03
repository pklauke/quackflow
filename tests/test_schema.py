from quackflow.schema import Bool, Float, Int, List, Long, Schema, String, Struct, Timestamp


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


class TestComplexTypes:
    def test_list_of_strings(self):
        class WithTags(Schema):
            tags = List(String())

        fields = WithTags.fields()

        assert fields["tags"].duckdb_type == "VARCHAR[]"

    def test_list_of_ints(self):
        class WithScores(Schema):
            scores = List(Int())

        fields = WithScores.fields()

        assert fields["scores"].duckdb_type == "INTEGER[]"

    def test_nested_list(self):
        class WithMatrix(Schema):
            matrix = List(List(Float()))

        fields = WithMatrix.fields()

        assert fields["matrix"].duckdb_type == "DOUBLE[][]"

    def test_list_nullable(self):
        class WithOptionalTags(Schema):
            tags = List(String(), nullable=True)

        fields = WithOptionalTags.fields()

        assert fields["tags"].nullable is True

    def test_list_default(self):
        class WithDefaultTags(Schema):
            tags = List(String(), default=[])

        fields = WithDefaultTags.fields()

        assert fields["tags"].default == []

    def test_struct(self):
        class WithLocation(Schema):
            location = Struct(lat=Float(), lon=Float())

        fields = WithLocation.fields()

        assert fields["location"].duckdb_type == "STRUCT(lat DOUBLE, lon DOUBLE)"

    def test_struct_nullable(self):
        class WithOptionalLocation(Schema):
            location = Struct(lat=Float(), lon=Float(), nullable=True)

        fields = WithOptionalLocation.fields()

        assert fields["location"].nullable is True

    def test_list_of_struct(self):
        class WithLocations(Schema):
            locations = List(Struct(lat=Float(), lon=Float()))

        fields = WithLocations.fields()

        assert fields["locations"].duckdb_type == "STRUCT(lat DOUBLE, lon DOUBLE)[]"
