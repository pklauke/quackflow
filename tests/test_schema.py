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


class TestAvroGeneration:
    def test_string_to_avro(self):
        assert String().to_avro_type() == "string"

    def test_int_to_avro(self):
        assert Int().to_avro_type() == "int"

    def test_long_to_avro(self):
        assert Long().to_avro_type() == "long"

    def test_float_to_avro(self):
        assert Float().to_avro_type() == "double"

    def test_bool_to_avro(self):
        assert Bool().to_avro_type() == "boolean"

    def test_timestamp_to_avro(self):
        assert Timestamp().to_avro_type() == {"type": "long", "logicalType": "timestamp-micros"}

    def test_list_to_avro(self):
        assert List(String()).to_avro_type() == {"type": "array", "items": "string"}

    def test_nested_list_to_avro(self):
        assert List(List(Int())).to_avro_type() == {
            "type": "array",
            "items": {"type": "array", "items": "int"},
        }

    def test_struct_to_avro(self):
        result = Struct(lat=Float(), lon=Float()).to_avro_type()

        assert result["type"] == "record"
        assert result["name"] == "Struct"
        assert {"name": "lat", "type": "double"} in result["fields"]
        assert {"name": "lon", "type": "double"} in result["fields"]

    def test_nullable_to_avro(self):
        assert String(nullable=True).to_avro_type() == ["null", "string"]

    def test_nullable_complex_to_avro(self):
        result = List(String(), nullable=True).to_avro_type()

        assert result == ["null", {"type": "array", "items": "string"}]

    def test_schema_to_avro(self):
        import json

        class User(Schema):
            id = Int()
            name = String()

        avro_str = User.to_avro()
        avro = json.loads(avro_str)

        assert avro["type"] == "record"
        assert avro["name"] == "User"
        assert {"name": "id", "type": "int"} in avro["fields"]
        assert {"name": "name", "type": "string"} in avro["fields"]

    def test_schema_to_avro_with_nullable(self):
        import json

        class Event(Schema):
            id = Int()
            metadata = String(nullable=True)

        avro_str = Event.to_avro()
        avro = json.loads(avro_str)

        assert {"name": "id", "type": "int"} in avro["fields"]
        assert {"name": "metadata", "type": ["null", "string"]} in avro["fields"]

    def test_schema_to_avro_complex(self):
        import json

        class Location(Schema):
            lat = Float()
            lon = Float()
            tags = List(String())

        avro_str = Location.to_avro()
        avro = json.loads(avro_str)

        assert avro["name"] == "Location"
        assert {"name": "lat", "type": "double"} in avro["fields"]
        assert {"name": "lon", "type": "double"} in avro["fields"]
        assert {"name": "tags", "type": {"type": "array", "items": "string"}} in avro["fields"]
