import datetime
import json
import typing


class _NoDefault:
    pass


NO_DEFAULT = _NoDefault()


class Field:
    _duckdb_type: str = ""
    _avro_type: str | dict[str, typing.Any] = ""

    def __init__(self, *, nullable: bool = False, default: typing.Any = NO_DEFAULT):
        self.nullable = nullable
        self.default = default

    @property
    def duckdb_type(self) -> str:
        return self._duckdb_type

    def to_avro_type(self) -> str | list[typing.Any] | dict[str, typing.Any]:
        base_type = self._avro_type
        if self.nullable:
            return ["null", base_type]
        return base_type


class String(Field):
    _duckdb_type = "VARCHAR"
    _avro_type = "string"

    def __init__(self, *, nullable: bool = False, default: str | _NoDefault = NO_DEFAULT):
        super().__init__(nullable=nullable, default=default)


class Int(Field):
    _duckdb_type = "INTEGER"
    _avro_type = "int"

    def __init__(self, *, nullable: bool = False, default: int | _NoDefault = NO_DEFAULT):
        super().__init__(nullable=nullable, default=default)


class Long(Field):
    _duckdb_type = "BIGINT"
    _avro_type = "long"

    def __init__(self, *, nullable: bool = False, default: int | _NoDefault = NO_DEFAULT):
        super().__init__(nullable=nullable, default=default)


class Float(Field):
    _duckdb_type = "DOUBLE"
    _avro_type = "double"

    def __init__(self, *, nullable: bool = False, default: float | _NoDefault = NO_DEFAULT):
        super().__init__(nullable=nullable, default=default)


class Bool(Field):
    _duckdb_type = "BOOLEAN"
    _avro_type = "boolean"

    def __init__(self, *, nullable: bool = False, default: bool | _NoDefault = NO_DEFAULT):
        super().__init__(nullable=nullable, default=default)


class Timestamp(Field):
    _duckdb_type = "TIMESTAMPTZ"
    _avro_type: dict[str, typing.Any] = {"type": "long", "logicalType": "timestamp-micros"}

    def __init__(self, *, nullable: bool = False, default: datetime.datetime | _NoDefault = NO_DEFAULT):
        super().__init__(nullable=nullable, default=default)


class List(Field):
    def __init__(
        self,
        element_type: Field,
        *,
        nullable: bool = False,
        default: typing.Sequence | _NoDefault = NO_DEFAULT,
    ):
        super().__init__(nullable=nullable, default=default)
        self.element_type = element_type

    @property
    def duckdb_type(self) -> str:
        return f"{self.element_type.duckdb_type}[]"

    def to_avro_type(self) -> str | list[typing.Any] | dict[str, typing.Any]:
        base_type: dict[str, typing.Any] = {
            "type": "array",
            "items": self.element_type.to_avro_type(),
        }
        if self.nullable:
            return ["null", base_type]
        return base_type


class Struct(Field):
    def __init__(
        self,
        *,
        nullable: bool = False,
        default: typing.Mapping | _NoDefault = NO_DEFAULT,
        **fields: Field,
    ):
        super().__init__(nullable=nullable, default=default)
        self.struct_fields = fields

    @property
    def duckdb_type(self) -> str:
        parts = [f"{name} {field.duckdb_type}" for name, field in self.struct_fields.items()]
        return f"STRUCT({', '.join(parts)})"

    def to_avro_type(self) -> str | list[typing.Any] | dict[str, typing.Any]:
        fields = [{"name": name, "type": field.to_avro_type()} for name, field in self.struct_fields.items()]
        base_type: dict[str, typing.Any] = {
            "type": "record",
            "name": "Struct",
            "fields": fields,
        }
        if self.nullable:
            return ["null", base_type]
        return base_type


class SchemaMeta(type):
    _fields: dict[str, Field]

    def __new__(mcs, name: str, bases: tuple, namespace: dict):
        cls = super().__new__(mcs, name, bases, namespace)
        if name != "Schema":
            field_dict = {}
            for key, value in namespace.items():
                if isinstance(value, Field):
                    field_dict[key] = value
            cls._fields = field_dict
        return cls


class Schema(metaclass=SchemaMeta):
    _fields: dict[str, Field] = {}

    @classmethod
    def fields(cls) -> dict[str, Field]:
        return cls._fields

    @classmethod
    def create_table_ddl(cls, table_name: str) -> str:
        columns = []
        for name, field in cls._fields.items():
            col = f"{name} {field.duckdb_type}"
            if not field.nullable:
                col += " NOT NULL"
            columns.append(col)
        return f"CREATE TABLE {table_name} ({', '.join(columns)})"

    @classmethod
    def to_avro(cls) -> str:
        fields = [{"name": name, "type": field.to_avro_type()} for name, field in cls._fields.items()]
        return json.dumps({"type": "record", "name": cls.__name__, "fields": fields})
