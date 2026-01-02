import typing


class Field:
    duckdb_type: str = ""

    def __init__(self, *, nullable: bool = False, default: typing.Any = None):
        self.nullable = nullable
        self.default = default


class String(Field):
    duckdb_type = "VARCHAR"


class Int(Field):
    duckdb_type = "INTEGER"


class Long(Field):
    duckdb_type = "BIGINT"


class Float(Field):
    duckdb_type = "DOUBLE"


class Bool(Field):
    duckdb_type = "BOOLEAN"


class Timestamp(Field):
    duckdb_type = "TIMESTAMP"


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
