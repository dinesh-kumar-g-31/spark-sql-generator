import sqlglot


spark_ddl_types = {
    "string": "string",
    "integer": "bigint",
    "number": "double",
    "boolean": "boolean",
    "timestamp": "timestamp",
    "object": "struct<>",
}


def get_property_description(description: str = ""):
    return sqlglot.expressions.Literal.string(description).sql(dialect="spark")
