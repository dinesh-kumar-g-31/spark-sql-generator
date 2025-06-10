from spark_sql_generator.spark_sql_generator import SQLColumnGenerator

print("Testing SQLColumnGenerator...")


def test_sql_column_generator():

    input_data = [
        {
            "operation": "ADD",
            "columns": [
                {"path": "person.name", "value": "string", "doc": "Person's full name"},
                {"path": "person.age", "value": "integer", "doc": "Person's age"},
            ],
        },
        {"operation": "REMOVE", "columns": ["person.address"]},
        {
            "operation": "REPLACE",
            "columns": [
                {"path": "person.email", "value": "string", "target_field": "type"}
            ],
        },
        {"operation": "ADD", "columns": [{"path": "person.phone", "value": "string"}]},
        {
            "operation": "REORDER",
            "columns": [{"path": "person.name", "moveafter": "first"}],
        },
        {"operation": "MOVE", "columns": [{"path": "person.old_id", "value": "id"}]},
    ]
    generator = SQLColumnGenerator(input_data)
    sql = generator.generate_sql()
    print("Generated SQL:", sql)


test_sql_column_generator()
