# Spark SQL Generator

A Python library for generating Spark SQL DDL statements from structured input data. This package helps automate the creation and modification of Spark SQL tables, supporting operations such as adding, removing, moving, reordering, and replacing columns, including support for nested structures and arrays.

## Features

- Generate `ALTER TABLE` statements for Spark SQL 
- Support for complex nested data structures (structs and arrays)
- Path-based field addressing for easy column manipulation
- Handling of schema modifications while preserving field order
- Comprehensive documentation for field types, comments, and paths
- Easily extensible for custom data types and operations

## Installation

You can install the package directly from PyPI:

```sh
pip install spark-sql-generator
```

Or install from source:

```sh
# Clone the repository
git clone https://github.com/dinesh-kumar-g-31/spark-sql-generator.git
cd spark-sql-generator

# Install locally
pip install .
```

## Usage

### Basic Example

Here's a simple example showing how to add columns to a table:

```python
from spark_sql_generator import SQLColumnGenerator

# Define your schema modifications
input_data = [
    {
        "operation": "ADD",
        "columns": [
            {"path": "id", "value": "int", "doc": "Primary key"},
            {"path": "name", "value": "string", "doc": "User name"},
            {"path": "address", "value": "object", "doc": "User address"},
            {"path": "address.street", "value": "string", "doc": "Street name"},
            {"path": "address.zip", "value": "int", "doc": "ZIP code"},
        ],
    }
]

# Generate the SQL statements
generator = SQLColumnGenerator(input_data)
sql_statements = generator.generate_sql()

# Print or execute the statements
for stmt in sql_statements:
    print(stmt)
```

### Comprehensive Example

This example demonstrates all supported operations:

```python
from spark_sql_generator import SQLColumnGenerator

# Define schema modifications with all supported operations
input_data = [
    # ADD: Adding various column types including nested structures
    {
        "operation": "ADD",
        "columns": [
            {"path": "person.name", "value": "string", "doc": "Person's name"},
            {"path": "person.age", "value": "integer", "doc": "Person's age"},
            {"path": "person.height", "value": "number", "doc": "Height in meters"},
            {"path": "person.active", "value": "boolean", "doc": "Is active"},
            # Nested object
            {"path": "person.address", "value": "object", "doc": "Person's address"},
            {"path": "person.address.street", "value": "string", "doc": "Street name"},
            {"path": "person.address.city", "value": "string", "doc": "City name"},
            # Array of primitives
            {"path": "person.tags", "value": "array", "arr_type": "string", "doc": "Tags"},
            # Array of objects
            {
                "path": "person.contacts", 
                "value": "array",
                "doc": "Contact list",
                "nestedFields": {
                    "name": "type",
                    "type": "string", 
                    "doc": "Contact type"
                }
            },
        ],
    },
    
    # REMOVE: Dropping columns
    {"operation": "REMOVE", "columns": ["person.obsolete_field"]},
    
    # REPLACE: Changing column properties
    {
        "operation": "REPLACE", 
        "columns": [
            {"path": "person.email", "value": "string", "target_field": "type"}
        ],
    },
    
    # MOVE: Renaming columns
    {
        "operation": "MOVE",
        "columns": [{"path": "person.old_name", "value": "full_name"}],
    },
    
    # REORDER: Changing column position
    {
        "operation": "REORDER",
        "columns": [{"path": "person.id", "moveafter": "first"}],
    },
    
    # Another ADD operation (separate because operation type changed)
    {
        "operation": "ADD",
        "columns": [
            {"path": "person.created_at", "value": "timestamp", "doc": "Creation timestamp"}
        ],
    },
]

# Generate the SQL statements
generator = SQLColumnGenerator(input_data)
sql = generator.generate_sql()
print("Generated SQL:", sql)
```

### Multi-Level Array Example

Here's an example of working with multi-level arrays and complex struct types:

```python
from spark_sql_generator import SQLColumnGenerator

# Example of multi-level arrays with structs
input_data = [
    {
        "operation": "ADD",
        "columns": [
            {
                "path": "diagnosticReport", 
                "value": "object", 
                "doc": "Diagnostic analysis report"
            },
            {
                "path": "diagnosticReport.testResults", 
                "value": "array",
                "doc": "Array of test results",
                "nestedFields": {
                    "testId": {
                        "type": "string",
                        "doc": "Unique test identifier"
                    },
                    "timestamp": {
                        "type": "timestamp",
                        "doc": "Time when test was executed"
                    },
                    "status": {
                        "type": "string",
                        "doc": "Test status (pass/fail/warning)"
                    },
                    "metrics": {
                        "type": "array",
                        "doc": "Test metrics",
                        "nestedFields": {
                            "name": {
                                "type": "string",
                                "doc": "Metric name"
                            },
                            "value": {
                                "type": "double",
                                "doc": "Metric value"
                            },
                            "threshold": {
                                "type": "double",
                                "doc": "Acceptance threshold"
                            },
                            "details": {
                                "type": "array",
                                "doc": "Detailed breakdown",
                                "nestedFields": {
                                    "component": {
                                        "type": "string",
                                        "doc": "Component name"
                                    },
                                    "measurement": {
                                        "type": "double",
                                        "doc": "Component measurement"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]
    }
]

generator = SQLColumnGenerator(input_data)
sql = generator.generate_sql()
print(sql)
```

This generates SQL for a structure with array of structs containing arrays of structs containing arrays of structs:

```sql
ALTER TABLE {table_name} 
    ADD COLUMNS (
    `diagnosticReport` struct<> COMMENT 'Diagnostic analysis report',
    `diagnosticReport`.`testResults` array<struct<
        testId: string COMMENT 'Unique test identifier',
        timestamp: timestamp COMMENT 'Time when test was executed',
        status: string COMMENT 'Test status (pass/fail/warning)',
        metrics: array<struct<
            name: string COMMENT 'Metric name',
            value: double COMMENT 'Metric value',
            threshold: double COMMENT 'Acceptance threshold',
            details: array<struct<
                component: string COMMENT 'Component name',
                measurement: double COMMENT 'Component measurement'
            >> COMMENT 'Detailed breakdown'
        >> COMMENT 'Test metrics'
    >> COMMENT 'Array of test results'
    )
```

## Supported Operations

### ADD
Adds new columns to a table. Supports:
- Simple columns: `{"path": "column_name", "value": "data_type", "doc": "comment"}`
- Nested columns: Use dot notation in path, e.g., `"path": "parent.child"`
- Object columns: `{"path": "obj_name", "value": "object", "doc": "comment"}`
- Array columns:
  - Simple array: `{"path": "array_name", "value": "array", "arr_type": "element_type", "doc": "comment"}`
  - Array of objects: Use the `nestedFields` property to define object structure

### REMOVE
Drops existing columns from a table:
```python
{"operation": "REMOVE", "columns": ["column_path1", "column_path2"]}
```

### MOVE
Renames columns while preserving their structure:
```python
{"operation": "MOVE", "columns": [{"path": "old_path", "value": "new_name"}]}
```

### REORDER
Changes the position of columns:
```python
{"operation": "REORDER", "columns": [{"path": "column_path", "moveafter": "position"}]}
```
Position can be:
- `"first"`: Move to the beginning
- Any valid column path: Move after the specified column

### REPLACE
Modifies column properties such as type or comment:
```python
{"operation": "REPLACE", "columns": [{"path": "column_path", "value": "new_value", "target_field": "field_to_change"}]}
```
Target fields can be:
- `"type"`: Change the data type
- `"comment"`: Update the column comment

## Supported Data Types

The library supports the following data types:
- `string`: Text data
- `integer` / `int`: Integer values
- `bigint`: Large integers
- `number` / `float` / `double`: Floating-point values
- `boolean`: Boolean values
- `timestamp`: Date/time values
- `object`: Nested structures
- `array`: Collections of elements

## Advanced Usage

### Complex Nested Structures

The library can handle deeply nested structures, including arrays of structs and multi-level nesting. Here's an example of defining a complex nested structure:

```python
from spark_sql_generator import SQLColumnGenerator

# Example of complex nested structure with arrays of structs
input_data = [
    {
        "operation": "ADD",
        "columns": [
            {
                "path": "analytics", 
                "value": "object", 
                "doc": "Analytics data container"
            },
            {
                "path": "analytics.reportData", 
                "value": "object", 
                "doc": "Report data structure"
            },
            {
                "path": "analytics.reportData.validations", 
                "value": "array",
                "doc": "Validation results array",
                "nestedFields": {
                    "id": {
                        "type": "string",
                        "doc": "Validation identifier"
                    },
                    "status": {
                        "type": "string",
                        "doc": "Validation status (pass/fail)"
                    },
                    "details": {
                        "type": "object",
                        "doc": "Validation details",
                        "nestedFields": {
                            "code": {
                                "type": "string",
                                "doc": "Error code if failed"
                            },
                            "count": {
                                "type": "bigint",
                                "doc": "Count of validated items"
                            },
                            "metadata": {
                                "type": "array",
                                "arrayType": "string",
                                "doc": "Additional metadata"
                            }
                        }
                    }
                }
            }
        ]
    }
]

generator = SQLColumnGenerator(input_data)
sql = generator.generate_sql()
print(sql)
```

This would generate SQL for a structure like:

```sql
ALTER TABLE {table_name} 
    ADD COLUMNS (
    `analytics` struct<> COMMENT 'Analytics data container',
    `analytics`.`reportData` struct<> COMMENT 'Report data structure',
    `analytics`.`reportData`.`validations` array<struct<
        id: string COMMENT 'Validation identifier',
        status: string COMMENT 'Validation status (pass/fail)',
        details: struct<
            code: string COMMENT 'Error code if failed',
            count: bigint COMMENT 'Count of validated items',
            metadata: array<string> COMMENT 'Additional metadata'
        > COMMENT 'Validation details'
    >> COMMENT 'Validation results array'
    )
```

### Rules for Complex Structures

When working with complex nested structures:

1. **Define parent structures first**: Always define object containers before their nested fields
2. **Use consistent paths**: Ensure path hierarchy is consistent (e.g., `parent.child.grandchild`)
3. **Nested arrays of structs**: Use the `nestedFields` property to define the structure of array elements
4. **Multi-level nesting**: You can nest objects within objects and arrays within structs to any depth
5. **Path escaping**: For field names containing special characters, proper escaping is automatically applied

## Output Example

The generated SQL statements will look like this:

```sql
-- Adding columns
ALTER TABLE {table_name} 
    ADD COLUMNS (
    `person`.`name` string COMMENT 'Person\'s name',
    `person`.`age` bigint COMMENT 'Person\'s age',
    `person`.`height` double COMMENT 'Height in meters',
    `person`.`active` boolean COMMENT 'Is active',
    `person`.`address` struct<> COMMENT 'Person\'s address',
    `person`.`address`.`street` string COMMENT 'Street name',
    `person`.`address`.`city` string COMMENT 'City name',
    `person`.`tags` array<string> COMMENT 'Tags',
    `person`.`contacts` array<struct<type:string>> COMMENT 'Contact list'
    )

-- Removing a column
ALTER TABLE {table_name} DROP COLUMN `person`.`obsolete_field`

-- Replacing column properties
ALTER TABLE {table_name} ALTER COLUMN `person`.`email` TYPE string

-- Moving/renaming columns
ALTER TABLE {table_name} RENAME COLUMN `person`.`old_name` TO `full_name`

-- Reordering columns
ALTER TABLE {table_name} ALTER COLUMN `person`.`id` FIRST

-- Adding more columns
ALTER TABLE {table_name} 
    ADD COLUMNS (
    `person`.`created_at` timestamp COMMENT 'Creation timestamp'
    )
```

## Best Practices

1. **Group related operations**: Group related column changes under a single operation type when possible
2. **Handle nested structures properly**: Use dot notation for nested columns and make sure to define parent objects
3. **Use meaningful column comments**: Add detailed descriptions to improve schema documentation
4. **Review SQL before execution**: Always review generated SQL before applying to production databases
5. **Consider operation order**: Some operations may depend on others, so order matters
6. **Define complex structures incrementally**: For deeply nested structures, define the hierarchy layer by layer
7. **Validate generated SQL for complex structures**: Always test complex nested structures in a development environment
8. **Use proper data types for nested structures**: Choose the appropriate data types for nested fields
9. **Understand memory implications**: Complex nested structures can have significant memory requirements

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Troubleshooting

### Common Issues with Complex Structures

1. **Missing Parent Objects**

   **Problem**: Error when adding nested fields without defining parent objects.
   
   **Solution**: Always define parent objects before their nested fields:
   
   ```python
   # Correct approach
   {"path": "parent", "value": "object", "doc": "Parent object"}
   {"path": "parent.child", "value": "string", "doc": "Child field"}
   ```

2. **Inconsistent Path Hierarchy**

   **Problem**: Unexpected SQL output due to inconsistent path naming.
   
   **Solution**: Maintain consistent path hierarchies throughout your definition:
   
   ```python
   # Correct approach - consistent paths
   {"path": "report.metrics.value", "value": "double", "doc": "Metric value"}
   {"path": "report.metrics.name", "value": "string", "doc": "Metric name"}
   
   # Incorrect approach - inconsistent paths
   {"path": "report.metrics.value", "value": "double", "doc": "Metric value"}
   {"path": "report_metrics_name", "value": "string", "doc": "Metric name"}
   ```

3. **Complex Array Structures**

   **Problem**: Incorrect definition of arrays containing complex structs.
   
   **Solution**: Use the `nestedFields` property with proper nesting:
   
   ```python
   {
       "path": "data.metrics",
       "value": "array",
       "doc": "Metrics array",
       "nestedFields": {
           "name": {"type": "string", "doc": "Metric name"},
           "details": {
               "type": "object",
               "doc": "Details object",
               "nestedFields": {
                   "value": {"type": "double", "doc": "Metric value"}
               }
           }
       }
   }
   ```

4. **SQL Size Limitations**

   **Problem**: Generated SQL exceeds database size limitations for very complex structures.
   
   **Solution**: Break down complex structures into smaller, manageable pieces:
   
   ```python
   # Instead of one large operation:
   input_data = [
       {"operation": "ADD", "columns": [/* many complex columns */]},
       # Split into multiple operations
       {"operation": "ADD", "columns": [/* first group */]},
       {"operation": "ADD", "columns": [/* second group */]},
   ]
   ```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
