from .helper import spark_ddl_types, get_property_description


class SQLFormatter:
    """Utility class for SQL formatting operations."""

    @staticmethod
    def format_path(path):
        """Format a dotted path with proper backticks."""
        return ".".join([f"`{p}`" for p in path.split(".")])

    @staticmethod
    def format_comment(comment):
        """Format a comment with property description."""
        return f"COMMENT {get_property_description(comment)}"

    @staticmethod
    def format_after_clause(item):
        """Format the AFTER/FIRST clause."""
        if "moveafter" not in item:
            return ""
        return (
            " FIRST"
            if item["moveafter"] == "first"
            else f" AFTER `{item['moveafter']}`"
        )


class SQLColumnGenerator:
    def __init__(self, input_data):
        self.input_data = input_data
        self.formatter = SQLFormatter()

    def generate_sql(self):
        sql_statements = []

        for operation_group in self.input_data:
            op_type = operation_group["operation"]

            if op_type == "ADD":
                add_generator = OrderPreservingGenerator(operation_group["columns"])
                sql = add_generator.generate_sql()
                sql_statements.append(sql)

            elif op_type == "REMOVE":
                drop_statements = []
                for column_path in operation_group["columns"]:
                    formatted_path = self.formatter.format_path(column_path)
                    drop_statements.append(
                        f"ALTER TABLE {{table_name}} DROP COLUMN {formatted_path}"
                    )
                sql_statements.extend(drop_statements)

            elif op_type == "MOVE":
                move_statements = []
                for column in operation_group["columns"]:
                    path = column["path"]
                    formatted_path = self.formatter.format_path(path)
                    new_name = column["value"]
                    move_statements.append(
                        f"ALTER TABLE {{table_name}} RENAME COLUMN {formatted_path} TO `{new_name}`"
                    )
                sql_statements.extend(move_statements)

            elif op_type == "REORDER":
                reorder_statements = []
                for column in operation_group["columns"]:
                    path = column["path"]
                    formatted_path = self.formatter.format_path(path)
                    if "moveafter" in column and column["moveafter"] == "first":
                        reorder_statements.append(
                            f"ALTER TABLE {{table_name}} ALTER COLUMN {formatted_path} FIRST"
                        )
                    else:
                        after_column = column["value"]
                        formatted_after = self.formatter.format_path(after_column)
                        reorder_statements.append(
                            f"ALTER TABLE {{table_name}} ALTER COLUMN {formatted_path} AFTER {formatted_after}"
                        )
                sql_statements.extend(reorder_statements)

            elif op_type == "REPLACE":
                replace_statements = []
                for column in operation_group["columns"]:
                    path = column["path"]
                    formatted_path = self.formatter.format_path(path)

                    target_field = column.get("target_field", "value")
                    if target_field == "description" or target_field == "comment":
                        replace_statements.append(
                            f"ALTER TABLE {{table_name}} ALTER COLUMN {formatted_path} COMMENT {get_property_description(column['value'])}"
                        )
                    elif target_field == "type":
                        data_type = spark_ddl_types.get(
                            column["value"], column["value"]
                        )
                        replace_statements.append(
                            f"ALTER TABLE {{table_name}} ALTER COLUMN {formatted_path} TYPE {data_type}"
                        )
                    elif target_field == "name":
                        new_name = column["value"]
                        replace_statements.append(
                            f"ALTER TABLE {{table_name}} RENAME COLUMN {formatted_path} TO `{new_name}`"
                        )

                sql_statements.extend(replace_statements)

        return sql_statements


class PathHandler:
    """Helper class for path operations."""

    def __init__(self):
        self.processed_paths = set()
        self.path_data = {}

    def is_processed(self, path):
        return path in self.processed_paths

    def mark_processed(self, path):
        self.processed_paths.add(path)

    def is_child_of_processed_path(self, path):
        """Check if a path is a child of any already processed path."""
        parts = path.split(".")
        for i in range(1, len(parts)):
            parent_path = ".".join(parts[:i])
            if parent_path in self.processed_paths:
                return True
        return False

    def mark_tree_processed(self, parent_path):
        """Mark a path and all its children as processed."""
        self.processed_paths.add(parent_path)
        prefix = parent_path + "."
        for path in self.path_data.keys():
            if path != parent_path and path.startswith(prefix):
                self.processed_paths.add(path)

    def has_children(self, parent_path):
        """Check if a path has children."""
        prefix = parent_path + "."
        for path in self.path_data.keys():
            if path != parent_path and path.startswith(prefix):
                return True
        return False


class OrderPreservingGenerator:
    """
    SQL Generator that preserves original field order and handles nested arrays properly.
    """

    def __init__(self, input_data):
        self.input_data = input_data
        self.processed_paths = set()
        self.field_order = {}  # Track field order for all path levels
        self.element_fields = {}  # Maps array path to all its element fields
        self.element_field_order = {}  # Track order of element fields within arrays
        self.array_element_fields = {}  # Maps array path to element field groups
        self.path_data = {}  # Maps each path to its data item
        self.top_level_items = []  # Track top-level fields in original order
        self.nested_arrays = (
            {}
        )  # Track paths that are nested arrays within array elements
        self.original_order = {}  # Track original order of input items
        self.formatter = SQLFormatter()

    def generate_sql(self):
        """Generate SQL while preserving field order."""
        self._preprocess_input()
        self._process_array_elements()

        column_definitions = self._process_top_level_items()
        column_definitions.extend(self._process_remaining_items())

        if not column_definitions:
            return f"ALTER TABLE {{table_name}} \n    ADD COLUMNS ()"

        joined_columns = ",\n".join(column_definitions)
        result = (
            f"ALTER TABLE {{table_name}} \n    ADD COLUMNS (\n{joined_columns}\n    )"
        )

        return result

    def _process_top_level_items(self):
        """Process all top-level items and return their column definitions."""
        column_definitions = []

        for item in self.top_level_items:
            path = item["path"]
            # Skip if already processed
            if path in self.processed_paths:
                continue

            # Check if this is a new struct definition
            if item["value"] == "object" and self._has_children(path):
                definition = self._format_struct(path, item)
                if definition:
                    column_definitions.append(definition)
                    # Mark this and all children as processed
                    self._mark_processed(path)
            else:
                definition = self._format_dotted_path(item)
                if definition:
                    column_definitions.append(definition)
                    self.processed_paths.add(path)

        return column_definitions

    def _process_remaining_items(self):
        """Process any remaining items that weren't handled as part of top-level structures."""
        column_definitions = []

        # Process any remaining dotted paths
        remaining_items = self._get_remaining_items()

        # Sort remaining items by their original order
        remaining_items.sort(key=lambda x: self.original_order.get(x["path"], 999999))

        for item in remaining_items:
            path = item["path"]
            definition = self._format_dotted_path(item)
            if definition:
                column_definitions.append(definition)
                self.processed_paths.add(path)

        return column_definitions

    def _get_remaining_items(self):
        """Get items that haven't been processed yet and aren't children of processed paths."""
        remaining_items = []
        for item in self.input_data:
            path = item["path"]
            if (
                path not in self.processed_paths
                and not self._is_child_of_processed_path(path)
            ):
                remaining_items.append(item)
        return remaining_items

    def _is_child_of_processed_path(self, path):
        """Check if a path is a child of any already processed path."""
        parts = path.split(".")
        for i in range(1, len(parts)):
            parent_path = ".".join(parts[:i])
            if parent_path in self.processed_paths:
                return True
        return False

    def _process_array_elements(self):
        """
        Group all fields belonging to the same array element structure.
        This ensures nested elements are properly grouped.
        """
        self.array_element_fields = {}  # Maps array path to element field groups
        self.nested_arrays = {}  # Maps parent array paths to nested array paths

        # First pass: identify all array paths that contain element fields
        element_paths = [path for path in self.path_data.keys() if ".element." in path]

        # Sort paths to ensure parent arrays are processed before nested arrays
        element_paths.sort(key=lambda x: x.count(".element."))

        for path in element_paths:
            self._process_element_path(path)

    def _process_element_path(self, path):
        """Process a single element path to extract array relationships."""
        # Find all occurrences of ".element." in the path
        segments = path.split(".element.")
        base_path = segments[0]

        # Initialize array fields tracking for base path
        if base_path not in self.array_element_fields:
            self.array_element_fields[base_path] = {}

        # Handle multi-level element paths (nested arrays)
        if len(segments) > 2:
            self._process_multilevel_element_path(path, segments, base_path)
        elif len(segments) == 2:
            self._process_simple_element_path(path, segments, base_path)

    def _process_multilevel_element_path(self, path, segments, base_path):
        """Process an element path with multiple .element. segments (nested arrays)."""
        # This is a path with multiple .element. segments
        # e.g., "checks.element.requirements.element.ruleValue"

        # Process first level
        first_element = segments[1].split(".", 1)
        first_field = first_element[0]  # e.g., "requirements"

        # Create nested array path
        nested_array_path = f"{base_path}.element.{first_field}"

        # Track this as a nested array
        if base_path not in self.nested_arrays:
            self.nested_arrays[base_path] = []
        if nested_array_path not in self.nested_arrays[base_path]:
            self.nested_arrays[base_path].append(nested_array_path)

        # Initialize element fields for nested array
        if nested_array_path not in self.array_element_fields:
            self.array_element_fields[nested_array_path] = {}

        # Get the field name from the last segment
        if len(segments) == 3:
            field_name = segments[2]
            self.array_element_fields[nested_array_path][field_name] = path
        else:
            # More complex nesting - handle second level
            if len(first_element) > 1:
                second_level = first_element[1].split(".", 1)
                if len(second_level) > 0:
                    field_name = second_level[0]
                    self.array_element_fields[nested_array_path][field_name] = path

    def _process_simple_element_path(self, path, segments, base_path):
        """Process a simple element path with a single .element. segment."""
        # Simple element field (first level array)
        remaining = segments[1]
        field_parts = remaining.split(".", 1)
        field_name = field_parts[0]

        # Store mapping from field name to full path
        self.array_element_fields[base_path][field_name] = path

    def _preprocess_input(self):
        """Pre-process input to track field order and gather element fields."""
        top_level_paths = set()
        self.array_nested_fields = {}
        self.element_field_order = {}

        self._track_original_order()
        self._process_path_items(top_level_paths)

    def _track_original_order(self):
        """Track the original order of all input items."""
        for idx, item in enumerate(self.input_data):
            path = item["path"]
            self.original_order[path] = idx

    def _process_path_items(self, top_level_paths):
        """Process each path item to build necessary data structures."""
        for item in self.input_data:
            path = item["path"]
            parts = path.split(".")

            # Store item data indexed by path
            self.path_data[path] = item

            # Track top-level items
            top_level = parts[0]
            if top_level not in top_level_paths:
                top_level_paths.add(top_level)
                self.top_level_items.append(item)

            self._process_array_nested_fields(item, path)
            self._process_element_path_fields(path, parts)
            self._build_field_order_hierarchy(parts)

    def _process_array_nested_fields(self, item, path):
        """Process nested fields for arrays."""
        if (
            "value" in item
            and item["value"] == "array"
            and "nestedFields" in item
            and item["nestedFields"]
            and item["nestedFields"] != "None"
        ):
            self.array_nested_fields[path] = item["nestedFields"]

    def _process_element_path_fields(self, path, parts):
        """Process and track element fields for paths with .element. segments."""
        if ".element." in path:
            array_path, element_field = self._split_element_path(path)

            # Initialize array element fields tracking if needed
            if array_path not in self.element_fields:
                self.element_fields[array_path] = []

            # Initialize element field order tracking
            if array_path not in self.element_field_order:
                self.element_field_order[array_path] = []

            # Track the element field
            if element_field not in self.element_fields[array_path]:
                self.element_fields[array_path].append(element_field)

            # Track the element field order
            if element_field not in self.element_field_order[array_path]:
                self.element_field_order[array_path].append(element_field)

    def _build_field_order_hierarchy(self, parts):
        """Build hierarchical field order tracking."""
        for i in range(len(parts)):
            parent = ".".join(parts[:i]) if i > 0 else ""
            field = parts[i]

            if parent not in self.field_order:
                self.field_order[parent] = []

            if field not in self.field_order[parent]:
                self.field_order[parent].append(field)

    def _split_element_path(self, path):
        """
        Split a path at first '.element.' occurrence to get the array path and first element field.
        """
        parts = path.split(".element.", 1)
        return parts[0], parts[1]

    def _has_children(self, parent_path):
        """Check if a path has children."""
        prefix = parent_path + "."
        for path in self.path_data.keys():
            if path != parent_path and path.startswith(prefix):
                return True
        return False

    def _mark_processed(self, parent_path):
        """Mark a path and all its children as processed."""
        self.processed_paths.add(parent_path)
        prefix = parent_path + "."
        for path in self.path_data.keys():
            if path != parent_path and path.startswith(prefix):
                self.processed_paths.add(path)

    def _get_direct_children(self, parent_path):
        """Get all direct children of a path including arrays and their elements."""
        direct_children = []
        prefix = parent_path + "." if parent_path else ""
        prefix_len = len(prefix)

        # First collect all potential direct children based on path structure
        for path in self.path_data.keys():
            # Skip the parent itself and already processed paths
            if path == parent_path or path in self.processed_paths:
                continue

            # Must start with parent prefix
            if not path.startswith(prefix):
                continue

            # Get remaining part after the prefix
            remaining = path[prefix_len:]

            # Direct child cases
            self._add_direct_child_if_matched(
                path, remaining, direct_children, parent_path
            )

        return direct_children

    def _add_direct_child_if_matched(
        self, path, remaining, direct_children, parent_path
    ):
        """Add a path to direct_children if it meets the criteria for being a direct child."""
        # 1. No dots in remaining (direct field)
        if "." not in remaining:
            direct_children.append(path)
        # 2. Direct array with elements
        elif remaining.count(".") >= 2 and ".element." in remaining:
            # Handle array elements - extract just the array part
            array_name = remaining.split(".")[0]
            array_path = f"{parent_path}.{array_name}"

            # Include the array itself if not already included
            if array_path not in direct_children and array_path in self.path_data:
                direct_children.append(array_path)
        # 3. Handle first level child even if it has further children
        elif "." in remaining and remaining.count(".") == 1:
            # Get just the first part before the dot
            first_part = remaining.split(".", 1)[0]
            child_path = f"{parent_path}.{first_part}"

            # Add this as a direct child if it exists in path_data
            if child_path in self.path_data and child_path not in direct_children:
                direct_children.append(child_path)

    def _format_struct(self, path, item):
        """Format a field as a struct with all its children."""
        if path in self.processed_paths:
            return None

        # Get direct children of this struct
        children = self._get_direct_children(path)
        if not children:
            # Empty struct
            formatted_path = self.formatter.format_path(path)
            comment = item.get("doc", "")
            after_clause = self.formatter.format_after_clause(item)

            # Mark the path as processed
            self.processed_paths.add(path)
            return f"    {formatted_path} struct<> {self.formatter.format_comment(comment)}{after_clause}"

        # Process all child fields
        struct_content = self._format_struct_content(path)

        formatted_path = self.formatter.format_path(path)
        comment = item.get("doc", "")
        after_clause = self.formatter.format_after_clause(item)

        # Mark this path and all its children as processed
        self._mark_processed_tree(path, children)

        return f"    {formatted_path} struct<\n{struct_content}\n    > {self.formatter.format_comment(comment)}{after_clause}"

    def _mark_processed_tree(self, path, children):
        """Mark a path, its children, and related array elements as processed."""
        self.processed_paths.add(path)
        for child in children:
            self.processed_paths.add(child)
            # Also mark any array element paths
            if child in self.element_fields:
                for element_field in self.element_fields[child]:
                    element_path = f"{child}.element.{element_field}"
                    self.processed_paths.add(element_path)

    def _format_struct_content(self, struct_path, indent_level=2):
        """Format the content of a struct, handling nested arrays correctly."""
        indent = " " * (indent_level * 4)
        lines = []

        # Get ordered children
        ordered_children = self._get_ordered_children(struct_path)

        # Process each child
        for child_path in ordered_children:
            lines.append(self._format_struct_child(child_path, indent, indent_level))

        return ",\n".join(lines)

    def _get_ordered_children(self, struct_path):
        """Get direct children in the correct order."""
        direct_children = self._get_direct_children(struct_path)

        # Get children in original order if available
        ordered_children = []
        for field in self.field_order.get(struct_path, []):
            child_path = f"{struct_path}.{field}" if struct_path else field
            if child_path in direct_children:
                ordered_children.append(child_path)

        # Add any missing children
        for child in direct_children:
            if child not in ordered_children:
                ordered_children.append(child)

        # Sort children by their original order in the input data
        ordered_children.sort(key=lambda x: self.original_order.get(x, 999999))
        return ordered_children

    def _format_struct_child(self, child_path, indent, indent_level):
        """Format a single child field within a struct."""
        # Get the field name (last segment of path)
        field = child_path.split(".")[-1]

        # Get the item data
        child_item = self.path_data.get(child_path)
        if not child_item:
            return ""

        field_type = child_item["value"]
        comment = child_item.get("doc", "")

        if field_type == "object":
            return self._format_object_field(
                child_path, field, comment, indent, indent_level
            )
        elif field_type == "array":
            return self._format_array_field(
                child_path, field, comment, child_item, indent, indent_level
            )
        else:
            # Simple field
            data_type = spark_ddl_types.get(field_type, field_type)
            return (
                f"{indent}{field}: {data_type} {self.formatter.format_comment(comment)}"
            )

    def _format_object_field(self, child_path, field, comment, indent, indent_level):
        """Format an object (struct) field."""
        if self._has_children(child_path):
            # Nested struct with children
            nested_content = self._format_struct_content(child_path, indent_level + 1)
            if nested_content.strip():
                return f"{indent}{field}: struct<\n{nested_content}\n{indent}> {self.formatter.format_comment(comment)}"

        # Empty struct
        return f"{indent}{field}: struct<> {self.formatter.format_comment(comment)}"

    def _format_array_field(
        self, child_path, field, comment, child_item, indent, indent_level
    ):
        """Format an array field."""
        # Handle simple arrays vs arrays with elements
        has_elements = (
            child_path in self.array_element_fields
            and self.array_element_fields[child_path]
        )

        if "arr_type" in child_item and not has_elements:
            # Simple array
            arr_type = child_item["arr_type"]
            return f"{indent}{field}: array<{arr_type}> {self.formatter.format_comment(comment)}"

        # Array with element fields
        array_struct_content = self._format_array_struct_content(
            child_path, indent_level + 1
        )
        if array_struct_content.strip():
            return f"{indent}{field}: array<struct<\n{array_struct_content}\n{indent}>> {self.formatter.format_comment(comment)}"

        # Fallback to simple array
        arr_type = child_item.get("arr_type", "string")
        return f"{indent}{field}: array<{arr_type}> {self.formatter.format_comment(comment)}"

    def _format_array_struct_content(self, array_path, indent_level=3):
        """Format the struct content of an array's elements."""
        indent = " " * (indent_level * 4)
        struct_fields = []
        processed_fields = set()

        # Get all field names and their original order
        all_fields = self._collect_array_fields(array_path)

        # Sort all fields by their original order
        ordered_field_names = sorted(
            all_fields.keys(), key=lambda x: all_fields[x]["order"]
        )

        # Process fields in the correct order
        for field_name in ordered_field_names:
            field_info = all_fields[field_name]
            field_type = field_info["type"]

            if field_name in processed_fields:
                continue

            if field_type == "nestedField":
                struct_fields.append(
                    self._format_nestedfield(field_name, field_info, indent)
                )
            elif field_type == "regular":
                struct_fields.append(
                    self._format_regular_field(
                        field_name, field_info, indent, indent_level
                    )
                )
            elif field_type == "nested_array":
                struct_fields.append(
                    self._format_nested_array_field(
                        field_name, field_info, indent, indent_level
                    )
                )

            processed_fields.add(field_name)

        return ",\n".join(struct_fields)

    def _collect_array_fields(self, array_path):
        """Collect all fields for an array including nestedFields and nested arrays."""
        all_fields = {}

        # First add nestedFields if present
        array_item = self.path_data.get(array_path)
        if (
            array_item
            and "nestedFields" in array_item
            and array_item["nestedFields"]
            and array_item["nestedFields"] != "None"
        ):
            nf = array_item["nestedFields"]
            nf_name = nf.get("name", "id")
            # Add to all_fields with a special order (first)
            all_fields[nf_name] = {
                "type": "nestedField",
                "order": -1,  # Ensure it comes first
                "data": nf,
            }

        # Add regular element fields
        if array_path in self.array_element_fields:
            element_fields_map = self.array_element_fields[array_path]
            for field_name, element_path in element_fields_map.items():
                if element_path in self.path_data:
                    all_fields[field_name] = {
                        "type": "regular",
                        "order": self.original_order.get(element_path, 999999),
                        "path": element_path,
                    }

        # Add nested arrays
        nested_arrays = self.nested_arrays.get(array_path, [])
        for nested_array_path in nested_arrays:
            field_name = nested_array_path.split(".")[-1]
            all_fields[field_name] = {
                "type": "nested_array",
                "order": self.original_order.get(nested_array_path, 999999),
                "path": nested_array_path,
            }

        return all_fields

    def _format_nestedfield(self, field_name, field_info, indent):
        """Format a nested field element."""
        nf = field_info["data"]
        nf_type = spark_ddl_types.get(nf.get("type", "string"), "string")
        return f"{indent}{field_name}: {nf_type} {self.formatter.format_comment(nf.get('doc', ''))}"

    def _format_regular_field(self, field_name, field_info, indent, indent_level):
        """Format a regular element field."""
        element_path = field_info["path"]
        item = self.path_data[element_path]
        item_type = item["value"]
        comment = item.get("doc", "")

        # Format based on type
        if item_type == "array":
            arr_type = item.get("arr_type", "string")
            return f"{indent}{field_name}: array<{arr_type}> {self.formatter.format_comment(comment)}"
        elif item_type == "object" and self._has_children(element_path):
            nested_content = self._format_struct_content(element_path, indent_level + 1)
            return f"{indent}{field_name}: struct<\n{nested_content}\n{indent}> {self.formatter.format_comment(comment)}"
        else:
            data_type = spark_ddl_types.get(item_type, item_type)
            return f"{indent}{field_name}: {data_type} {self.formatter.format_comment(comment)}"

    def _format_nested_array_field(self, field_name, field_info, indent, indent_level):
        """Format a nested array field."""
        nested_array_path = field_info["path"]
        nested_content = self._format_nested_array_content(
            nested_array_path, indent_level + 1
        )

        # Get array definition
        array_def = None
        for item in self.input_data:
            if item["path"] == nested_array_path:
                array_def = item
                break

        comment = array_def.get("doc", "") if array_def else ""

        if nested_content.strip():
            return f"{indent}{field_name}: array<struct<\n{nested_content}\n{indent}>> {self.formatter.format_comment(comment)}"
        else:
            arr_type = array_def.get("arr_type", "string") if array_def else "string"
            return f"{indent}{field_name}: array<{arr_type}> {self.formatter.format_comment(comment)}"

    def _format_nested_array_content(self, nested_array_path, indent_level=4):
        """Format content for a nested array within an array element."""
        indent = " " * (indent_level * 4)
        struct_fields = []
        processed_fields = set()

        # Add nestedFields from the array definition
        self._add_nested_array_fields(
            nested_array_path, indent, struct_fields, processed_fields
        )

        # Add element fields from collected sources
        self._add_element_fields_from_path_scan(
            nested_array_path, indent, struct_fields, processed_fields
        )
        self._add_element_fields_from_mapping(
            nested_array_path, indent, struct_fields, processed_fields
        )

        return ",\n".join(struct_fields)

    def _add_nested_array_fields(
        self, nested_array_path, indent, struct_fields, processed_fields
    ):
        """Add nested fields from array definition to struct fields."""
        array_def = None
        for item in self.input_data:
            if item["path"] == nested_array_path:
                array_def = item
                break

        if (
            array_def
            and "nestedFields" in array_def
            and array_def["nestedFields"]
            and array_def["nestedFields"] != "None"
        ):
            nf = array_def["nestedFields"]
            nf_type = (
                spark_ddl_types.get(nf["type"], nf["type"])
                if "type" in nf
                else "string"
            )
            nf_name = nf["name"]
            struct_fields.append(
                f"{indent}{nf_name}: {nf_type} {self.formatter.format_comment(nf.get('doc', ''))}"
            )
            processed_fields.add(nf_name)

    def _add_element_fields_from_path_scan(
        self, nested_array_path, indent, struct_fields, processed_fields
    ):
        """Add element fields found by scanning paths."""
        element_fields = {}
        for path in self.path_data.keys():
            # Look for fields that belong to this nested array
            parts = nested_array_path.split(".element.")
            if len(parts) >= 2:
                base_path = parts[0]
                nested_array_name = parts[1]
                pattern = f"{base_path}.element.{nested_array_name}.element."

                if path.startswith(pattern):
                    # Extract the field name
                    field_name = path[len(pattern) :]
                    if "." in field_name:
                        field_name = field_name.split(".", 1)[0]

                    element_fields[field_name] = path

        # Add the fields
        for field_name, path in element_fields.items():
            if field_name in processed_fields:
                continue

            # Get the item data
            item = self.path_data.get(path)
            if not item:
                continue

            field_type = item["value"]
            comment = item.get("doc", "")

            # Format the field
            data_type = spark_ddl_types.get(field_type, field_type)
            struct_fields.append(
                f"{indent}{field_name}: {data_type} {self.formatter.format_comment(comment)}"
            )
            processed_fields.add(field_name)

    def _add_element_fields_from_mapping(
        self, nested_array_path, indent, struct_fields, processed_fields
    ):
        """Add element fields from the array_element_fields mapping."""
        if nested_array_path in self.array_element_fields:
            element_fields_map = self.array_element_fields[nested_array_path]

            for field_name, element_path in element_fields_map.items():
                if field_name in processed_fields:
                    continue

                # Get element data
                item = self.path_data.get(element_path)
                if not item:
                    continue

                field_type = item["value"]
                comment = item.get("doc", "")

                # Format field
                data_type = spark_ddl_types.get(field_type, field_type)
                struct_fields.append(
                    f"{indent}{field_name}: {data_type} {self.formatter.format_comment(comment)}"
                )
                processed_fields.add(field_name)

    def _format_dotted_path(self, item):
        """Format a field as a direct dotted path."""
        path = item["path"]

        # Skip if this is an element field that will be processed with its array
        if ".element." in path:
            array_path, _ = self._split_element_path(path)
            if path in self.processed_paths or array_path in self.processed_paths:
                return None

        # Skip if part of a struct that's already processed
        parts = path.split(".")
        if len(parts) > 1:
            for i in range(1, len(parts)):
                parent_path = ".".join(parts[0:i])
                if parent_path in self.processed_paths:
                    return None

            # Skip if part of a struct that will be processed
            for i in range(1, len(parts)):
                parent_path = ".".join(parts[0:i])
                parent_item = self.path_data.get(parent_path)
                if (
                    parent_item
                    and parent_item["value"] == "object"
                    and self._has_children(parent_path)
                ):
                    return None

        value_type = item["value"]
        comment = item.get("doc", "")
        after_clause = self.formatter.format_after_clause(item)

        formatted_path = self.formatter.format_path(path)

        if value_type == "object":
            return self._format_dotted_object(
                path, formatted_path, comment, after_clause
            )
        elif value_type == "array":
            return self._format_dotted_array(
                path, formatted_path, item, comment, after_clause
            )
        else:
            data_type = spark_ddl_types.get(value_type, value_type)
            return f"    {formatted_path} {data_type} {self.formatter.format_comment(comment)}{after_clause}"

    def _format_dotted_object(self, path, formatted_path, comment, after_clause):
        """Format an object field as a dotted path."""
        if self._has_children(path):
            struct_content = self._format_struct_content(path)
            if struct_content.strip():
                return f"    {formatted_path} struct<\n{struct_content}\n    > {self.formatter.format_comment(comment)}{after_clause}"
        return f"    {formatted_path} struct<> {self.formatter.format_comment(comment)}{after_clause}"

    def _format_dotted_array(self, path, formatted_path, item, comment, after_clause):
        """Format an array field as a dotted path."""
        # For arrays, check if there are nested fields or element fields
        has_element_fields = (
            path in self.array_element_fields and self.array_element_fields[path]
        )
        has_nested_fields = (
            "nestedFields" in item
            and item["nestedFields"]
            and item["nestedFields"] != "None"
        )

        if has_nested_fields or has_element_fields:
            array_struct_content = self._format_array_struct_content(path)
            if array_struct_content.strip():
                self._mark_array_elements_processed(path)
                return f"    {formatted_path} array<struct<\n{array_struct_content}\n    >> {self.formatter.format_comment(comment)}{after_clause}"

        arr_type = item.get("arr_type", "string")
        return f"    {formatted_path} array<{arr_type}> {self.formatter.format_comment(comment)}{after_clause}"

    def _mark_array_elements_processed(self, path):
        """Mark all array element paths as processed."""
        if path in self.array_element_fields:
            for field_name, element_path in self.array_element_fields[path].items():
                if element_path in self.path_data:
                    self.processed_paths.add(element_path)

            if path in self.nested_arrays:
                for nested_array_path in self.nested_arrays[path]:
                    if nested_array_path in self.array_element_fields:
                        for _, nested_element_path in self.array_element_fields[
                            nested_array_path
                        ].items():
                            if nested_element_path in self.path_data:
                                self.processed_paths.add(nested_element_path)
