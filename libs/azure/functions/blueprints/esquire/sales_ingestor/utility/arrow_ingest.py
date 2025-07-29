import pyarrow as pa

_AR2PG = {
    pa.int8(): "SMALLINT",
    pa.int16(): "SMALLINT",
    pa.int32(): "INTEGER",
    pa.int64(): "BIGINT",
    pa.uint8(): "INTEGER",
    pa.uint16(): "INTEGER",
    pa.uint32(): "BIGINT",
    pa.uint64(): "NUMERIC",
    pa.float16(): "REAL",
    pa.float32(): "REAL",
    pa.float64(): "DOUBLE PRECISION",
    pa.bool_(): "BOOLEAN",
    pa.string(): "TEXT",
    pa.large_string(): "TEXT",
    pa.binary(): "BYTEA",
    pa.date32(): "DATE",
    pa.date64(): "DATE",
}

def _pg_type(field: pa.Field) -> str:
    """Map Arrow field to PostgreSQL type based on its value type."""
    arrow_type = field.type

    # If it's a dictionary, unwrap and inspect the actual value type
    if pa.types.is_dictionary(arrow_type):
        arrow_type = arrow_type.value_type

    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP WITH TIME ZONE" if arrow_type.tz else "TIMESTAMP"
    if pa.types.is_decimal(arrow_type):
        return f"NUMERIC({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "TEXT"
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_integer(arrow_type):
        if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
            return "SMALLINT"
        elif pa.types.is_int32(arrow_type):
            return "INTEGER"
        elif pa.types.is_int64(arrow_type):
            return "BIGINT"
    if pa.types.is_floating(arrow_type):
        if pa.types.is_float32(arrow_type):
            return "REAL"
        elif pa.types.is_float64(arrow_type):
            return "DOUBLE PRECISION"

    # Fallback for unknown or complex types
    return "JSONB"