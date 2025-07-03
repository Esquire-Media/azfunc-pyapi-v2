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
    """Translate a PyArrow field to a PostgreSQL column type."""
    if pa.types.is_timestamp(field.type):
        unit = field.type.unit  # s|ms|us|ns
        return f"TIMESTAMP" + (" WITH TIME ZONE" if field.type.tz else "")
    if pa.types.is_decimal(field.type):
        return f"NUMERIC({field.type.precision},{field.type.scale})"
    return _AR2PG.get(field.type, "JSONB")   # fallback for lists/structs/etc.

