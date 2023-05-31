from marshmallow.fields import Nested, Field


def marshmallow_schema_to_dict(self):
    """
    Convert Marshmallow schemas to a dictionary representing the structure's schema.

    Returns
    -------
    dict
        Dictionary representing the structure's schema.

    """
    return {
        f"{schema}.{table}": {
            "fields": {
                field.name: {
                    "type": field.__class__.__name__,
                    "read_only": field.dump_only,
                    "write_only": field.load_only,
                    "required": field.required,
                    "allow_none": field.allow_none,
                }
                for field in getattr(model, f"__marshmallow__")().fields.values()
                if (isinstance(field, Field) and not isinstance(field, Nested))
                or (
                    isinstance(field, Nested)
                    and len(type_ := type(field.schema).__name__.split(".")) == 1
                )
            },
            "relationships": {
                field.name: {
                    "type": ".".join(type_[1:]),
                    "read_only": field.dump_only,
                    "write_only": field.load_only,
                    "required": field.required,
                    "allow_none": field.allow_none,
                    "many": field.many,
                }
                for field in getattr(model, f"__marshmallow__")().fields.values()
                if isinstance(field, Nested)
                if len(type_ := type(field.schema).__name__.split(".")) > 1
            },
        }
        for schema, tables in self.models.items()
        for table, model in tables.items()
    }
