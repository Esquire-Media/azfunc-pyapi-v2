from marshmallow import Schema, fields, validate


class CallbackBaseSchema(Schema):
    id = fields.UUID(required=True)
    message = fields.Str(required=True)
    success = fields.Bool(required=True)


class CallbackInfoSchema(Schema):
    cbInfo = fields.Nested(CallbackBaseSchema, required=True)
    name = fields.Str()


class Coordinate(Schema):
    lat = fields.Float(
        required=True, validate=validate.Range(min=-90, max=90)
    )  # Latitude values range from -90 to 90
    lng = fields.Float(
        required=True, validate=validate.Range(min=-180, max=180)
    )  # Longitude values range from -180 to 180
