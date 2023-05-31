from marshmallow import Schema, fields


class ResponseBaseSchema(Schema):
    id = fields.UUID(required=True)
    name = fields.Str(required=True)


class ResponseWithSaveSchema(ResponseBaseSchema):
    location = fields.Str(required=True)
