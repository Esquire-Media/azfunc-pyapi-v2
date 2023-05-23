from marshmallow import Schema, fields

class LogRecordBaseSchema(Schema):
    name = fields.String()
    level = fields.Integer(attribute="levelno")
    levelname = fields.String()
    pathname = fields.String()
    filename = fields.String()
    module = fields.String()
    lineno = fields.Integer()
    funcName = fields.String(attribute="funcName")
    created = fields.Float()
    msecs = fields.Float()
    relativeCreated = fields.Float(attribute="relativeCreated")
    thread = fields.Integer()
    threadName = fields.String(attribute="threadName")
    process = fields.Integer()
    message = fields.String()
