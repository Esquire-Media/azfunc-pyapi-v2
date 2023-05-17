from marshmallow import Schema, fields, validate


PartnerOptions = [
    "appnexus",
    "appnexusbidder",
    "chalkdigital",
    "eltoro",
    "facebook",
    "liquidm",
    "lotame",
    "resetdigital",
    "rtbiq",
    "thetradedesk",
]


class LotameSegment(Schema):
    files = fields.List(fields.Url(), required=True)
    name = fields.Str(required=True)
    id = fields.Int(required=True)


class DevicesSegment(Schema):
    partner = fields.Str(required=True, validate=validate.OneOf(PartnerOptions))
    organization = fields.Str(required=True)
    sourcePaths = fields.List(fields.Url(), required=True)
    callback = fields.Str(required=True)
    name = fields.Str()
    validate = fields.Bool(missing=True)
    segments = fields.List(fields.Str())
    lotameSegments = fields.List(fields.Nested(LotameSegment))
    ttl = fields.Int()
    extend = fields.Bool(missing=False)
    advertiserId = fields.Str()
    secret = fields.Str()
    username = fields.Str()
    password = fields.Str()
    memberid = fields.Int()
    orgId = fields.Str()
    cpm = fields.Float()
    segmentId = fields.Int()
