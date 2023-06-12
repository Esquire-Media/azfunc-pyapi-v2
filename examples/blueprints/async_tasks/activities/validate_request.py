from libs.azure.functions import Blueprint
from marshmallow import Schema, fields, validate
from dateutil.relativedelta import relativedelta

bp = Blueprint()


# Activity
@bp.activity_trigger(input_name="request")
def validate_request(request: dict):
    s = CampaignSchema()
    return s.dumps(s.load(request))


class AdvertiserSchema(Schema):
    id = fields.Int(required=True)
    tenant = fields.UUID(required=True)
    name = fields.Str(required=True)


class DateRangeSchema(Schema):
    start = fields.Date(required=True)
    end = fields.Date()
    months = fields.Integer()

    def validate(self, data, **kwargs):
        if 'start' in data and 'end' in data:
            start = data['start']
            end = data['end']
            months = relativedelta(end, start).months
            data['months'] = months
        elif 'start' in data and 'months' in data:
            start = data['start']
            months = data['months']
            end = start + relativedelta(months=months)
            data['end'] = end

        return super().validate(data, **kwargs)


class FilterSchema(Schema):
    field = fields.Str(required=True)
    operator = fields.Str(validate=validate.OneOf([">=", "<=", "=", "!=", ">", "<"]))
    value = fields.Raw(required=True)


CARIBBEAN_ISO       = [28, 44, 52, 192, 212, 214, 308, 332, 388, 659, 662, 670, 780]
CENTRAL_AMERICA_ISO = [84, 188, 222, 320, 340, 484, 558, 591]
NORTH_AMERICA_ISO   = [124, 840, 304]
SOUTH_AMERICA_ISO   = [32, 68, 76, 152, 170, 218, 254, 328, 604, 600, 740, 858, 862]

class BaseSchema(Schema):
    country = fields.Int(
        required=True,
        validate=validate.OneOf(
            CARIBBEAN_ISO + CENTRAL_AMERICA_ISO + NORTH_AMERICA_ISO + SOUTH_AMERICA_ISO
        ),
    )
    zips = fields.List(fields.Int())


class TargetingSchema(Schema):
    type = fields.Str(validate=validate.OneOf(["broad"]), required=True)
    base = fields.Nested(BaseSchema, required=True)
    filters = fields.List(fields.Nested(FilterSchema))


class CampaignSchema(Schema):
    advertiser = fields.Nested(AdvertiserSchema, required=True)
    date_range = fields.Nested(DateRangeSchema, required=True)
    budget = fields.Int(required=True)
    creative = fields.Url(required=True)
    targeting = fields.List(fields.Nested(TargetingSchema))
