from libs.azure.functions import Blueprint
from marshmallow import Schema, fields, validate, ValidationError, validates_schema

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
    end = fields.Date(required=False)


class FilterSchema(Schema):
    field = fields.Str(required=True)
    operator = fields.Str(validate=validate.OneOf([">=", "<=", "=", "!=", ">", "<"]))
    value = fields.Raw(required=True)


class BaseSchema(Schema):
    cities = fields.List(fields.Str())
    zips = fields.List(fields.Int())


class TargetingSchema(Schema):
    type = fields.Str(validate=validate.OneOf(["broad"]), required=True)
    base = fields.Nested(BaseSchema, required=True)
    filters = fields.List(fields.Nested(FilterSchema))


class CampaignSchema(Schema):
    advertiser = fields.Nested(AdvertiserSchema, required=True)
    date_range = fields.Nested(DateRangeSchema, required=True)
    budget = fields.Int(required=True)
    impressions = fields.Int(required=True)
    creative = fields.Url(required=True)
    targeting = fields.List(fields.Nested(TargetingSchema))

    @validates_schema
    def validate_budget(self, data, **kwargs):
        CPM = 25
        if "budget" in data and "impressions" in data:
            min_budget = data["impressions"] / (CPM * 1000)
            if data["budget"] < min_budget:
                raise ValidationError(
                    f'"budget" is too low to reach the request number of "impressions"',
                    "budget",
                )
