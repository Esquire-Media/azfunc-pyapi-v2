# File: libs/azure/functions/blueprints/oneview/tasks/schemas.py

from marshmallow import Schema, fields, validate, pre_load
from datetime import date
from dateutil.relativedelta import relativedelta


class AdvertiserSchema(Schema):
    """
    A Schema for advertiser data.

    Attributes
    ----------
    id : str
        The advertiser ID. Required.
    tenant : UUID
        The tenant ID. Required.
    name : str
        The name of the advertiser.
    domain : str
        The domain of the advertiser.
    category : str
        The category of the advertiser.
    """

    id = fields.Str(required=True)
    tenant = fields.UUID(required=True)
    name = fields.Str()
    domain = fields.Str()
    category = fields.Str()


class DateRangeSchema(Schema):
    """
    A Schema for date range data.

    Attributes
    ----------
    start : date
        The start date of the range. Required.
    end : date
        The end date of the range.
    months : int
        The number of months in the range.
    """

    start = fields.Date(required=True)
    end = fields.Date()
    months = fields.Integer()

    @pre_load
    def pre_load_input(self, data, **kwargs):
        """
        Pre-load method to calculate the number of months between start and end dates or
        calculate the end date based on the start date and the number of months.

        Parameters
        ----------
        data : dict
            The input data.

        Returns
        -------
        dict
            The modified input data.
        """
        if "start" in data and "end" in data:
            if isinstance(data["start"], str):
                start = date.fromisoformat(data["start"])
            else:
                start = data["start"]
            if isinstance(data["end"], str):
                end = date.fromisoformat(data["end"])
            else:
                end = data["end"]
            rel = relativedelta(end, start)
            data["months"] = rel.months + rel.years * 12
        elif "start" in data and "months" in data:
            if isinstance(data["start"], str):
                start = date.fromisoformat(data["start"])
            else:
                start = data["start"]
            months = int(data["months"])
            data["end"] = (start + relativedelta(months=months)).isoformat()
        return data


class FilterSchema(Schema):
    """
    A Schema for filter data.

    Attributes
    ----------
    field : str
        The field to filter on. Required.
    operator : str
        The comparison operator. Must be one of [">=", "<=", "=", "!=", ">", "<"].
    value : Any
        The filter value. Required.
    """

    field = fields.Str(required=True)
    operator = fields.Str(validate=validate.OneOf([">=", "<=", "=", "!=", ">", "<"]))
    value = fields.Raw(required=True)


CARIBBEAN_ISO = [28, 44, 52, 192, 212, 214, 308, 332, 388, 659, 662, 670, 780]
CENTRAL_AMERICA_ISO = [84, 188, 222, 320, 340, 484, 558, 591]
NORTH_AMERICA_ISO = [124, 840, 304]
SOUTH_AMERICA_ISO = [32, 68, 76, 152, 170, 218, 254, 328, 604, 600, 740, 858, 862]


class BaseSchema(Schema):
    """
    Base schema for targeting data.

    Attributes
    ----------
    country : int
        The country ISO code. Required. Must be one of the defined regions.
    zips : List[int]
        List of ZIP codes.
    """

    country = fields.Int(
        required=True,
        validate=validate.OneOf(
            CARIBBEAN_ISO + CENTRAL_AMERICA_ISO + NORTH_AMERICA_ISO + SOUTH_AMERICA_ISO
        ),
    )
    zips = fields.List(fields.Str())


class TargetingSchema(Schema):
    """
    A Schema for targeting data.

    Attributes
    ----------
    type : str
        The targeting type. Must be "broad". Required.
    base : BaseSchema
        The base targeting schema. Required.
    filters : List[FilterSchema]
        List of filter schemas.
    """

    type = fields.Str(validate=validate.OneOf(["broad"]), required=True)
    base = fields.Nested(BaseSchema, required=True)
    filters = fields.List(fields.Nested(FilterSchema))


class BudgetingSchema(Schema):
    """
    A Schema for budgeting data.

    Attributes
    ----------
    monthly_impressions : int
        Estimated monthly impressions. Required.
    cpm_client : int
        CPM billed to the client. Required.
    cpm_tenant : int
        CPM billed to the reseller. Required
    """

    monthly_impressions = fields.Int(required=True)
    cpm_client = fields.Int(required=True)
    cpm_tenant = fields.Int(required=True)


class CampaignSchema(Schema):
    """
    A Schema for campaign data.

    Attributes
    ----------
    advertiser : AdvertiserSchema
        The advertiser schema. Required.
    date_range : DateRangeSchema
        The date range schema. Required.
    budget : BudgetingSchema
        The budgeting schema. Required.
    landing_page : str
        The landing page URL. Required.
    creative : str
        The creative URL. Required.
    targeting : List[TargetingSchema]
        List of targeting schemas.
    """

    advertiser = fields.Nested(AdvertiserSchema, required=True)
    date_range = fields.Nested(DateRangeSchema, required=True)
    budget = fields.Nested(BudgetingSchema, required=True)
    title = fields.Str(required=True)
    creative = fields.Url(required=True)
    landing_page = fields.Url(required=True)
    targeting = fields.List(fields.Nested(TargetingSchema))


class AdvertiserRecordSchema(Schema):
    """
    A Schema for advertiser record data.

    Attributes
    ----------
    PartitionKey : UUID
        The partition key. Required.
    RowKey : UUID
        The row key. Required.
    OrganizationID : str
        The organization ID. Required.
    OrganizationName : str
        The organization name. Required.
    Domain : str
        The domain of the advertiser. Required.
    ContentCategory : str
        The content category of the advertiser. Required.
    OneView_AdvertiserID : str
        The OneView advertiser ID. Required.
    OneView_OrgID : str
        The OneView organization ID. Required.
    """

    PartitionKey = fields.UUID(required=True)
    RowKey = fields.UUID(required=True)
    OrganizationID = fields.Str(required=True)
    OrganizationName = fields.Str(required=True)
    Domain = fields.Str(required=True)
    ContentCategory = fields.Str(required=True)
    OneView_AdvertiserID = fields.Str(required=True)
    OneView_OrgID = fields.Str(required=True)


class CreativeRecordSchema(Schema):
    """
    CreativeRecordSchema class.

    Parameters
    ----------
    PartitionKey : str
        The partition key. Required.
    RowKey : str
        The row key. Required.
    OneView_CreativeID : str
        The OneView creative ID. Required.
    Landing_Page : URL
        Landing page URL. Required.
    """

    PartitionKey = fields.Str(required=True)
    RowKey = fields.Str(required=True)
    OneView_CreativeID = fields.Str(required=True)
    Landing_Page = fields.URL(required=True)


class CampaignRecordSchema(Schema):
    """
    CampaignRecordSchema class.

    Parameters
    ----------
    PartitionKey : UUID
        The partition key. Required.
    RowKey : UUID
        The row key. Required.
    OneView_CampaignID : str
        The OneView campaign ID. Required.
    Start : date
        The start date. Required.
    End : date
        The end date. Required.
    """

    PartitionKey = fields.UUID(required=True)
    RowKey = fields.UUID(required=True)
    InstanceID = fields.UUID(required=True)
    OneView_CampaignID = fields.Str(required=True)
    CPM_Client = fields.Int(required=False)
    CPM_Tenant = fields.Int(required=False)
    Monthly_Impressions = fields.Int(required=True)
    Start = fields.Date(required=True)
    End = fields.Date(required=True)


class FlightRecordSchema(Schema):
    """
    FlightRecordSchema class.

    Parameters
    ----------
    PartitionKey : UUID
        The partition key. Required.
    RowKey : UUID
        The row key. Required.
    OneView_FlightID : str
        The OneView flight ID. Required.
    Start : date
        The start date. Required.
    End : date
        The end date. Required.
    """

    PartitionKey = fields.Str(required=True)
    RowKey = fields.UUID(required=True)
    OneView_FlightID = fields.Str(required=True)
    Start = fields.Date(format="%Y-%m-%d", required=True)
    End = fields.Date(format="%Y-%m-%d", required=True)


class StateSchema(Schema):
    """
    StateSchema class.

    Parameters
    ----------
    advertiser : AdvertiserRecordSchema
        The advertiser record schema. Allow None.
    creative : CreativeRecordSchema
        The creative record schema. Allow None.
    campaign : CampaignRecordSchema
        The campaign record schema. Allow None.
    flights : List[FlightRecordSchema]
        List of flight record schemas.
    """

    advertiser = fields.Nested(AdvertiserRecordSchema, allow_none=True)
    creative = fields.Nested(CreativeRecordSchema, allow_none=True)
    campaigns = fields.List(fields.Nested(CampaignRecordSchema))
    flights = fields.List(fields.Nested(FlightRecordSchema))


class OrchestratorStatusSchema(Schema):
    """
    OrchestratorStatusSchema class.

    Parameters
    ----------
    id : str
        The status ID.
    statusQueryGetUri : URL
        The URL for status query.
    sendEventPostUri : URL
        The URL for sending an event.
    terminatePostUri : URL
        The URL for terminating the orchestrator.
    rewindPostUri : URL
        The URL for rewinding the orchestrator.
    purgeHistoryDeleteUri : URL
        The URL for purging history.
    restartPostUri : URL
        The URL for restarting the orchestrator.
    suspendPostUri : URL
        The URL for suspending the orchestrator.
    resumePostUri : URL
        The URL for resuming the orchestrator.
    """

    id = fields.Str()
    statusQueryGetUri = fields.URL()
    sendEventPostUri = fields.URL()
    terminatePostUri = fields.URL()
    rewindPostUri = fields.URL()
    purgeHistoryDeleteUri = fields.URL()
    restartPostUri = fields.URL()
    suspendPostUri = fields.URL()
    resumePostUri = fields.URL()


class LeaseSchema(Schema):
    """
    LeaseSchema class.

    Parameters
    ----------
    expires : DateTime
        Expiration date and time. Required.
    provider : str
        Provider name. Required.
    access_id : str
        Access identifier. Required.
    """

    expires = fields.DateTime(required=True)
    provider = fields.Str(required=True)
    access_id = fields.Str(required=True)


class TracebackSchema(Schema):
    file = fields.Str(required=True)
    line = fields.Int(required=True)


class ErrorSchema(Schema):
    """
    ErrorSchema class.

    Parameters
    ----------
    step : str
        The step where the error occurred. Required.
    reason : str
        The reason for the error. Required.
    """

    type = fields.Str(require=True)
    message = fields.Str(require=True)
    traceback = fields.List(fields.Nested(TracebackSchema))


class RequestSchema(Schema):
    """
    RequestSchema class.

    Parameters
    ----------
    request : CampaignSchema
        The campaign schema.
    links : OrchestratorStatusSchema
        The orchestrator status schema.
    existing : StateSchema
        The state schema.
    md5 : str
        The MD5 hash.
    lease : LeaseSchema
        Lease details.
    """

    request = fields.Nested(CampaignSchema)
    links = fields.Nested(OrchestratorStatusSchema)
    existing = fields.Nested(
        StateSchema,
        required=False,
        allow_none=True,
    )
    md5 = fields.Str(
        required=False,
        allow_none=True,
    )
    lease = fields.Nested(
        LeaseSchema,
        required=False,
        allow_none=True,
    )
    errors = fields.List(
        fields.Nested(ErrorSchema),
        required=False,
        allow_none=True,
        default=[],
    )


class StatusSchema(Schema):
    """
    StatusSchema class.

    Parameters
    ----------
    started : DateTime
        The start time. Required.
    updated : DateTime
        The last updated time.
    message : str
        The status message.
    errors : List[ErrorSchema]
        List of error schemas.
    state : RequestSchema
        The request schema. Allow None.
    """

    started = fields.DateTime(required=True, format="iso")
    updated = fields.DateTime(required=False, format="iso")
    message = fields.Str(required=False, allow_none=True)
    state = fields.Nested(RequestSchema, allow_none=True)
