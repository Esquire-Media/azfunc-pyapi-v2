# File: libs/azure/functions/blueprints/schedules/report.py

from azure.functions import TimerRequest
from libs.azure.functions.blueprints.roku_async.helpers import TABLE_CLIENTS
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from libs.data import from_bind
import logging
import pandas as pd

bp = Blueprint()


def generate_reports_accounting(delta):
    if isinstance(delta, relativedelta):
        start = datetime.utcnow().date() + delta
        end = datetime.utcnow().date()
    if isinstance(delta, tuple):
        start = delta[0]
        end = delta[1]

    provider = from_bind("roku")
    for agency in TABLE_CLIENTS["agencies"].query_entities("PartitionKey eq 'tenants'"):
        df: pd.DataFrame = generate_report_accounting(
            provider, agency["OneView_AgencyID"], start, end
        ).to_pandas()
        if "CPM_Client" in agency.keys():
            df["cpm_client"] = float(agency["CPM_Client"])
        else:
            df["cpm_client"] = 40.0
        if "CPM_Client" in agency.keys():
            df["cpm_tenant"] = float(agency["CPM_Tenant"])
        else:
            df["cpm_tenant"] = 32.0

        df["cost_client"] = df.apply(
            lambda row: row["impressions"] / 1000 * row["cpm_client"], axis=1
        )
        df["cost_tenant"] = df.apply(
            lambda row: row["impressions"] / 1000 * row["cpm_tenant"], axis=1
        )

        df.groupby(["agency", "advertiser", "campaign"]).agg(
            {
                "date": ["min", "max"],
                "impressions": "sum",
                "cpm_tenant": "mean",
                "cost_tenant": "sum",
                "cpm_client": "mean",
                "cost_client": "sum",
            }
        ).to_excel("test.xlsx")


def generate_report_accounting(provider, agency_uid, start, end):
    qf = provider["insights"]
    qf = qf[
        [
            qf["agency"],
            qf["advertiser"],
            qf["campaign"],
            qf["date"],
            qf["impressions"],
        ]
    ]
    qf = qf[
        (qf["agency_uid"] == agency_uid) & (qf["date"] >= start) & (qf["date"] < end)
    ]
    return qf


def generate_reports_insights(delta):
    if isinstance(delta, relativedelta):
        start = datetime.utcnow().date() + delta
        end = datetime.utcnow().date()
    if isinstance(delta, tuple):
        start = delta[0]
        end = delta[1]

    provider = from_bind("roku")
    for agency in TABLE_CLIENTS["agencies"].list_entities():
        logging.warn(agency["OneView_AgencyID"])
        df: pd.DataFrame = get_insights(
            provider, agency["OneView_AgencyID"], start, end
        ).to_pandas()
        # if "CPM_Client" in agency.keys():
        #     df["cpm_client"] = float(agency["CPM_Client"])
        # else:
        #     df["cpm_client"] = 40.0
        # if "CPM_Client" in agency.keys():
        #     df["cpm_tenant"] = float(agency["CPM_Tenant"])
        # else:
        #     df["cpm_tenant"] = 32.0

        # df["cost_client"] = df.apply(
        #     lambda row: row["impressions"] / 1000 * row["cpm_client"], axis=1
        # )
        # df["cost_tenant"] = df.apply(
        #     lambda row: row["impressions"] / 1000 * row["cpm_tenant"], axis=1
        # )

        df.groupby(["agency", "advertiser", "campaign", "creative_name"]).agg(
            {
                "date": ["min", "max"],
                "impressions": "sum",
                "video_one_quarter_count": "sum",
                "video_midpoint_count": "sum",
                "video_three_quarter_count": "sum",
                "video_complete_count": "sum",
            }
        ).to_excel(f'{agency["OneView_AgencyID"]}.xlsx')


def get_insights(provider, agency_uid, start, end):
    qf = provider["insights"]
    qf = qf[
        [
            qf["agency"],
            qf["advertiser"],
            qf["campaign"],
            qf["creative_name"],
            qf["date"],
            qf["impressions"],
            qf["video_one_quarter_count"],
            qf["video_midpoint_count"],
            qf["video_three_quarter_count"],
            qf["video_complete_count"],
        ]
    ]
    qf = qf[
        (qf["agency_uid"] == agency_uid) & (qf["date"] >= start) & (qf["date"] < end)
    ]
    return qf


@bp.logger()
@bp.timer_trigger(arg_name="timer", schedule="0 0 0 * * TUE,FRI")
def roku_async_schedule_report_semiweekly(timer: TimerRequest) -> None:
    generate_reports_insights(relativedelta(days=-30))


@bp.logger()
@bp.timer_trigger(arg_name="timer", schedule="0 0 0 1 * *")
def roku_async_schedule_report_monthly(timer: TimerRequest) -> None:
    now = datetime.utcnow()
    end = datetime(now.year, now.month, 1)
    start = end + relativedelta(months=-1)
    generate_reports_accounting((start, end))
