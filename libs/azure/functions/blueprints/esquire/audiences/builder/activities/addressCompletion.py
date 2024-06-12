# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/filterResults.py.py

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from azure.durable_functions import Blueprint
from sqlalchemy import select, func
from urllib.parse import unquote
import os, pandas as pd, uuid

from libs.data import from_bind

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_addressCompletion(ingress: dict):
    if isinstance(ingress["source"], str):
        input_blob = BlobClient.from_blob_url(ingress["source"])
    else:
        input_blob = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["source"]["conn_str"]],
            container_name=ingress["source"]["container_name"],
            blob_name=ingress["source"]["blob_name"],
        )
    df = pd.read_csv(input_blob.download_blob())
    df = df[~df["zip4"].isnull()][["zipcode", "zip4"]]

    output_blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["destination"]["conn_str"]],
        container_name=ingress["destination"]["container_name"],
        blob_name="{}/{}.csv".format(
            ingress["destination"]["blob_prefix"],
            uuid.uuid4().hex,
        ),
    )

    query = """SELECT {} FROM {} WHERE CONCAT(zipCode,'-',plus4code) in ({}""".format(
        "address AS street, city, state, zipCode AS zip, plus4code AS zip4",
        "addresses",
        (
            "'"
            + (
                "','".join(
                    df.apply(
                        lambda r: str(r["zipcode"]).zfill(5)
                        + "-"
                        + str(r["zip4"]).zfill(4),
                        axis=1,
                    )
                    .drop_duplicates()
                    .to_list()
                )
            )
            + "'"
        ),
    )

    provider = from_bind("audiences")
    addresses = provider.models["dbo"]["addresses"]
    session = provider.connect()
    query = select(
        addresses.address.label("street"),
        addresses.city,
        addresses.state,
        addresses.zipCode.label("zip"),
        addresses.plus4Code.label("zip4"),
    ).where(
        func.concat(addresses.zipCode, "-", addresses.plus4Code).in_(
            df.apply(
                lambda r: str(r["zipcode"]).zfill(5) + "-" + str(r["zip4"]).zfill(4),
                axis=1,
            )
            .drop_duplicates()
            .to_list()
        )
    )
    df = pd.DataFrame(session.execute(query).all())

    output_blob.upload_blob(df.to_csv(index=False))
    return (
        unquote(output_blob.url)
        + "?"
        + generate_blob_sas(
            account_name=output_blob.account_name,
            container_name=output_blob.container_name,
            blob_name=output_blob.blob_name,
            account_key=output_blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )
