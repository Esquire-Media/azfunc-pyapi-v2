# File: libs/azure/functions/blueprints/esquire/audiences/maids/addresses/activities/new_movers.py

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
import pandas as pd, os

bp: Blueprint = Blueprint()

INCOME_MAP = {
    "15 K": 15000,
    "20 K": 20000,
    "25 K": 25000,
    "30 K": 30000,
    "35 K": 35000,
    "40 K": 40000,
    "45 K": 45000,
    "50 K": 50000,
    "55 K": 55000,
    "60 K": 60000,
    "65 K": 65000,
    "70 K": 70000,
    "75 K": 75000,
    "80 K": 80000,
    "85 K": 85000,
    "90 K": 90000,
    "95 K": 95000,
    "100 K": 100000,
    "125 K": 125000,
    "150 K": 150000,
    "175 K": 175000,
    "200 K": 200000,
    "250 K": 250000,
    "300 K": 300000,
    "350 K": 350000,
    "400 K": 400000,
    "450 K": 450000,
    "500 K": 500000,
    "550 K": 550000,
    "600 K": 600000,
    "650 K": 650000,
    "700 K": 700000,
    "750 K": 750000,
    "800 K": 800000,
    "850 K": 850000,
    "900 K": 900000,
    "950 K": 950000,
    "1 M": 1000000,
    "2 M": 2000000,
}
PREMIUM_HOME_VALUE_MAP = {
    "50 K": 50000,
    "100 K": 100000,
    "150 K": 150000,
    "200 K": 200000,
    "250 K": 250000,
    "300 K": 300000,
    "350 K": 350000,
    "400 K": 400000,
    "450 K": 450000,
    "500 K": 500000,
    "550 K": 550000,
    "600 K": 600000,
    "650 K": 650000,
    "700 K": 700000,
    "750 K": 750000,
    "800 K": 800000,
    "850 K": 850000,
    "900 K": 900000,
    "950 K": 950000,
    "1 M": 1000000,
    "1.5 M": 1500000,
    "2 M": 2000000,
}
STATE_ABBREVIATIONS_MAP = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}


# activity to fill in the geo data for each audience object
@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesMaids_newMovers(ingress: dict):
    addresses = get_addresses(audience_id=ingress["audience"]["id"])

    # Handle data output based on destination configuration
    if ingress.get("destination"):
        # Configuring BlobClient for data upload
        if isinstance(ingress["destination"], str):
            blob = BlobClient.from_blob_url(ingress["destination"])
        elif isinstance(ingress["destination"], dict):
            blob = BlobClient.from_connection_string(
                conn_str=os.environ[ingress["destination"]["conn_str"]],
                container_name=ingress["destination"]["container_name"],
                blob_name=ingress["destination"]["blob_name"],
            )
            to_type = ingress["destination"].get("format", None)
        if not to_type:
            _, to_type = os.path.splitext(blob.blob_name)
            if not to_type:
                to_type = "csv"
        blob.upload_blob(
            getattr(addresses, "to_" + to_type.replace(".", ""))(),
            overwrite=True,
        )

        return (
            blob.url
            + "?"
            + generate_blob_sas(
                account_name=blob.account_name,
                account_key=blob.credential.account_key,
                container_name=blob.container_name,
                blob_name=blob.blob_name,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + relativedelta(days=2),
            )
        )
    else:
        # Return cleaned addresses as a dictionary
        return addresses.to_dict(orient="records")


def get_addresses(audience_id: str):
    # connect to Synapse salesforce database
    provider = from_bind("salesforce")
    session: Session = provider.connect()

    audience = provider.models["dbo"]["Audience__c"]
    zipcode_join = provider.models["dbo"]["Zipcode_Join__c"]
    zipcodes = provider.models["dbo"]["Zipcodes__c"]

    aud_details = (
        pd.DataFrame(
            session.query(
                audience.New_Construction__c,
                audience.New_Homeowner__c,
                audience.Moved_to_State__c,
                audience.Move_from_State__c,
                audience.Income_Greater_Than__c,
                audience.Income_Less_Than__c,
                audience.Premium_Home_Value_Greater_Than__c,
                audience.Premium_Home_Value_Less_Than__c,
                audience.Age_Greater_Than__c,
                audience.Age_Less_Than__c,
                audience.Move_to_a_Different__c,
                audience.Moved_to_the_Same__c,
                audience.Single_Family__c,
                audience.Multi_Family__c,
            ).filter(audience.Id == audience_id)
        )
        .iloc[0]
        .to_dict()
    )

    zips = [
        row[0]
        for row in session.query(zipcodes.Name)
        .join(
            zipcode_join,
            zipcode_join.Zipcode__c == zipcodes.Id,
        )
        .filter(zipcode_join.Audience__c == audience_id)
        .all()
    ]

    session.close()

    aud_provider = from_bind("audiences")
    aud_session: Session = aud_provider.connect()
    movers = aud_provider.models["dbo"]["movers"]

    address_query = aud_session.query(
        movers.address,
        movers.city,
        movers.state,
        movers.zipcode,
        movers.plus4Code,
    ).filter(movers.zipcode.in_(zips))

    # new construction and new homeowner might be manual pulls, ask
    if aud_details["New_Construction__c"]:
        pass
    if aud_details["New_Homeowner__c"]:
        pass
    if aud_details[
        "Moved_to_State__c"
    ]:  # MIGHT need map for full state names/abbreviations, CAN HAVE MULTIPLE VALES ()
        address_query = address_query.filter(
            movers.state in aud_details["Moved_to_State__c"]
        )  # check distinct to see what multiple items looks like to format this
        # Select Distinct came up with only 'Null'
    if aud_details["Move_from_State__c"]:
        address_query = address_query.filter(
            movers.oldState == aud_details["Move_from_State__c"]
        )
        # Select Distinct came up with only 'Null'
    if aud_details["Income_Greater_Than__c"]:  #
        address_query = address_query.filter(
            movers.estimatedIncome
            > INCOME_MAP.get(aud_details["Income_Greater_Than__c"], 0)
        )
    if aud_details["Income_Less_Than__c"]:
        address_query = address_query.filter(
            movers.estimatedIncome
            < INCOME_MAP.get(aud_details["Income_Less_Than__c"], 0)
        )
    if aud_details["Premium_Home_Value_Greater_Than__c"]:
        address_query = address_query.filter(
            movers.estimatedHomeValue
            > PREMIUM_HOME_VALUE_MAP.get(
                aud_details["Premium_Home_Value_Greater_Than__c"], 0
            )
        )
    if aud_details["Premium_Home_Value_Less_Than__c"]:
        address_query = address_query.filter(
            movers.estimatedHomeValue
            < PREMIUM_HOME_VALUE_MAP.get(
                aud_details["Premium_Home_Value_Greater_Than__c"], 0
            )
        )
    if aud_details["Age_Greater_Than__c"]:
        address_query = address_query.filter(
            movers.estimatedAge > aud_details["Age_Greater_Than__c"]
        )
    if aud_details["Age_Less_Than__c"]:
        address_query = address_query.filter(
            movers.estimatedAge < aud_details["Age_Less_Than__c"]
        )
    if aud_details["Move_to_a_Different__c"]:
        # different filter based on value of move to different if it is zip code, city, or state
        address_query = address_query.filter(movers.state != movers.oldState)
        # Select Distinct came up with only 'Null'
    if aud_details["Moved_to_the_Same__c"]:  # same as above
        address_query = address_query.filter(movers.state == movers.oldState)
        # Select Distinct came up with only 'Null'
    if aud_details["Single_Family__c"]:
        address_query = address_query.filter(movers.addresstype == "SingleFamily")
    if aud_details["Multi_Family__c"]:
        address_query = address_query.filter(movers.addresstype == "Highrise")

    addresses = pd.DataFrame(address_query.all())

    aud_session.close()

    return addresses
