import adaptive_cards.card_types as types
from adaptive_cards.card import AdaptiveCard
from adaptive_cards.elements import TextBlock, Image
from adaptive_cards.containers import Container, ContainerTypes, ColumnSet, Column
import httpx
import json
import logging
from libs.azure.functions import Blueprint

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function that logs a message
@bp.activity_trigger(input_name="ingress")
def activity_microsoftGraph_postErrorCard(ingress: dict):
    """
    Params:

    function_name   : Name of the Azure function.
    instance_id     : ID of the particular function instance.
    owners          : List of MS emails for users that will be @ mentioned in the card.
    error           : Detailed error log (will be truncated to the first 10 lines).
    icon_url        : URL for a custom image to associate with this error. Used to quickly differentiate errors.
    webhook         : URL of webhook where the adaptive card will be sent.
    """

    # initialize the object that will contain all card elements
    containers: list[ContainerTypes] = []

    # build a summary section with meta information
    containers.append(
        Container(
            items=[
                TextBlock(text="Summary", size=types.FontSize.MEDIUM, weight="Bolder"),
                ColumnSet(
                    columns=[
                        Column(
                            items=[
                                TextBlock(text="Function Name"),
                                TextBlock(text="Instance ID"),
                                TextBlock(text="Owner(s)"),
                            ],
                            width="100px",
                        ),
                        Column(
                            items=[
                                TextBlock(text=ingress["function_name"]),
                                TextBlock(text=ingress["instance_id"]),
                                TextBlock(
                                    text=" ".join(
                                        [f"<at>{owner['name']}</at>" for owner in ingress['owners']]
                                    )
                                ),
                            ],
                            spacing=types.Spacing.MEDIUM,
                            rtl=False,
                        ),
                    ],
                    separator=True,
                ),
            ],
            spacing=types.Spacing.SMALL,
        )
    )

    # build a details section with a truncated error trace
    containers.append(
        Container(
            items=[
                TextBlock(text="Details", size=types.FontSize.MEDIUM, weight="Bolder"),
                TextBlock(
                    text=ingress["error"],
                    size=types.FontSize.SMALL,
                    wrap=True,
                    max_lines=10,
                    separator=True,
                ),
            ]
        )
    )

    # build Adaptive card from container elements
    card = AdaptiveCard.new().version("1.5").add_items(containers).create()

    # key exports as "schema", but it needs to be "$schema"
    content = {
        "$" + k if k == "schema" else k: v for k, v in json.loads(card.to_json()).items()
    }
    # add entities for owner mentions
    content["msteams"] = {
        "entities": [
            {
                "type": "mention",
                "text": f"<at>{owner['name']}</at>",
                "mentioned": {"id": owner["id"], "name": owner["name"]},
            }
            for owner in ingress['owners']
        ]
    }

    # send card to the specified webhook
    return httpx.Client(timeout=None).request(
        method="POST",
        url=ingress["webhook"],
        json={
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": content,
                }
            ],
        },
    ).content