from libs.azure.functions import Blueprint
import mailchimp_marketing as MailchimpMarketing

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_mailchimp_updateListMembers(ingress: dict):
    client = MailchimpMarketing.Client()
    client.set_config({"api_key": ingress["api_key"], "server": ingress["server"]})
    client.lists.batch_list_members(
        ingress["list_id"], {"members": ingress["members"], "update_existing": True}
    )
    return ""
