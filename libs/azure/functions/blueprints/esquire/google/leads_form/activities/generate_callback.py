import os
from datetime import datetime as dt, timedelta
from libs.azure.storage.blob.sas import get_blob_download_url
from libs.azure.functions import Blueprint
from azure.storage.blob import BlobClient

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_googleLeadsForm_generateCallback(ingress: dict):
   
   # return a formatted HTML string to use as the email body
    return format_model_as_html(model=ingress)

def build_table_row(key, value) -> str:
    return f"""
    <tr>
        <td><strong>{key}</strong></td>
        <td>{value}</td>
    </tr>
    """.replace(
        "\n", ""
    )


def format_model_as_html(model: dict) -> str:
    # return an HTML email with a data table and a logo-based email signature
    return f"""
    <p>A new Google Forms Lead submission was detected by Esquire Advertising:</p>
    <p>&nbsp;</p>
    <h3><span style="text-decoration: underline;">Summary</span></h3>
    <table>
        <tbody>
            {''.join([build_table_row(k,v) for k,v in model['user_column_data'].items()])}
        </tbody>
    </table>

    <p>&nbsp;</p>
    <table border="0" cellspacing="0" cellpadding="0">
    <tbody>
    <tr>
    <td valign="top">
    <p><img class="Do8Zj" src="data:image/png;base64,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" width="143" height="78" crossorigin="use-credentials" data-imagetype="AttachmentByCid" data-custom="AAMkAGM2OGY4YTkwLTJkMzQtNGI2Ni05ZGNiLTI2OWMzYzVhZDRmYwBGAAAAAABESFfjcyjWRbx6e%2BCCO0UYBwABeSoIwUD%2BQosr8MPG9XP8AAAAAAEJAAABeSoIwUD%2BQosr8MPG9XP8AALwmQstAAABEgAQAHOFeptaxVZHn7N2ktRGFFI%3D" data-outlook-trace="F:2|T:2" /></p>
    </td>
    <td valign="top">
    <p><strong><span data-ogsc="rgb(241, 90, 41)">&nbsp;</span></strong></p>
    </td>
    <td valign="top">
    <p><strong>Esquire Advertising</strong></p>
    <p><em>For questions, contact us at:</em></p>
    <p><strong><a href="mailto:hello@esquireadvertising.com">hello@esquireadvertising.com</a></strong></p>
    <p>&nbsp;</p>
    </td>
    </tr>
    </tbody>
    </table>
    """.replace(
        "\n", ""
    )
