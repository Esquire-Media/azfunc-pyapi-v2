from azure.functions import Out
from datetime import datetime
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.utils.gravityforms import extract_answers
import simplejson as json

bp = Blueprint()


@bp.route(route="io", methods=["POST"])
@bp.table_output(
    arg_name="table", connection="AzureWebJobsStorage", table_name="formData"
)
async def io_collector(req: HttpRequest, table: Out[str]):
    table.set(
        json.dumps(
            {
                "PartitionKey": req.params["FormID"],
                "RowKey": req.params["EntryID"],
                "CreationTime": datetime.utcnow().isoformat(),
                **extract_answers(req.get_json()),
            }
        )
    )

    return HttpResponse("OK")

from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas


@bp.route(route="io/{FormID}/{EntryID}", methods=["GET"])
@bp.table_input(
    arg_name="message",
    connection="AzureWebJobsStorage",
    table_name="formData",
    partition_key="{FormID}",
    row_key="{EntryID}",
)
async def io_generate_pdf(req: HttpRequest, message: str):
    pdf = BytesIO()
    data = json.loads(message)[0]

    c = canvas.Canvas(pdf, pagesize=letter)
    
    # You can adjust these values for your specific layout
    width, height = letter
    
    # Add your own business details
    c.drawString(50, height - 50, "Esquire Media, LLC")
    c.drawString(50, height - 70, "Business Address")
    
    # Add customer details
    c.drawString(350, height - 50, data['Name First'])
    c.drawString(350, height - 70, data['Name Last'])

    # Add invoice details
    c.drawString(50, height - 150, f"Invoice Number: @{req.route_params['FormID']}@{req.route_params['EntryID']}")
    c.drawString(50, height - 170, f"Date: {data['CreationTime']}")

    # Add item details - assuming 'items' is a list of tuples like (description, cost)
    items = [
        ('Product 1', 19.99),
        ('Product 2', 29.99),
        ('Product 3', 39.99),
    ]
    line_height = 14
    table_height = height - 200
    for i, item in enumerate(items):
        description, cost = item
        c.drawString(50, table_height - (i * line_height), description)
        c.drawString(500, table_height - (i * line_height), f'${cost:.2f}')

    # Total cost
    total_cost = sum(item[1] for item in items)
    c.drawString(50, height - 300, f"Total: ${total_cost:.2f}")
    
    c.save()
    
    pdf.seek(0)
    
    return HttpResponse(pdf.read(), headers={"Content-Disposition": "attachment"})
