from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.openapi.clients import specifications
from urllib.parse import urlparse
import simplejson as json
import yaml

# Create a Blueprint instance
bp = Blueprint()


@bp.route(route="docs/redoc/{spec}", methods=["GET"])
async def redoc(req: HttpRequest):
    return HttpResponse(
        f"""<!DOCTYPE html>
<html>
  <head>
    <title>Esquire API Integrations</title>
    <!-- needed for adaptive design -->
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">

    <!--
    Redoc doesn't change outer page styles
    -->
    <style>
      body {{
        margin: 0;
        padding: 0;
      }}
    </style>
  </head>
  <body>
    <redoc spec-url='/api/docs/yaml/{req.route_params.get("spec")}' show-header=false></redoc>
    <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"> </script>
  </body>
</html>
""",
        headers={"Content-Type": "text/html; charset=utf-8"},
    )
