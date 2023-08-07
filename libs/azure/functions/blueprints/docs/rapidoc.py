from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.openapi.clients import specifications
from urllib.parse import urlparse
import simplejson as json
import yaml

# Create a Blueprint instance
bp = Blueprint()


@bp.route(route="docs/rapidoc/{spec}", methods=["GET"])
async def rapidoc(req: HttpRequest):
    url = urlparse(req.url)
    url = f"{url.scheme}://{url.hostname}{':'+str(url.port) if url.port else ''}/api/docs/yaml"
    return HttpResponse(
        f"""<!doctype html> <!-- Important: must specify -->
<html>
  <head>
    <title>Esquire API Integrations</title>
    <meta charset="utf-8"> <!-- Important: rapi-doc uses utf8 characters -->
    <script type="module" src="https://unpkg.com/rapidoc/dist/rapidoc-min.js"></script>
  </head>
  <body>
    <rapi-doc
      spec-url = "{url}/{req.route_params.get("spec")}"
    > </rapi-doc>
  </body>
</html>
""",
        headers={"Content-Type": "text/html; charset=utf-8"},
    )
