from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.openapi.clients import specifications
from urllib.parse import urlparse
import simplejson as json
import yaml

# Create a Blueprint instance
bp = Blueprint()


@bp.route(route="docs/rapidoc/swaggish/{spec}", methods=["GET"])
async def rapidoc_swaggish(req: HttpRequest):
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
      spec-url = "/api/docs/yaml/{req.route_params.get("spec")}"
      theme = "light"
      render-style = "view"
      default-schema-tab = "schema"
      show-components = "true"
      schema-description-expanded = "true"
    > </rapi-doc>
  </body>
</html>
""",
        headers={"Content-Type": "text/html; charset=utf-8"},
    )

@bp.route(route="docs/rapidoc/{spec}", methods=["GET"])
async def rapidoc(req: HttpRequest):
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
      spec-url = "/api/docs/yaml/{req.route_params.get("spec")}"
      theme = "light"
      render-style = "focus"
      show-method-in-nav-bar = "as-colored-block"
      use-path-in-nav-bar = "true"
      show-components = "true"
      show-curl-before-try = "true"
      allow-try = "false"
      schema-description-expanded = "true"
      default-schema-tab = "schema"
    > </rapi-doc>
  </body>
</html>
""",
        headers={"Content-Type": "text/html; charset=utf-8"},
    )