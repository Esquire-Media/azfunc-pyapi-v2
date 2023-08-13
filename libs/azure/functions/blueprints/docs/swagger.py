from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.openapi.clients import specifications
import simplejson as json

# Create a Blueprint instance
bp = Blueprint()


@bp.route(route="swagger", methods=["GET"])
async def swagger_ui(req: HttpRequest):
    return HttpResponse(
        f"""<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width, user-scalable=no, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="ie=edge">
    <title>Esquire API Integrations</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.3.1/swagger-ui-bundle.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.3.1/swagger-ui-standalone-preset.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.3.1/swagger-ui.css" />
    <style>
        body {{
            margin: 0
        }}
    </style>
</head>
<body>
<div id="swagger-ui">

</div>
<script>
    const ui = SwaggerUIBundle({{
        urls: {json.dumps([{"url": f"/api/docs/yaml/{key}", "name": key} for key in specifications.keys()])},
        dom_id: '#swagger-ui',
        defaultModelsExpandDepth: -1,
        deepLinking: true,
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        plugins: [
            SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
    }})
</script>
</body>
</html>
""",
        headers={"Content-Type": "text/html; charset=utf-8"},
    )
