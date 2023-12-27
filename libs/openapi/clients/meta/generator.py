# File: libs/openapi/clients/meta/generator.py

from libs.utils.jupyter import is_running_in_jupyter_notebook, get_jupyter_event_loop
from typing import Any, Dict, Tuple
import asyncio, orjson, httpx, logging


async def read_files_from_github_async(
    owner: str = "facebook",
    repo: str = "facebook-business-sdk-codegen",
    folder_path: str = "api_specs/specs",
    file_extension: str = ".json",
):
    """
    Asynchronously reads files from a specified GitHub repository and folder path.

    Parameters
    ----------
    owner : str, optional
        The owner of the GitHub repository.
    repo : str, optional
        The name of the GitHub repository.
    folder_path : str, optional
        The path to the folder in the repository.
    file_extension : str, optional
        The file extension of the files to be read. Reads all files if empty.

    Returns
    -------
    dict
        A dictionary where keys are file names and values are the file contents.

    Notes
    -----
    This function uses asynchronous HTTP requests to retrieve file data.
    """
    async with httpx.AsyncClient() as client:
        api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{folder_path}"

        response = await client.get(api_url)
        if response.status_code != 200:
            print("Failed to retrieve data from GitHub")
            return []

        files = [
            file
            for file in orjson.loads(response.content)
            if (file["name"].endswith(file_extension) if file_extension else True)
        ]

        file_contents = {}
        tasks = []
        for file in files:
            task = asyncio.create_task(read_file(client, file))
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        for file_name, content in results:
            if content is not None:
                file_contents[file_name] = content
            else:
                print(f"Failed to read file {file_name}")

        return file_contents


async def read_file(client, file):
    """
    Asynchronously reads the content of a single file from a GitHub repository.

    Parameters
    ----------
    client : httpx.AsyncClient
        The HTTP client used for making asynchronous requests.
    file : dict
        A dictionary containing information about the file (e.g., download URL).

    Returns
    -------
    tuple
        A tuple containing the file name and its content. Returns None for content if reading fails.
    """
    file_response = await client.get(file["download_url"])
    if file_response.status_code == 200:
        return file["name"], orjson.loads(file_response.content)
    else:
        return file["name"], None


def read_files_from_github(
    owner: str = "facebook",
    repo: str = "facebook-business-sdk-codegen",
    folder_path: str = "api_specs/specs",
    file_extension: str = ".json",
):
    """
    Synchronous wrapper for `read_files_from_github_async`. This function facilitates calling the asynchronous
    `read_files_from_github_async` function in a synchronous context.

    Parameters
    ----------
    owner : str
        The owner of the GitHub repository.
    repo : str
        The name of the GitHub repository.
    folder_path : str
        The path to the folder in the repository.
    file_extension : str
        The file extension of the files to be read.

    Returns
    -------
    dict
        A dictionary where keys are file names and values are the file contents.
    """
    import nest_asyncio

    if is_running_in_jupyter_notebook():
        loop = get_jupyter_event_loop()
    else:
        loop = asyncio.get_event_loop()

    nest_asyncio.apply(loop)
    return loop.run_until_complete(
        read_files_from_github_async(owner, repo, folder_path, file_extension)
    )


def generate_openapi(json_files: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Generate an OpenAPI v3.1 specification from the facebook-business-sdk-codegen JSON spec files.

    This function processes input JSON files to create a dictionary representing an OpenAPI specification.
    It handles complex types, enumerations, and includes predefined schemas for common data types.

    Parameters
    ----------
    json_files : Dict[str, Any]
        A dictionary with filenames as keys and file content as values.

    Returns
    -------
    Dict[str, Any]
        A dictionary representing the OpenAPI v3.1 specification.
    """
    if not json_files:
        json_files = read_files_from_github()

    parameter_separator = "-"

    # Initialize base structure of OpenAPI specification
    openapi_spec = {
        "openapi": "3.1.0",
        "info": {"title": "Facebook Business API", "version": "18.0"},
        "servers": [{"url": "https://graph.facebook.com/v18.0"}],
        "components": {
            "headers": {
                "X-Business-Use-Case-Usage": {
                    "description": "Contains usage and throttling data.",
                    "schema": {"$ref": "#/components/schemas/BusinessUseCase"},
                }
            },
            "parameters": {
                "Limit": {
                    "in": "query",
                    "name": "limit",
                    "schema": {"type": "integer"},
                },
                "After": {"in": "query", "name": "after", "schema": {"type": "string"}},
                "Before": {
                    "in": "query",
                    "name": "before",
                    "schema": {"type": "string"},
                },
                "Filtering": {
                    "in": "query",
                    "name": "filtering",
                    "schema": {"$ref": "#/components/schemas/Filtering"},
                    "style": "form",
                    "explode": False,
                },
            },
            "schemas": {
                "BusinessUseCase": {"type": "string"},
                "Date": {
                    "type": "string",
                    "format": "date",
                    "example": "2018-03-20",
                },
                "DateTime": {
                    "type": "string",
                    "format": "datetime",
                    "example": "2017-07-21T17:32:28Z",
                },
                "Filtering": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field": {"type": "string"},
                            "operator": {"type": "string"},
                            "value": {"type": "string"},
                        },
                    },
                },
                "List": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/Object"},
                },
                "Object": {
                    "type": "object",
                    "additionalProperties": True,
                },
                "Pagination": {
                    "type": "object",
                    "properties": {
                        "paging": {
                            "type": "object",
                            "properties": {
                                "cursors": {
                                    "type": "object",
                                    "properties": {
                                        "after": {"type": "string"},
                                        "before": {"type": "string"},
                                    },
                                },
                                "previous": {"type": "string"},
                                "next": {"type": "string"},
                            },
                        }
                    },
                },
            },
            "securitySchemes": {
                "AccessToken": {
                    "type": "apiKey",
                    "in": "query",
                    "name": "access_token",
                }
            },
            "responses": {
                "Object": {
                    "description": "Unknown response",
                    "content": {
                        "application/json": {
                            "schema": {
                                "oneOf": [
                                    {"$ref": "#/components/schemas/Object"},
                                    {
                                        "type": "object",
                                        "allOf": [
                                            {"$ref": "#/components/schemas/Pagination"}
                                        ],
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": {
                                                    "$ref": "#/components/schemas/Object"
                                                },
                                            }
                                        },
                                    },
                                ]
                            }
                        }
                    },
                    "headers": {
                        "X-Business-Use-Case-Usage": {
                            "$ref": "#/components/headers/X-Business-Use-Case-Usage"
                        }
                    },
                }
            },
        },
        "paths": {},
        "security": [{"AccessToken": []}],
        "tags": [],
    }

    def convert_to_openapi_type(json_type: str) -> Dict[str, Any]:
        """
        Convert a JSON type to an OpenAPI compatible type or reference.

        This function maps basic JSON types to their OpenAPI counterparts, handling complex types such as
        objects and arrays, and provides references to predefined schemas for known types.

        Parameters
        ----------
        json_type : str
            The JSON type to convert.

        Returns
        -------
        Dict[str, Any]
            A dictionary representing the OpenAPI type or reference.
        """
        basic_types = {
            # Mapping of JSON types to OpenAPI types
            "string": "string",
            "file": "string",
            "number": "number",
            "float": "number",
            "integer": "integer",
            "int": "integer",
            "unsigned int": "integer",
            "boolean": "boolean",
            "bool": "boolean",
            "object": "object",
            "Object": "object",
            "map": "object",
            "array": "array",
            "null": None,
        }

        # Convert basic types
        if json_type in basic_types:
            match basic_types[json_type]:
                case "object":
                    return {"$ref": "#/components/schemas/Object"}
                case _:
                    return {"type": basic_types[json_type]}

        # Handle list and map types
        if json_type.startswith("list<"):
            item_type = json_type[5:-1]
            return {"type": "array", "items": convert_to_openapi_type(item_type)}

        if json_type.startswith("map<"):
            key_value_types = json_type[4:-1].split(",")
            if len(key_value_types) < 2:
                return {"$ref": "#/components/schemas/Object"}
            return {
                "type": "object",
                "additionalProperties": convert_to_openapi_type(
                    key_value_types[1].strip()
                ),
            }

        match json_type:
            case "date":
                return {"$ref": "#/components/schemas/Date"}
            case "datetime":
                return {"$ref": "#/components/schemas/DateTime"}
            case "list":
                return {"$ref": "#/components/schemas/List"}
            case _:
                return {"$ref": f"#/components/schemas/{json_type}"}

    def process_enumeration(enumeration: Dict[str, Any]) -> None:
        """
        Process and add an enumeration to the OpenAPI specification.

        This function takes an enumeration definition and adds it to the OpenAPI schemas section.

        Parameters
        ----------
        enumeration : Dict[str, Any]
            A dictionary representing the enumeration to process.
        """
        enum_schema = {"type": "string", "enum": enumeration["values"]}
        openapi_spec["components"]["schemas"][enumeration["name"]] = enum_schema

    def process_api(
        api: Dict[str, Any], object_id: str
    ) -> Tuple[str, str, Dict[str, Any]]:
        """
        Process an individual API definition and return its path, method, and operation details.

        This helper function extracts necessary information from an API definition to construct a path,
        method, and operation dictionary as per OpenAPI specification.

        Parameters
        ----------
        api : Dict[str, Any]
            A dictionary representing the API definition.
        object_id : str
            The object ID associated with the API.

        Returns
        -------
        tuple
            A tuple containing the API path, method, and operation dictionary.
        """
        path = (
            "/{{{}{}Id}}/{}".format(object_id, parameter_separator, e)
            if (e := api.get("endpoint", ""))
            else "/{{{}{}Id}}".format(object_id, parameter_separator)
        )
        operationId = "{}.{}".format(
            object_id, api.get("name", api["method"]).replace("#", "").title()
        )
        if api.get("endpoint"):
            operationId += "." + api["endpoint"].replace("_", " ").title().replace(
                " ", ""
            )
        method = api["method"].lower()
        operation = {
            "operationId": operationId,
            "responses": {},
        }
        if "return" in api:
            if operationId == "User.Get.Accounts":
                logging.warning(api['return'])
            return_ref = f"{api['return']}"
            response_ref = (
                ref
                if openapi_spec["components"]["schemas"].get(
                    (
                        (ref := convert_to_openapi_type(api["return"])).get("$ref", "")
                    ).split("/")[-1]
                )
                else {"$ref": "#/components/schemas/Object"}
            )
            openapi_spec["components"]["responses"][return_ref] = {
                "description": "Successful response",
                "content": {
                    "application/json": {
                        "schema": {
                            "oneOf": [
                                response_ref,
                                {
                                    "type": "object",
                                    "allOf": [
                                        {"$ref": "#/components/schemas/Pagination"}
                                    ],
                                    "properties": {
                                        "data": {
                                            "type": "array",
                                            "items": response_ref,
                                        }
                                    },
                                },
                            ],
                        }
                    }
                },
                "headers": {
                    "X-Business-Use-Case-Usage": {
                        "$ref": "#/components/headers/X-Business-Use-Case-Usage"
                    }
                },
            }
        if return_ref != "Object":
            operation["responses"]["200"] = {
                "$ref": f"#/components/responses/{return_ref}"
            }
        operation["responses"]["default"] = {"$ref": "#/components/responses/Object"}
        if "params" in api:
            # Use references to parameters instead of defining them in the operation
            operation["parameters"] = []
            for param in api["params"]:
                if param["name"] != "fields":
                    param_ref = "{}{}{}".format(
                        object_id,
                        parameter_separator,
                        param["name"].replace("_", " ").title().replace(" ", ""),
                    )
                    openapi_spec["components"]["parameters"][param_ref] = {
                        "name": param["name"],
                        "in": "query",
                        "schema": convert_to_openapi_type(param["type"]),
                    }
                    operation["parameters"].append(
                        {"$ref": f"#/components/parameters/{param_ref}"}
                    )
            if method == "get":
                operation["parameters"] += [
                    {"$ref": "#/components/parameters/Limit"},
                    {"$ref": "#/components/parameters/After"},
                    {"$ref": "#/components/parameters/Before"},
                ]
        operation["tags"] = [object_id] + (
            ["Async"] if "async" in object_id.lower() else []
        )
        return path, method, operation

    # Process each file in the input dictionary
    for filename, content in json_files.items():
        object_id = filename.split(".")[0]

        # Handle dictionaries representing objects or APIs
        if isinstance(content, dict):
            if "fields" in content:
                schema = {"type": "object", "properties": {}}
                for field in content["fields"]:
                    schema["properties"][field["name"]] = convert_to_openapi_type(
                        field["type"]
                    )
                    openapi_spec["components"]["parameters"].setdefault(
                        "{}{}Fields".format(object_id, parameter_separator),
                        {
                            "in": "query",
                            "name": "fields",
                            "style": "form",
                            "explode": False,
                            "schema": {
                                "type": "array",
                                "items": {"type": "string", "enum": []},
                            },
                        },
                    )["schema"]["items"]["enum"].append(field["name"])

                openapi_spec["components"]["schemas"][object_id] = schema

        # Handle lists representing enumerations
        elif isinstance(content, list):
            for enum in content:
                process_enumeration(enum)

    for filename, content in json_files.items():
        object_id = filename.split(".")[0]
        if isinstance(content, dict):
            if "apis" in content:
                for api in content["apis"]:
                    path, method, operation = process_api(api, object_id)
                    # match object_id:
                    #     case "User":
                    #         default = "me"
                    #     case _:
                    #         default = None
                    openapi_spec["paths"].setdefault(path, {})["parameters"] = [
                        {
                            "name": "{}{}Id".format(object_id, parameter_separator),
                            "in": "path",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    ]
                    # if default:
                    #     openapi_spec["paths"][path]["parameters"][0][
                    #         "default"
                    #     ] = default
                    if openapi_spec["components"]["parameters"].get(
                        "{}{}Fields".format(object_id, parameter_separator)
                    ):
                        openapi_spec["paths"][path]["parameters"].append(
                            {
                                "$ref": "#/components/parameters/{}{}Fields".format(
                                    object_id, parameter_separator
                                )
                            }
                        )
                    openapi_spec["paths"][path][method] = operation
                    openapi_spec["tags"] += list(set([t for t in operation["tags"]]))
    # Deduplicate and sort tags
    openapi_spec["tags"] = list(set(openapi_spec["tags"]))
    openapi_spec["tags"].sort()
    openapi_spec["tags"] = [{"name": t} for t in openapi_spec["tags"]]

    return openapi_spec
