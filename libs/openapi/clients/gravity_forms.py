from aiopenapi3 import OpenAPI
from aiopenapi3.plugin import Init
import copy
import os


class GraityFormsInitPlugin(Init):
    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def initialized(self, ctx: "Init.Context") -> "Init.Context":
        return super().initialized(ctx)


class GravityFormsAPI:
    def __new__(
        cls,
        url: str = os.environ.get("GRAVITY_FORMS_URL"),
        username: str = os.environ.get("GRAVITY_FORMS_USERNAME"),
        password: str = os.environ.get("GRAVITY_FORMS_PASSWORD"),
    ) -> OpenAPI:
        if url[-6:] != "/gf/v2":
            if url[-1] != "/":
                url += "/"
            url += "gf/v2"
        spec = copy.deepcopy(cls.spec)
        spec["servers"].append({"url": url})
        api = OpenAPI(
            url=url,
            document=spec,
        )
        api.authenticate(
            basicAuth={
                "username": username,
                "password": password,
            }
        )
        return api

    spec = {
        "openapi": "3.0.0",
        "info": {"title": "Gravity Forms REST API v2", "version": "2.0.0"},
        "servers": [],
        "paths": {
            "/forms": {
                "get": {
                    "summary": "Get all forms",
                    "parameters": [
                        {"$ref": "#/components/parameters/include"},
                    ],
                    "responses": {
                        "200": {
                            "description": "A list of forms",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/FormList"}
                                }
                            },
                        }
                    },
                }
            },
            "/forms/{formId}": {
                "get": {
                    "summary": "Get a form by ID",
                    "parameters": [
                        {"$ref": "#/components/parameters/formId"},
                    ],
                    "responses": {
                        "200": {
                            "description": "A form",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Form"}
                                }
                            },
                        }
                    },
                }
            },
            "/forms/{formId}/entries": {
                "get": {
                    "summary": "Get entries by form ID",
                    "parameters": [
                        {"$ref": "#/components/parameters/formId"},
                        {"$ref": "#/components/parameters/_field_ids"},
                        {"$ref": "#/components/parameters/_labels"},
                        {"$ref": "#/components/parameters/include"},
                        {"$ref": "#/components/parameters/paging"},
                        {"$ref": "#/components/parameters/sorting"},
                        {"$ref": "#/components/parameters/search"},
                    ],
                    "responses": {
                        "200": {
                            "description": "A form",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/EntryList"}
                                }
                            },
                        }
                    },
                }
            },
            "/forms/{formId}/results": {
                "get": {
                    "summary": "Get entries by form ID",
                    "parameters": [
                        {"$ref": "#/components/parameters/formId"},
                        {"$ref": "#/components/parameters/search"},
                    ],
                    "responses": {
                        "200": {
                            "description": "A form",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Results"}
                                }
                            },
                        }
                    },
                }
            },
            "/entries": {
                "get": {
                    "summary": "Get entries",
                    "parameters": [
                        {"$ref": "#/components/parameters/_field_ids"},
                        {"$ref": "#/components/parameters/_labels"},
                        {"$ref": "#/components/parameters/include"},
                        {"$ref": "#/components/parameters/paging"},
                        {"$ref": "#/components/parameters/sorting"},
                        {"$ref": "#/components/parameters/search"},
                    ],
                    "responses": {
                        "200": {
                            "description": "A list of entries",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/EntryList"}
                                }
                            },
                        }
                    },
                }
            },
            "/entries/{entryId}": {
                "get": {
                    "summary": "Get an entry by ID",
                    "parameters": [
                        {"$ref": "#/components/parameters/entryId"},
                        {"$ref": "#/components/parameters/_field_ids"},
                        {"$ref": "#/components/parameters/_labels"},
                    ],
                    "responses": {
                        "200": {
                            "description": "An entry",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Entry"}
                                }
                            },
                        }
                    },
                }
            },
        },
        "components": {
            "securitySchemes": {"basicAuth": {"type": "http", "scheme": "basic"}},
            "parameters": {
                "entryId": {
                    "name": "entryId",
                    "in": "path",
                    "required": True,
                    "description": "The ID of the entry",
                    "schema": {"type": "string"},
                },
                "formId": {
                    "name": "formId",
                    "in": "path",
                    "required": True,
                    "description": "The ID of the form",
                    "schema": {"type": "string"},
                },
                "form_ids": {
                    "name": "form_ids",
                    "in": "query",
                    "schema": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "An array of forms to include in the response.",
                    },
                },
                "_field_ids": {
                    "name": "_field_ids",
                    "in": "query",
                    "schema": {
                        "type": "string",
                        "description": "A comma separated list of fields to include in the response",
                    },
                },
                "_labels": {
                    "name": "_labels",
                    "in": "query",
                    "schema": {
                        "type": "integer",
                        "description": "Enables the inclusion of field labels in the results. Use the value “1” to include labels.",
                    },
                },
                "include": {
                    "name": "include",
                    "in": "query",
                    "schema": {"type": "array", "items": {"type": "number"}},
                },
                "paging": {
                    "name": "paging",
                    "in": "query",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "page_size": {
                                "type": "integer",
                                "description": "The number of results per page",
                            },
                            "current_page": {
                                "type": "integer",
                                "description": "The current page from which to pull details",
                            },
                            "offset": {
                                "type": "integer",
                                "description": "The record number on which to start retrieving data",
                            },
                        },
                    },
                    "style": "deepObject",
                    "explode": True,
                },
                "search": {
                    "name": "search",
                    "in": "query",
                    "schema": {
                        "type": "string"
                        # "type": "object",
                        # "properties": {
                        #     "status": {
                        #         "type": "string",
                        #         "description": "The status of the entry",
                        #     },
                        #     "mode": {
                        #         "type": "string",
                        #         "description": "The type of search to apply",
                        #     },
                        #     "field_filters": {
                        #         "type": "array",
                        #         "items": {
                        #             "type": "object",
                        #             "properties": {
                        #                 "key": {
                        #                     "type": "string",
                        #                     "description": "The field ID",
                        #                 },
                        #                 "value": {
                        #                     "type": "string",
                        #                     "description": "The value to search for",
                        #                 },
                        #                 "operator": {
                        #                     "type": "string",
                        #                     "description": "The comparison operator to use",
                        #                 },
                        #             },
                        #         },
                        #         "description": "An array of filters to search by",
                        #     },
                        # },
                    },
                },
                "sorting": {
                    "name": "sorting",
                    "in": "query",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "key": {
                                "type": "string",
                                "description": "The database field by which to sort.",
                            },
                            "direction": {
                                "type": "string",
                                "description": "The direction. Either ASC, DESC, or RAND (random order).",
                                "enum": ["ASC", "DESC", "RAND"],
                            },
                            "is_numeric": {
                                "type": "boolean",
                                "description": "If the key is numeric.",
                            },
                        },
                        "description": "The sorting criteria. The default sort is by entry id, descending (most recent entries first).",
                    },
                    "style": "deepObject",
                    "explode": True,
                },
            },
            "schemas": {
                "Form": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "integer",
                            "description": "The unique identifier of the form.",
                        },
                        "title": {
                            "type": "string",
                            "description": "The title of the form.",
                        },
                        "description": {
                            "type": "string",
                            "description": "The description of the form.",
                        },
                        "labelPlacement": {
                            "type": "string",
                            "description": "The label placement for the form.",
                        },
                        "button": {
                            "type": "object",
                            "description": "The button object for the form.",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "description": "The button type.",
                                },
                                "text": {
                                    "type": "string",
                                    "description": "The button text.",
                                },
                                "imageUrl": {
                                    "type": "string",
                                    "description": "The button image URL.",
                                },
                            },
                        },
                        "fields": {
                            "type": "array",
                            "description": "The fields for the form.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {
                                        "type": "integer",
                                        "description": "The field id.",
                                    },
                                    "label": {
                                        "type": "string",
                                        "description": "The field label.",
                                    },
                                    "type": {
                                        "type": "string",
                                        "description": "The field type.",
                                    },
                                    "isRequired": {
                                        "type": "boolean",
                                        "description": "Indicates whether the field is required.",
                                    },
                                    "size": {
                                        "type": "string",
                                        "description": "The size of the field.",
                                    },
                                    "errorMessage": {
                                        "type": "string",
                                        "description": "The error message for the field.",
                                    },
                                    "visibility": {
                                        "type": "string",
                                        "description": "The visibility of the field.",
                                    },
                                    "inputs": {
                                        "type": "array",
                                        "description": "The inputs for the field.",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {
                                                    "type": "string",
                                                    "description": "The input id.",
                                                },
                                                "label": {
                                                    "type": "string",
                                                    "description": "The input label.",
                                                },
                                                "name": {
                                                    "type": "string",
                                                    "description": "The input name.",
                                                },
                                            },
                                        },
                                    },
                                    "choices": {
                                        "anyOf": [
                                            {"type": "string"},
                                            {
                                                "type": "array",
                                                "description": "The choices for the field.",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "text": {
                                                            "type": "string",
                                                            "description": "The choice text.",
                                                        },
                                                        "value": {
                                                            "type": "string",
                                                            "description": "The choice value.",
                                                        },
                                                        "isSelected": {
                                                            "type": "boolean",
                                                            "description": "Indicates whether the choice is selected.",
                                                        },
                                                    },
                                                },
                                            },
                                        ]
                                    },
                                    "conditionalLogic": {
                                        "type": "object",
                                        "description": "The conditional logic for the field.",
                                        "properties": {
                                            "actionType": {
                                                "type": "string",
                                                "description": "The action type.",
                                            },
                                            "logicType": {
                                                "type": "string",
                                                "description": "The logic type.",
                                            },
                                            "rules": {
                                                "type": "array",
                                                "description": "The rules for the conditional logic.",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "fieldId": {
                                                            "type": "integer",
                                                            "description": "The field id.",
                                                        },
                                                        "operator": {
                                                            "type": "string",
                                                            "description": "The operator.",
                                                        },
                                                        "value": {
                                                            "type": "string",
                                                            "description": "The value.",
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    },
                                },
                                "additionalProperties": True,
                            },
                        },
                    },
                    "additionalProperties": True,
                },
                "FormList": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/Form"},
                },
                "Entry": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string",
                            "description": "The unique identifier of the entry.",
                        },
                        "form_id": {
                            "type": "string",
                            "description": "The identifier of the form associated with the entry.",
                        },
                        "post_id": {
                            "type": "string",
                            "description": "The identifier of the post associated with the entry.",
                        },
                        "date_created": {
                            "type": "string",
                            "format": "date-time",
                            "description": "The date and time when the entry was created.",
                        },
                        "is_starred": {
                            "type": "string",
                            "description": "Indicates whether the entry is starred.",
                        },
                        "is_read": {
                            "type": "string",
                            "description": "Indicates whether the entry is read.",
                        },
                        "ip": {
                            "type": "string",
                            "description": "The IP address from which the entry was submitted.",
                        },
                        "source_url": {
                            "type": "string",
                            "description": "The URL from which the entry was submitted.",
                        },
                        "user_agent": {
                            "type": "string",
                            "description": "The user agent of the browser from which the entry was submitted.",
                        },
                        "currency": {
                            "type": "string",
                            "description": "The currency associated with the entry.",
                        },
                        "payment_status": {
                            "type": "string",
                            "description": "The payment status of the entry.",
                        },
                        "payment_date": {
                            "type": "string",
                            "format": "date-time",
                            "description": "The date and time of the payment associated with the entry.",
                        },
                        "payment_amount": {
                            "type": "string",
                            "description": "The amount of the payment associated with the entry.",
                        },
                        "payment_method": {
                            "type": "string",
                            "description": "The method of payment associated with the entry.",
                        },
                        "transaction_id": {
                            "type": "string",
                            "description": "The transaction ID associated with the entry.",
                        },
                        "is_fulfilled": {
                            "type": "string",
                            "description": "Indicates whether the entry is fulfilled.",
                        },
                        "created_by": {
                            "type": "string",
                            "description": "The user who created the entry.",
                        },
                        "transaction_type": {
                            "type": "string",
                            "description": "The type of transaction associated with the entry.",
                        },
                        "status": {
                            "type": "string",
                            "description": "The status of the entry.",
                        },
                    },
                    "additionalProperties": True,
                },
                "EntryList": {
                    "type": "object",
                    "properties": {
                        "total_count": {"type": "number"},
                        "entries": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/Entry"},
                        },
                    },
                },
                "Results": {
                    "type": "object",
                    "properties": {
                        "entry_count": {"type": "number"},
                        "field_data": {
                            "type": "object",
                            "additionalProperties": True,
                        },
                    },
                },
            },
        },
        "security": [{"basicAuth": []}],
    }
