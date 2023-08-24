from facebook_business.typechecker import TypeChecker
import ast, importlib, inspect, os, pathlib, pkgutil, textwrap


class MetaSDKParser:
    type_mappings = {
        "str": {"type": "string"},
        "string": {"type": "string"},
        "int": {"type": "integer"},
        "unsigned int": {"type": "integer"},
        "float": {"type": "number", "format": "float"},
        "double": {"type": "number", "format": "double"},
        "bool": {"type": "boolean"},
        "datetime": {"type": "string", "format": "date-time"},
        "file": {"type": "string"},
        "map": {"$ref": "#/components/schemas/Generic"},
        "object": {"$ref": "#/components/schemas/Generic"},
        "Object": {"$ref": "#/components/schemas/Generic"},
    }

    method_map = {
        "create": "post",
        "get": "get",
        "update": "put",
        "delete": "delete",
    }

    ignore_functions = [
        func.__name__
        for members in inspect.getmembers(
            importlib.import_module(f"facebook_business.adobjects.abstractcrudobject")
        )
        for member in members
        if inspect.isclass(member) and member.__name__ == "AbstractCrudObject"
        for funcs in inspect.getmembers(member)
        for func in funcs
        if inspect.isfunction(func)
    ]

    @staticmethod
    def get_package_directory():
        loader = pkgutil.get_loader("facebook_business")
        if loader is not None:
            package_directory = os.path.dirname(loader.get_filename())
            return package_directory
        else:
            raise Exception("The facebook_business module is not installed.")

    @staticmethod
    def get_module_names():
        return [
            filename[:-3]
            for filename in os.listdir(
                pathlib.Path(MetaSDKParser.get_package_directory(), "adobjects")
            )
            if filename.endswith(".py")
            and filename.split(os.path.sep)[-1]
            not in ["__init__.py", "abstractobject.py", "abstractcrudobject.py"]
        ]

    @staticmethod
    def get_modules(*names):
        if names:
            modules = []
            for a in names:
                try:
                    modules.append(
                        importlib.import_module(
                            f"facebook_business.adobjects.{a.lower()}"
                        )
                    )
                except:
                    pass
            return modules
        else:
            return [
                importlib.import_module(f"facebook_business.adobjects.{a.lower()}")
                for a in MetaSDKParser.get_module_names()
            ]

    @staticmethod
    def find_refs(data, refs=None):
        """Recursively search for $ref values in data."""
        if refs is None:
            refs = []

        if isinstance(data, dict):
            for key, value in data.items():
                if key == "$ref":
                    refs.append(value)
                else:
                    MetaSDKParser.find_refs(value, refs)
        elif isinstance(data, list):
            for item in data:
                MetaSDKParser.find_refs(item, refs)

        return refs

    # Generate schemas based on the classes
    def set_schemas(self, module):
        for member in inspect.getmembers(module):
            for c in member:
                if (
                    inspect.isclass(c)
                    and c.__name__ not in ["AbstractObject", "AbstractCrudObject"]
                    and hasattr(c, "Field")
                    and hasattr(c, "_field_types")
                ):
                    if c.__name__ in self.spec["components"]["schemas"].keys():
                        continue

                    f = c.Field()
                    # Base Class
                    self.spec["components"]["schemas"][c.__name__] = {
                        "type": "object",
                        "properties": {},
                    }
                    self.spec["components"]["schemas"][c.__name__ + "_" + "List"] = {
                        "type": "object",
                        "allOf": [{"$ref": "#/components/schemas/PaginatedResponse"}],
                        "properties": {
                            "data": {
                                "type": "array",
                                "items": {"$ref": "#/components/schemas/" + c.__name__},
                            }
                        },
                    }
                    # Enumeration Classes
                    for enums in inspect.getmembers(c):
                        for e in enums:
                            if inspect.isclass(e):
                                if e.__name__ not in ["Field", "ABCMeta"]:
                                    self.spec["components"]["schemas"][
                                        c.__name__ + "-" + e.__name__
                                    ] = {
                                        "type": "string",
                                        "enum": [
                                            i
                                            for a in dir(e)
                                            if not a.startswith("_")
                                            if not inspect.isclass(i := getattr(e, a))
                                        ],
                                    }

    # Generate schemas based on class Fields
    def set_field_schemas(self, module):
        schemas = self.spec["components"]["schemas"].keys()
        primary_schemas = [s for s in schemas if "-" not in s]
        for member in inspect.getmembers(module):
            for c in member:
                if inspect.isclass(c) and c.__name__ in primary_schemas:
                    f = c.Field()
                    field_checker = c()._field_checker
                    properties = [attr for attr in dir(f) if not attr.startswith("_")]
                    types = {}
                    for k in properties:
                        p = getattr(f, k)
                        t = c._field_types.get(p, field_checker.get_type(p)) or "string"
                        l = 0
                        while t[:5] == "list<" and t[-1] == ">":
                            l += 1
                            t = t[5:-1]
                        if m := (t[:4] == "map<" and t[-1] == ">"):
                            t = "Object"
                        if not t:
                            t = "string"
                        if t not in field_checker.primitive_types:
                            if t in field_checker._enum_data:
                                t = c.__name__ + "-" + t
                            elif _t := next(
                                (s for s in schemas if t == s),
                                next(
                                    (s for s in schemas if s.endswith(t)),
                                    False,
                                ),
                            ):
                                t = _t

                        if r := self.type_mappings.get(t, False):
                            types[f"{c.__name__}-{k}"] = r
                        elif t in schemas:
                            types[f"{c.__name__}-{k}"] = {
                                "$ref": f"#/components/schemas/{t}"
                            }
                        if f"{c.__name__}-{k}" in types.keys():
                            self.spec["components"]["schemas"][c.__name__][
                                "properties"
                            ][k] = {"$ref": f"#/components/schemas/{c.__name__}-{k}"}
                            for i in range(0, l):
                                types[f"{c.__name__}-{k}"] = {
                                    "type": "array",
                                    "items": types[f"{c.__name__}-{k}"],
                                }

                    self.spec["components"]["schemas"].update(types)

    # Generate paramaters based on classes
    def set_parameters(self, module):
        primary_schemas = [
            s for s in self.spec["components"]["schemas"].keys() if "-" not in s
        ]
        for member in inspect.getmembers(module):
            for c in member:
                if inspect.isclass(c) and c.__name__ in primary_schemas:
                    if [
                        m.__name__[4:]
                        for _member in inspect.getmembers(c)
                        for m in _member
                        if inspect.isfunction(m)
                    ]:
                        properties = self.spec["components"]["schemas"][c.__name__][
                            "properties"
                        ].keys()
                        for p in properties:
                            _p = c.__name__ + "-" + p
                            if _p in self.spec["components"]["schemas"].keys():
                                self.spec["components"]["parameters"][_p] = {
                                    "in": "path" if p == "id" else "query",
                                    "name": _p if p == "id" else p,
                                    **(
                                        {}
                                        if p == "id"
                                        else {
                                            "style": "form",
                                            "explode": False,
                                        }
                                    ),
                                    "schema": {"$ref": f"#/components/schemas/{_p}"},
                                    **({"required": True} if p == "id" else {}),
                                }
                        self.spec["components"]["parameters"][
                            c.__name__ + "_" + "Fields"
                        ] = {
                            "in": "query",
                            "name": "fields",
                            "style": "form",
                            "explode": False,
                            "schema": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "enum": [
                                        p
                                        for p in properties
                                        if "{}{}{}".format(c.__name__, "-", p)
                                        in self.spec["components"]["schemas"].keys()
                                    ],
                                },
                            },
                        }

    # Generate responses based on classes
    def set_responses(self, module):
        primary_schemas = [
            s for s in self.spec["components"]["schemas"].keys() if "-" not in s
        ]
        for members in inspect.getmembers(module):
            for c in members:
                if inspect.isclass(c) and c.__name__ in primary_schemas:
                    for op in ["create", "get", "update", "delete"]:
                        self.spec["components"]["responses"][c.__name__ + "_" + op] = {
                            "description": f"Results of a request related to {c.__name__} records.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/" + c.__name__
                                    }
                                }
                            },
                            "headers": {
                                "X-Business-Use-Case-Usage": {
                                    "$ref": "#/components/headers/X-Business-Use-Case-Usage"
                                }
                            },
                        }
                        self.spec["components"]["responses"][
                            c.__name__ + "_" + op + "_List"
                        ] = {
                            "description": f"Results of a request related to {c.__name__} records.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/{}{}{}".format(
                                            c.__name__, "_", "List"
                                        )
                                    },
                                }
                            },
                            "headers": {
                                "X-Business-Use-Case-Usage": {
                                    "$ref": "#/components/headers/X-Business-Use-Case-Usage"
                                }
                            },
                        }

    # Generate responses based on classes
    def set_node_operations(self, module):
        primary_schemas = [
            s for s in self.spec["components"]["schemas"].keys() if "-" not in s
        ]
        for member in inspect.getmembers(module):
            for c in member:
                if inspect.isclass(c) and c.__name__ in primary_schemas:
                    path = f"/{{{c.__name__}-id}}"
                    for op in [
                        m.__name__[4:]
                        for _member in inspect.getmembers(c)
                        for m in _member
                        if inspect.isfunction(m)
                        and m.__name__.startswith("api_")
                        and m.__name__ != "api_create"
                    ]:
                        if path not in self.spec["paths"]:
                            self.spec["paths"][path] = {
                                "parameters": [
                                    {
                                        "$ref": f"#/components/parameters/{c.__name__}-id"
                                    },
                                    {
                                        "$ref": "#/components/parameters/{}{}{}".format(
                                            c.__name__, "_", "Fields"
                                        )
                                    },
                                ]
                            }
                        self.spec["paths"][path][self.method_map[op]] = {
                            "operationId": "{}{}".format(op.capitalize(), c.__name__),
                            **(
                                {
                                    "parameters": [
                                        {"$ref": f"#/components/parameters/{p}"}
                                        for p in self.spec["components"][
                                            "parameters"
                                        ].keys()
                                        if p.startswith(f"{c.__name__}-")
                                    ]
                                }
                                if op != "delete"
                                else {}
                            ),
                            "responses": {
                                "200": {
                                    "$ref": "#/components/responses/{}{}{}".format(
                                        c.__name__, "_", op
                                    )
                                },
                                "default": {"$ref": "#/components/responses/Generic"},
                            },
                            "security": [{"access_token": []}],
                            "tags": [c.__name__],
                        }

    # Generate responses based on classes
    def set_edge_operations(self, module):
        schemas = self.spec["components"]["schemas"].keys()
        primary_schemas = [s for s in schemas if "-" not in s]
        operations = {
            node.__name__: [
                {"func": member}
                for members in inspect.getmembers(node)
                for member in members
                if inspect.isfunction(member)
                and member.__name__ not in self.ignore_functions
                and (parts := member.__name__.split("_"))[0]
                in ["create", "get", "delete"]
            ]
            for members in inspect.getmembers(module)
            for node in members
            if inspect.isclass(node) and node.__name__ in primary_schemas
        }
        for node in operations.values():
            for member in node:
                parser = RequestVisitor()
                parser.visit(
                    ast.parse(textwrap.dedent(inspect.getsource(member["func"])))
                )
                if parser.data["request"].get("target_class", False):
                    member.update(parser.data)

        for name, node in operations.items():
            for op in node:
                if (
                    op.get("request", False)
                    and op["request"].get("target_class", False)
                    and op["request"]["target_class"] in primary_schemas
                ):
                    path = f"/{{{name}-id}}{op['request']['endpoint']}"
                    if path not in self.spec["paths"]:
                        self.spec["paths"][path] = {
                            "parameters": [
                                {"$ref": f"#/components/parameters/{name}-id"},
                                {
                                    "$ref": "#/components/parameters/{}{}{}".format(
                                        op["request"]["target_class"], "_", "Fields"
                                    )
                                },
                                {"$ref": f"#/components/parameters/Filtering"},
                            ]
                        }
                    field_checker = TypeChecker(op["param_types"], op["enums"])
                    parameters = {}
                    for k in op["param_types"].keys():
                        t = field_checker.get_type(k) or "string"
                        l = 0
                        while t[:5] == "list<" and t[-1] == ">":
                            l += 1
                            t = t[5:-1]
                        if m := (t[:4] == "map<" and t[-1] == ">"):
                            t = "Object"
                        if not t:
                            t = "string"
                        if t not in field_checker.primitive_types:
                            if t in field_checker._enum_data:
                                t = field_checker._enum_data[t]
                            elif _t := next(
                                (s for s in schemas if t == s),
                                next(
                                    (s for s in schemas if t in s),
                                    False,
                                ),
                            ):
                                t = _t

                        if r := self.type_mappings.get(t, False):
                            parameters[k] = r
                        elif t in schemas:
                            parameters[k] = {"$ref": f"#/components/schemas/{t}"}
                        elif "." in t:
                            parameters[k] = {
                                "$ref": f"#/components/schemas/{t.replace('.','-')}"
                            }

                        if k in parameters.keys():
                            for _ in range(0, l):
                                parameters[k] = {
                                    "type": "array",
                                    "items": parameters[k],
                                }

                    self.spec["paths"][path][op["request"]["method"].lower()] = {
                        "operationId": "{}{}{}".format(
                            name,
                            "_",
                            op["func"]
                            .__name__.replace("_", " ")
                            .title()
                            .replace(" ", ""),
                        ),
                        "parameters": [
                            {
                                "in": "query",
                                "name": k,
                                "style": "form",
                                "explode": False,
                                "schema": v,
                            }
                            for k, v in parameters.items()
                            if k != "fields"
                        ]
                        + (
                            [
                                {"$ref": "#/components/parameters/Limit"},
                                {"$ref": "#/components/parameters/After"},
                                {"$ref": "#/components/parameters/Before"},
                            ]
                            if op["request"]["method"].lower() == "get"
                            else []
                        ),
                        "responses": {
                            "200": {
                                "$ref": "#/components/responses/{}{}{}_List".format(
                                    op["request"]["target_class"],
                                    "_",
                                    op["func"].__name__.split("_")[0],
                                )
                            },
                            "default": {"$ref": "#/components/responses/Generic"},
                        },
                        "security": [{"access_token": []}],
                        "tags": list(set([name, op["request"]["target_class"]])),
                    }

    def set_tags(self):
        tags = list(
            set(
                [
                    tag
                    for path in self.spec["paths"].values()
                    for op in path.values()
                    if isinstance(op, dict) and "tags" in op.keys()
                    for tag in op["tags"]
                ]
            )
        )
        tags.sort()
        self.spec["tags"] = [{"name": tag} for tag in tags]

    @staticmethod
    def get_by_tags(source, *tags):
        spec = {
            "openapi": source["openapi"],
            "info": source["info"],
            "servers": source["servers"],
            "tags": [],
        }
        if not tags:
            tags = [t["name"] for t in source["tags"]]
        for tag in tags:
            for path_key, path_value in source["paths"].items():
                if tag in [
                    t
                    for op in path_value.values()
                    if isinstance(op, dict) and "tags" in op.keys()
                    for t in op["tags"]
                ]:
                    for op_key, op_value in path_value.items():
                        if "paths" not in spec.keys():
                            spec["paths"] = {}
                        if path_key not in spec["paths"].keys():
                            spec["paths"][path_key] = {}
                        spec["paths"][path_key][op_key] = source["paths"][path_key][
                            op_key
                        ]
                        if isinstance(op_value, dict):
                            spec["tags"] += op_value["tags"]

                            if "security" in op_value.keys():
                                for security in op_value["security"]:
                                    for security_key in security.keys():
                                        if (
                                            security_key
                                            in source["components"][
                                                "securitySchemes"
                                            ].keys()
                                        ):
                                            if "components" not in spec.keys():
                                                spec["components"] = {}
                                            if (
                                                "securitySchemes"
                                                not in spec["components"]
                                            ):
                                                spec["components"][
                                                    "securitySchemes"
                                                ] = {}
                                            spec["components"]["securitySchemes"][
                                                security_key
                                            ] = source["components"]["securitySchemes"][
                                                security_key
                                            ]

        for ref in MetaSDKParser.find_refs(spec["paths"]):
            src = source
            dest = spec
            parts = ref[2:].split("/")
            for t in parts:
                src = src[t]
                if t not in dest.keys():
                    dest[t] = {}
                dest = dest[t]
            dest.update(src)

        for ref in MetaSDKParser.find_refs(spec["components"]["parameters"]):
            src = source
            dest = spec
            parts = ref[2:].split("/")
            for t in parts:
                src = src[t]
                if t not in dest.keys():
                    dest[t] = {}
                dest = dest[t]
            dest.update(src)

        for ref in MetaSDKParser.find_refs(spec["components"]["responses"]):
            src = source
            dest = spec
            parts = ref[2:].split("/")
            for t in parts:
                src = src[t]
                if t not in dest.keys():
                    dest[t] = {}
                dest = dest[t]
            dest.update(src)

        while refs := [
            r
            for r in MetaSDKParser.find_refs(spec["components"]["schemas"])
            if r[2:].split("/")[-1] not in spec["components"]["schemas"].keys()
        ]:
            for ref in refs:
                src = source
                dest = spec
                parts = ref[2:].split("/")
                for t in parts:
                    src = src[t]
                    if t not in dest.keys():
                        dest[t] = {}
                    dest = dest[t]
                dest.update(src)

        tags = list(set(spec["tags"]))
        tags.sort()
        spec["tags"] = [{"name": tag} for tag in tags]
        return spec

    def __init__(self, *names):
        self.spec = {
            "openapi": "3.1.0",
            "info": {
                "title": "Facebook/Meta Business API",
                "version": "v17.0",
                "description": "The Facebook/Meta Business API is an upgraded version of the Marketing API that includes the Marketing API as well as many Facebook APIs from different platforms such as Pages, Business Manager, Instagram, etc.",
                "contact": {
                    "email": "isaac@esqads.com",
                    "name": "Isaac Jesup",
                },
            },
            "servers": [{"url": "https://graph.facebook.com/v17.0"}],
            "components": {
                "securitySchemes": {
                    "access_token": {
                        "type": "apiKey",
                        "in": "query",
                        "name": "access_token",
                    }
                },
                "headers": {
                    "X-Business-Use-Case-Usage": {
                        "description": "Contains usage and throttling data.",
                        "schema": {"$ref": "#/components/schemas/BusinessUseCase"},
                    },
                },
                "parameters": {
                    "Limit": {
                        "in": "query",
                        "name": "limit",
                        "schema": {"type": "integer"},
                    },
                    "After": {
                        "in": "query",
                        "name": "after",
                        "schema": {"type": "string"},
                    },
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
                "responses": {
                    "Generic": {
                        "description": f"Results of a request who's schema is not implemented yet.",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "additionalProperties": True,
                                }
                            }
                        },
                        "headers": {
                            "X-Business-Use-Case-Usage": {
                                "$ref": "#/components/headers/X-Business-Use-Case-Usage"
                            }
                        },
                    },
                },
                "schemas": {
                    "BusinessUseCase": {
                        "type": "string"
                        # "type": "object",
                        # "additionalProperties": {
                        #     "type": "array",
                        #     "items": {
                        #         "type": "object",
                        #         "properties": {
                        #             "type": {"type": "string"},
                        #             "call_count": {"type": "integer"},
                        #             "total_cpu_time": {"type": "integer"},
                        #             "total_time": {"type": "integer"},
                        #             "estimated_time_to_regain_access": {
                        #                 "type": "integer"
                        #             },
                        #         },
                        #     },
                        # },
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
                    "Generic": {"additionalProperties": True},
                    "PaginatedResponse": {
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
            },
            "paths": {
                "/": {
                    "post": {
                        "operationId": "Batch",
                        "requestBody": {
                            "required": True,
                            "content": {
                                "application/x-www-form-urlencoded": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "batch": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "method": {
                                                            "type": "string",
                                                            "enum": [
                                                                "POST",
                                                                "GET",
                                                                "PUT",
                                                                "DELETE",
                                                            ],
                                                        },
                                                        "relative_url": {
                                                            "type": "string"
                                                        },
                                                    },
                                                    "additionalProperties": True,
                                                },
                                            }
                                        },
                                    }
                                }
                            },
                        },
                    }
                }
            },
            "tags": [],
        }
        for module in MetaSDKParser.get_modules(*names):
            self.set_schemas(module)
            self.set_field_schemas(module)
            self.set_parameters(module)
            self.set_responses(module)
            self.set_node_operations(module)
        for module in MetaSDKParser.get_modules(*names):
            self.set_edge_operations(module)
        self.set_tags()
        # self.spec = self.get_by_tags(self.spec)


class RequestVisitor(ast.NodeVisitor):
    def __init__(self):
        self.data = {}

    def visit_FunctionDef(self, node):
        param_types = {}
        enums = {}
        request = {}
        for n in node.body:
            match n:
                case ast.Assign():
                    match next(iter([t.id for t in n.targets]), None):
                        case "param_types":
                            param_types = dict(
                                zip(
                                    [k.value for k in n.value.keys],
                                    [k.value for k in n.value.values],
                                )
                            )
                        case "enums":
                            enums = dict(
                                zip(
                                    [k.value for k in n.value.keys],
                                    [
                                        [i.value for i in v.elts]
                                        if isinstance(v, ast.List)
                                        else v.func.value.value.value.id
                                        + "."
                                        + v.func.value.value.attr
                                        for v in n.value.values
                                    ],
                                )
                            )
                        case "request":
                            match n.value.func.id:
                                case "FacebookRequest":
                                    for k in n.value.keywords:
                                        match k.value:
                                            case ast.Constant():
                                                request[k.arg] = k.value.value
                                            case ast.Name():
                                                request[k.arg] = k.value.id
        self.data = {
            "param_types": param_types,
            "enums": enums,
            "request": request,
        }
