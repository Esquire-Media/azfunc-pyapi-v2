from aiopenapi3 import OpenAPI
from aiopenapi3.plugin import Init
import httpx
import httpx_auth
import os


class OnSpotInitPlugin(Init):
    """
    A class used to initialize the OnSpot API.

    Attributes
    ----------
    production : bool
        A flag indicating whether the API is in production mode.

    Methods
    -------
    initialized(ctx)
        Runs when the OpenAPI object is initialized.
    """

    def __init__(self, production=False):
        """
        Constructs all the necessary attributes for the OnSpotInitPlugin object.

        Parameters
        ----------
        production : bool, optional
            A flag indicating whether the API is in production mode (default is False).
        """
        super().__init__()
        self.production = production

    def initialized(self, ctx: "Init.Context") -> "Init.Context":  # pragma: no cover
        """
        Updates the server list and security schemes in the given context.

        Parameters
        ----------
        ctx : Init.Context
            The context to be initialized.

        Returns
        -------
        Init.Context
            The initialized context.
        """
        ctx.initialized.servers = [
            server
            for server in ctx.initialized.servers
            if ("Production" if self.production else "QA") in server.description
        ]
        if sigv4 := ctx.initialized.components.securitySchemes.get("sigv4", None):
            ctx.initialized.components.securitySchemes["sigv4"] = type(sigv4)(
                type="http", scheme="aws4auth"
            )


class OnSpotAPI:
    """
    A class used to interact with the OnSpot API.

    This class is a wrapper around the OpenAPI object, providing a convenient
    way to interact with the OnSpot API.

    Methods
    -------
    __new__(access_key, secret_key, api_key, region, production)
        Constructs and returns an OpenAPI object for interacting with the
        OnSpot API.

    Notes
    -----
    The `__new__` method is used instead of `__init__` because we want to
    return an instance of OpenAPI, not an instance of OnSpotAPI.

    Examples
    --------
    >>> api = OnSpotAPI()
    >>> session_factory = api.createRequest(("/geoframe/all/count", "post"))
    OR
    >>> session_factory = api.createRequest("Device Counts.geoframeAllCount")
    Both of these session_factory variables use the same endpoint. The first
    instantiates the factory by using a (path, method) tuple. The second uses
    a string in the following format Tag.OperationID.
    >>> await session_factory(
    >>>     data={
    >>>         "type": "FeatureCollection",
    >>>         "features": [
    >>>             {
    >>>                 "type": "Feature",
    >>>                 "geometry": {
    >>>                     "type": "Polygon",
    >>>                     "coordinates": [
    >>>                         [
    >>>                             [-123.46478462219238, 39.07166187346857],
    >>>                             [-123.4659218788147, 39.06921298141872],
    >>>                             [-123.46278905868529, 39.0687964947246],
    >>>                             [-123.4616732597351, 39.07079560844253],
    >>>                             [-123.46478462219238, 39.07166187346857],
    >>>                         ]
    >>>                     ],
    >>>                 },
    >>>                 "properties": {
    >>>                     "name": "Feature Example",
    >>>                     "start": "2022-06-08T00:00:00",
    >>>                     "end": "2022-06-19T23:59:59",
    >>>                     "callback": "https://iupd9.localto.net/api/logger",
    >>>                 },
    >>>             }
    >>>         ],
    >>>     }
    >>> )
    """

    def __new__(
        cls,
        access_key: str = os.environ.get("ONSPOT_ACCESS_KEY"),
        secret_key: str = os.environ.get("ONSPOT_SECRET_KEY"),
        api_key: str = os.environ.get("ONSPOT_API_KEY"),
        region: str = os.environ.get("ONSPOT_REGION", "us-east-1"),
        production: bool = False,
    ) -> OpenAPI:
        """
        Constructs and returns an OpenAPI object for interacting with the OnSpot API.

        The access key, secret key, API key, and region are obtained from the environment variables
        ONSPOT_ACCESS_KEY, ONSPOT_SECRET_KEY, ONSPOT_API_KEY, and ONSPOT_REGION, respectively. If
        ONSPOT_REGION is not set, "us-east-1" is used as the default region. The production flag is
        False by default.

        Parameters
        ----------
        access_key : str, optional
            The access key for the API.
        secret_key : str, optional
            The secret key for the API.
        api_key : str, optional
            The API key for the API.
        region : str, optional
            The region for the API.
        production : bool, optional
            A flag indicating whether the API is in production mode.

        Returns
        -------
        OpenAPI
            An OpenAPI object for interacting with the OnSpot API.

        Notes
        -----
        The OpenAPI object returned by this method is authenticated and ready
        to use for making API requests.

        The OpenAPI specification for OnSpot is only available through their API.
        Therefore, in order to bootstrap the OpenAPI client for OnSpot, this method
        first sends an authenticated GET request to the OnSpot API. The JSON response
        from this request is then used as the document parameter to initialize the
        OpenAPI object.
        """
        spec_url = f'https://api.{"" if production else "qa."}onspotdata.com/openapi'
        auth = httpx_auth.HeaderApiKey(
            api_key=api_key, header_name="x-api-key"
        ) + httpx_auth.AWS4Auth(
            access_id=access_key,
            secret_key=secret_key,
            region=region,
            service="execute-api",
        )
        api = OpenAPI(
            url="https://osd-api-docs.s3.amazonaws.com/openapi.json",
            document=OnSpotAPI.spec,
            plugins=[OnSpotInitPlugin(production=production)],
        )
        api.authenticate(
            apiKey=api_key,
            sigv4={
                "access_id": access_key,
                "secret_key": secret_key,
                "service": "execute-api",
                "region": region,
            },
        )
        return api

    spec = {
        "components": {
            "examples": {
                "Address": {"value": ""},
                "Attribution": {
                    "value": {
                        "name": "My Attribution Request",
                        "organization": "onspot",
                        "targetingFeatureCollection": {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "MultiPolygon",
                                        "coordinates": [
                                            [
                                                [
                                                    [
                                                        -123.46478462219238,
                                                        39.07166187346857,
                                                    ],
                                                    [
                                                        -123.4659218788147,
                                                        39.06921298141872,
                                                    ],
                                                    [
                                                        -123.46278905868529,
                                                        39.0687964947246,
                                                    ],
                                                    [
                                                        -123.4616732597351,
                                                        39.07079560844253,
                                                    ],
                                                    [
                                                        -123.46478462219238,
                                                        39.07166187346857,
                                                    ],
                                                ]
                                            ]
                                        ],
                                    },
                                    "properties": {
                                        "name": "Feature Example",
                                        "start": "2020-05-08T00:00:00",
                                        "end": "2020-06-19T23:59:59",
                                        "callback": "https://yourapi.com/callback",
                                    },
                                }
                            ],
                        },
                        "attributionFeatureCollection": {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "MultiPolygon",
                                        "coordinates": [
                                            [
                                                [
                                                    [
                                                        -123.46478462219238,
                                                        39.07166187346857,
                                                    ],
                                                    [
                                                        -123.4659218788147,
                                                        39.06921298141872,
                                                    ],
                                                    [
                                                        -123.46278905868529,
                                                        39.0687964947246,
                                                    ],
                                                    [
                                                        -123.4616732597351,
                                                        39.07079560844253,
                                                    ],
                                                    [
                                                        -123.46478462219238,
                                                        39.07166187346857,
                                                    ],
                                                ]
                                            ]
                                        ],
                                    },
                                    "properties": {
                                        "name": "Feature Example",
                                        "start": "2020-05-08T00:00:00",
                                        "end": "2020-06-19T23:59:59",
                                        "callback": "https://yourapi.com/callback",
                                    },
                                }
                            ],
                        },
                        "callback": "https://myserver.com/sink",
                    }
                },
                "Files": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Files",
                                "paths": ["s3://mybucket/my/file.csv"],
                                "properties": {
                                    "name": "Feature Example",
                                    "callback": "https://yourapi.com/callback",
                                },
                            }
                        ],
                    }
                },
                "MultiPolygon": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": [
                                        [
                                            [
                                                [
                                                    -123.46478462219238,
                                                    39.07166187346857,
                                                ],
                                                [-123.4659218788147, 39.06921298141872],
                                                [-123.46278905868529, 39.0687964947246],
                                                [-123.4616732597351, 39.07079560844253],
                                                [
                                                    -123.46478462219238,
                                                    39.07166187346857,
                                                ],
                                            ]
                                        ]
                                    ],
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                },
                            }
                        ],
                    }
                },
                "MultiPolygonGroupedByInterval": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": [
                                        [
                                            [
                                                [
                                                    -123.46478462219238,
                                                    39.07166187346857,
                                                ],
                                                [-123.4659218788147, 39.06921298141872],
                                                [-123.46278905868529, 39.0687964947246],
                                                [-123.4616732597351, 39.07079560844253],
                                                [
                                                    -123.46478462219238,
                                                    39.07166187346857,
                                                ],
                                            ]
                                        ]
                                    ],
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                    "interval": 4,
                                },
                            }
                        ],
                    }
                },
                "MultiPolygonDemographics": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": [
                                        [
                                            [
                                                [
                                                    -123.46478462219238,
                                                    39.07166187346857,
                                                ],
                                                [-123.4659218788147, 39.06921298141872],
                                                [-123.46278905868529, 39.0687964947246],
                                                [-123.4616732597351, 39.07079560844253],
                                                [
                                                    -123.46478462219238,
                                                    39.07166187346857,
                                                ],
                                            ]
                                        ]
                                    ],
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                    "demographics": ["estimated_age"],
                                },
                            }
                        ],
                    }
                },
                "PointAndRadius": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "PointAndRadius",
                                    "point": {"lat": 37.7685474, "lng": -122.4779697},
                                    "radius": 25.0,
                                    "points": 16,
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                },
                            }
                        ],
                    }
                },
                "PointAndRadiusDemographics": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "PointAndRadius",
                                    "point": {"lat": 37.7685474, "lng": -122.4779697},
                                    "radius": 25.0,
                                    "points": 16,
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                    "demographics": ["estimated_age"],
                                },
                            }
                        ],
                    }
                },
                "PointAndRadiusGroupedByInterval": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "PointAndRadius",
                                    "point": {"lat": 37.7685474, "lng": -122.4779697},
                                    "radius": 25.0,
                                    "points": 16,
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                    "interval": 4,
                                },
                            }
                        ],
                    }
                },
                "Polygon": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-123.46478462219238, 39.07166187346857],
                                            [-123.4659218788147, 39.06921298141872],
                                            [-123.46278905868529, 39.0687964947246],
                                            [-123.4616732597351, 39.07079560844253],
                                            [-123.46478462219238, 39.07166187346857],
                                        ]
                                    ],
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                },
                            }
                        ],
                    }
                },
                "PolygonDemographics": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-123.46478462219238, 39.07166187346857],
                                            [-123.4659218788147, 39.06921298141872],
                                            [-123.46278905868529, 39.0687964947246],
                                            [-123.4616732597351, 39.07079560844253],
                                            [-123.46478462219238, 39.07166187346857],
                                        ]
                                    ],
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                    "demographics": ["estimated_age"],
                                },
                            }
                        ],
                    }
                },
                "PolygonGroupedByInterval": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-123.46478462219238, 39.07166187346857],
                                            [-123.4659218788147, 39.06921298141872],
                                            [-123.46278905868529, 39.0687964947246],
                                            [-123.4616732597351, 39.07079560844253],
                                            [-123.46478462219238, 39.07166187346857],
                                        ]
                                    ],
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "start": "2020-05-08T00:00:00",
                                    "end": "2020-06-19T23:59:59",
                                    "callback": "https://yourapi.com/callback",
                                    "interval": 4,
                                },
                            }
                        ],
                    }
                },
                "TradeArea": {
                    "value": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": [
                                        [
                                            [
                                                [
                                                    -123.46478462219238,
                                                    39.07166187346857,
                                                ],
                                                [-123.4659218788147, 39.06921298141872],
                                                [-123.46278905868529, 39.0687964947246],
                                                [-123.4616732597351, 39.07079560844253],
                                                [
                                                    -123.46478462219238,
                                                    39.07166187346857,
                                                ],
                                            ]
                                        ]
                                    ],
                                },
                                "properties": {
                                    "name": "Feature Example",
                                    "callback": "https://yourapi.com/callback",
                                    "radius": 10,
                                    "includeHouseholds": True,
                                    "includeWorkplaces": True,
                                },
                            }
                        ],
                    }
                },
            },
            "requestBodies": {
                "Address": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Address"}
                        }
                    },
                    "description": "Address Request object",
                    "required": True,
                },
                "AttributionBase": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/Attribution"
                                }
                            },
                            "schema": {"$ref": "#/components/schemas/AttributionBase"},
                        }
                    },
                    "description": "Attribution object",
                    "required": True,
                },
                "AttributionWithSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/Attribution"
                                }
                            },
                            "schema": {
                                "$ref": "#/components/schemas/AttributionWithSave"
                            },
                        }
                    },
                    "description": "AttributionWithSave request",
                    "required": True,
                },
                "DevicesSegment": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/DevicesSegment"}
                        }
                    },
                    "description": "DevicesSegment request",
                    "required": True,
                },
                "FilesBase": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "Files": {"$ref": "#/components/examples/Files"}
                            },
                            "schema": {"$ref": "#/components/schemas/FilesBase"},
                        }
                    },
                    "description": "GeoJson with file feature type",
                    "required": True,
                },
                "FilesSocialNetwork": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "Files": {"$ref": "#/components/examples/Files"}
                            },
                            "schema": {
                                "$ref": "#/components/schemas/FilesSocialNetwork"
                            },
                        }
                    },
                    "description": "GeoJson with file feature type",
                    "required": True,
                },
                "FilesPoliticalSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "Files": {"$ref": "#/components/examples/Files"}
                            },
                            "schema": {
                                "$ref": "#/components/schemas/FilesPoliticalSave"
                            },
                        }
                    },
                    "description": "GeoJson with file feature type",
                    "required": True,
                },
                "FilesHouseholdAddressesSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "Files": {"$ref": "#/components/examples/Files"}
                            },
                            "schema": {
                                "$ref": "#/components/schemas/FilesHouseholdAddressesSave"
                            },
                        }
                    },
                    "description": "GeoJson with file feature type",
                    "required": True,
                },
                "FilesWithMinMaxDevicesSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "Files": {"$ref": "#/components/examples/Files"}
                            },
                            "schema": {
                                "$ref": "#/components/schemas/FilesWithMinMaxDevicesSave"
                            },
                        }
                    },
                    "description": "GeoJson with file feature type",
                    "required": True,
                },
                "GeoJsonBase": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {"$ref": "#/components/schemas/GeoJsonBase"},
                        }
                    },
                    "description": "GeoJson object",
                    "required": True,
                },
                "GeoJsonSegment": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {"$ref": "#/components/schemas/GeoJsonSegment"},
                        }
                    },
                    "description": "GeoJsonSegment object",
                    "required": True,
                },
                "GeoJsonDemographics": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygonDemographics"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadiusDemographics"
                                },
                                "Polygon": {
                                    "$ref": "#/components/examples/PolygonDemographics"
                                },
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonDemographics"
                            },
                        }
                    },
                    "description": "GeoJson Demographics Object",
                    "required": True,
                },
                "GeoJsonGroupedByDay": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonGroupedByDay"
                            },
                        }
                    },
                    "description": "GeoJsonGroupedByDay object",
                    "required": True,
                },
                "GeoJsonGroupedByInterval": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygonGroupedByInterval"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadiusGroupedByInterval"
                                },
                                "Polygon": {
                                    "$ref": "#/components/examples/PolygonGroupedByInterval"
                                },
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonGroupedByInterval"
                            },
                        }
                    },
                    "description": "GeoJsonGroupedByInterval object",
                    "required": True,
                },
                "GeoJsonPoliticalWithSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygonGroupedByInterval"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadiusGroupedByInterval"
                                },
                                "Polygon": {
                                    "$ref": "#/components/examples/PolygonGroupedByInterval"
                                },
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonPoliticalWithSave"
                            },
                        }
                    },
                    "description": "GeoJsonPoliticalWithSave object",
                    "required": True,
                },
                "GeoJsonPoliticalAggregate": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygonGroupedByInterval"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadiusGroupedByInterval"
                                },
                                "Polygon": {
                                    "$ref": "#/components/examples/PolygonGroupedByInterval"
                                },
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonPoliticalAggregate"
                            },
                        }
                    },
                    "description": "GeoJsonPoliticalAggregate object",
                    "required": True,
                },
                "GeoJsonSocialNetwork": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonSocialNetwork"
                            },
                        }
                    }
                },
                "GeoJsonWithMinMax": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonWithMinMax"
                            },
                        }
                    },
                    "description": "GeoJsonWithMinMax object",
                    "required": True,
                },
                "GeoJsonWithMinMaxDevices": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonWithMinMaxDevices"
                            },
                        }
                    },
                    "description": "GeoJsonWithMinMaxDevices object",
                    "required": True,
                },
                "GeoJsonWithMinMaxDevicesSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonWithMinMaxDevicesSave"
                            },
                        }
                    },
                    "description": "GeoJsonWithMinMaxDevicesSave object",
                    "required": True,
                },
                "GeoJsonWithDevices": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonWithDevices"
                            },
                        }
                    },
                    "description": "GeoJsonWithDevices object",
                    "required": True,
                },
                "GeoJsonWithDevicesSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonWithDevicesSave"
                            },
                        }
                    },
                    "description": "GeoJsonWithDevicesSave object",
                    "required": True,
                },
                "GeoJsonWithHeadersSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonWithHeadersSave"
                            },
                        }
                    },
                    "description": "GeoJsonWithHeadersSave object",
                    "required": True,
                },
                "GeoJsonWithWorkplaceHeadersSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonWithWorkplaceHeadersSave"
                            },
                        }
                    },
                    "description": "GeoJsonWithWorkplaceHeadersSave object",
                    "required": True,
                },
                "GeoJsonWithObservationsSave": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/MultiPolygon"
                                },
                                "PointAndRadius": {
                                    "$ref": "#/components/examples/PointAndRadius"
                                },
                                "Polygon": {"$ref": "#/components/examples/Polygon"},
                            },
                            "schema": {
                                "$ref": "#/components/schemas/GeoJsonWithObservationsSave"
                            },
                        }
                    },
                    "description": "GeoJsonWithObservationsSave object",
                    "required": True,
                },
                "TradeArea": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "MultiPolygon": {
                                    "$ref": "#/components/examples/TradeArea"
                                }
                            },
                            "schema": {"$ref": "#/components/schemas/GeoJsonTradeArea"},
                        }
                    },
                    "description": "trade area request body",
                    "required": True,
                },
            },
            "responses": {
                "200-Callback-Response": {
                    "description": "Callback successfully processed"
                },
                "200-ServerResponse-Async": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {"$ref": "#/components/schemas/Async"},
                            }
                        }
                    },
                    "description": "Successful operation",
                },
                "200-ServerResponse-AsyncSave": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "allOf": [
                                    {"$ref": "#/components/schemas/Async"},
                                    {
                                        "type": "object",
                                        "properties": {
                                            "location": {
                                                "type": "string",
                                                "example": "s3://mybucket/myorganization/Feature Example.csv",
                                                "description": "Location File will be uploaded to",
                                            }
                                        },
                                    },
                                ]
                            }
                        }
                    },
                    "description": "Successful operation",
                },
                "200-ServerResponse-AsyncPublish": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "allOf": [
                                    {"$ref": "#/components/schemas/Async"},
                                    {
                                        "type": "object",
                                        "properties": {
                                            "location": {
                                                "type": "string",
                                                "example": "thetradedesk",
                                                "description": "Partner segment will be sent to",
                                            }
                                        },
                                    },
                                ]
                            }
                        }
                    },
                    "description": "Successful operation",
                },
                "400": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Error"}
                        }
                    },
                    "description": "Invalid format or values supplied in request",
                },
                "403": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Error"}
                        }
                    },
                    "description": "Incorrect or expired authorization signature",
                },
                "415": {"description": "Invalid content-type header"},
                "500": {"description": "Internal server error"},
            },
            "schemas": {
                "Address": {
                    "allOf": [
                        {
                            "type": "object",
                            "required": ["name", "sources", "mappings", "callback"],
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "example": "MyRequest",
                                    "description": "Name for the request",
                                },
                                "sources": {
                                    "type": "array",
                                    "example": ["s3://onspot-app-dev/addresslist.csv"],
                                    "description": "Array of s3 paths to read address csvs from",
                                    "items": {"type": "string"},
                                },
                                "mappings": {
                                    "$ref": "#/components/schemas/AddressMappings"
                                },
                                "callback": {
                                    "type": "string",
                                    "example": "https://yourapi.com/callback",
                                    "description": "Callback, will be triggered when the async process is done",
                                },
                                "matchAcceptanceThreshold": {
                                    "type": "number",
                                    "example": 29.9,
                                    "default": 29.9,
                                    "description": "Match Acceptance Threshold, only include matches that meet an ElasticSearch acceptance threshold",
                                },
                            },
                        },
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                    ]
                },
                "AddressMappings": {
                    "description": "Mappings object that will match headers to normalized column names",
                    "type": "object",
                    "properties": {
                        "street": {
                            "type": "array",
                            "example": '["address"]',
                            "description": "street = [Name of column in csv]",
                            "items": {"type": "string"},
                        },
                        "city": {
                            "type": "array",
                            "example": '["mCity"]',
                            "description": "city = [Name of column in csv]",
                            "items": {"type": "string"},
                        },
                        "state": {
                            "type": "array",
                            "example": '["mState"]',
                            "description": "state = [Name of column in csv]",
                            "items": {"type": "string"},
                        },
                        "zip": {
                            "type": "array",
                            "example": '["zipcode"]',
                            "description": "zip = [Name of column in csv]",
                            "items": {"type": "string"},
                        },
                        "zip4": {
                            "type": "array",
                            "example": '["zip-4"]',
                            "description": "zip4 = [Name of column in csv",
                            "items": {"type": "string"},
                        },
                    },
                },
                "Async": {
                    "properties": {
                        "id": {
                            "description": "Unique id used to track the async request",
                            "example": "123e4567-e89b-12d3-a456-426655440000",
                            "type": "string",
                        },
                        "name": {
                            "description": "Name of the feature that this result was generated from",
                            "example": "Feature Example",
                            "type": "string",
                        },
                    },
                    "required": ["id", "name"],
                    "type": "object",
                },
                "AttributionBase": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "example": "My Attribution Request",
                            "description": "Name for this attribution request",
                        },
                        "organization": {
                            "type": "string",
                            "example": "onspot",
                            "description": "Organization making this request",
                        },
                        "targetingFeatureCollection": {
                            "$ref": "#/components/schemas/GeoJsonBase"
                        },
                        "attributionFeatureCollection": {
                            "$ref": "#/components/schemas/GeoJsonBase"
                        },
                        "callback": {
                            "type": "string",
                            "example": "https://myserver.com/sink",
                            "description": "Callback which will receive result of this request",
                        },
                    },
                    "required": [
                        "attributionFeatureCollection",
                        "callback",
                        "name",
                        "targetingFeatureCollection",
                    ],
                },
                "AttributionWithSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/AttributionBase"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                    ]
                },
                "AttributionData": {
                    "properties": {
                        "count": {
                            "type": "integer",
                            "format": "int32",
                            "example": 234,
                            "description": "Total count of devices that were seen in both the targetingFeatureCollection and the attributionFeatureCollection",
                        },
                        "frequency": {
                            "$ref": "#/components/schemas/DeviceCountFrequencyDatum"
                        },
                        "dayOfWeek": {
                            "type": "array",
                            "description": "Day of week data for records that were in the attributionFeatureCollection",
                            "items": {
                                "$ref": "#/components/schemas/DeviceCountGroupedByDay"
                            },
                        },
                        "hourOfDay": {
                            "type": "array",
                            "description": "Hour of day records that were in the attributionFeatureCollection",
                            "items": {
                                "$ref": "#/components/schemas/DeviceCountGroupedByHour"
                            },
                        },
                        "zipcodes": {"$ref": "#/components/schemas/ZipCodeResultSet"},
                        "cities": {
                            "type": "array",
                            "description": "Top 5 cities",
                            "items": {"type": "string"},
                        },
                    },
                    "required": [
                        "cities",
                        "count",
                        "dayOfWeek",
                        "frequency",
                        "hourOfDay",
                        "zipcodes",
                    ],
                    "type": "object",
                },
                "CallbackResponse": {
                    "properties": {
                        "id": {
                            "description": "id",
                            "example": "123e4567-e89b-12d3-a456-426655440000",
                            "type": "string",
                        },
                        "message": {
                            "description": "Information about the status of the request",
                            "example": "SUCCESS",
                            "type": "string",
                        },
                        "success": {
                            "description": "Success status",
                            "example": True,
                            "type": "boolean",
                        },
                    },
                    "required": ["id", "success"],
                    "type": "object",
                },
                "DeviceCount": {
                    "properties": {
                        "count": {
                            "description": "Number of times device was seen in the feature",
                            "example": 42,
                            "format": "int32",
                            "type": "integer",
                        },
                        "did": {
                            "description": "Device Id",
                            "example": "FYKO2ISHHHPJVVYQRXOZRMIHNDQUZX6XHXW3BYJ62PCILOVHKVQBI4TFGV4MYI4P2R5MK56VQCQ2W===",
                            "type": "string",
                        },
                    },
                    "required": ["count", "did"],
                    "type": "object",
                },
                "DeviceCountByIntervalDatum": {
                    "properties": {
                        "start": {
                            "type": "string",
                            "example": "2018-07-01T00:00",
                            "description": "Start date of interval",
                        },
                        "end": {
                            "type": "string",
                            "example": "2018-07-10T00:00",
                            "description": "End date of interval",
                        },
                        "count": {
                            "type": "integer",
                            "format": "int32",
                            "example": 23,
                            "description": "Unique count of devices within the interval",
                        },
                    },
                    "required": ["count", "end", "start"],
                    "type": "object",
                },
                "DeviceCountGroupedByDay": {
                    "type": "object",
                    "required": ["count", "day"],
                    "properties": {
                        "count": {
                            "type": "integer",
                            "format": "int32",
                            "example": 42,
                            "description": "Device count",
                        },
                        "day": {
                            "type": "string",
                            "example": "MONDAY",
                            "description": "Day of week",
                            "enum": [
                                "MONDAY",
                                "TUESDAY",
                                "WEDNESDAY",
                                "THURSDAY",
                                "FRIDAY",
                                "SATURDAY",
                                "SUNDAY",
                            ],
                        },
                    },
                },
                "DeviceCountGroupedByHour": {
                    "type": "object",
                    "required": ["count", "hour"],
                    "properties": {
                        "count": {
                            "type": "integer",
                            "format": "int32",
                            "example": 42,
                            "description": "Device count",
                        },
                        "hour": {
                            "type": "string",
                            "example": "1",
                            "description": "Hour of day",
                            "enum": [
                                "0",
                                "1",
                                "2",
                                "3",
                                "4",
                                "5",
                                "6",
                                "7",
                                "8",
                                "9",
                                "10",
                                "11",
                                "12",
                                "13",
                                "14",
                                "15",
                                "16",
                                "17",
                                "18",
                                "19",
                                "20",
                                "21",
                                "22",
                                "23",
                            ],
                        },
                    },
                },
                "Error": {
                    "properties": {
                        "message": {
                            "example": "An error has occurred",
                            "description": "Message describing the error that occurred",
                            "type": "string",
                        }
                    },
                    "required": ["message"],
                    "type": "object",
                },
                "DevicesSegment": {
                    "type": "object",
                    "required": ["partner", "organization", "sourcePaths"],
                    "properties": {
                        "callback": {
                            "example": "https://yourapi.com/callback",
                            "description": "Callback, response will be delivered to this url",
                            "type": "string",
                        },
                        "name": {
                            "example": "Feature Example",
                            "description": "Name for this feature",
                            "type": "string",
                        },
                        "validate": {
                            "default": True,
                            "description": "Validate on submission, will validate some input params on submission to the api, if False validation will be done as part of the async processing",
                            "example": True,
                            "type": "boolean",
                        },
                        "partner": {
                            "type": "string",
                            "example": "thetradedesk",
                            "enum": [
                                "appnexus",
                                "appnexusbidder",
                                "chalkdigital",
                                "eltoro",
                                "facebook",
                                "liquidm",
                                "lotame",
                                "resetdigital",
                                "rtbiq",
                                "thetradedesk",
                            ],
                        },
                        "segments": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "description": "Segment name",
                                "example": "My Segment",
                            },
                        },
                        "lotameSegments": {
                            "type": "array",
                            "description": "Lotame Segments",
                            "items": {"$ref": "#/components/schemas/LotameSegment"},
                        },
                        "sourcePaths": {
                            "type": "array",
                            "example": "s3://onspot-app/myorg/mydevices.txt",
                            "description": "Path to device files, should be a single device id per line in each file",
                            "items": {"type": "string"},
                        },
                        "organization": {
                            "type": "string",
                            "example": "onspot",
                            "description": "Organization Name",
                        },
                        "ttl": {
                            "type": "integer",
                            "format": "int32",
                            "example": 43200,
                            "description": "TTL for segment, not all partners accept this.",
                        },
                        "extend": {
                            "type": "boolean",
                            "default": False,
                            "description": "Should request include household extension?",
                            "example": True,
                        },
                        "advertiserId": {
                            "type": "string",
                            "example": "myid",
                            "description": "Advertiser Id, only applies to TheTradeDesk segments",
                        },
                        "secret": {
                            "type": "string",
                            "example": "shhh",
                            "description": "Secret, applies to TheTradeDesk segments",
                        },
                        "username": {
                            "type": "string",
                            "example": "username",
                            "description": "Username, applies to AppNexus segments",
                        },
                        "password": {
                            "type": "string",
                            "example": "shhh",
                            "description": "Password, applies to AppNexus segments",
                        },
                        "memberid": {
                            "type": "integer",
                            "example": 12345,
                            "format": "int32",
                            "description": "Member id, applies to AppNexus segments",
                        },
                        "orgId": {
                            "type": "string",
                            "description": "OrgId passed when publishing to eltoro",
                            "example": "myorgid",
                        },
                        "cpm": {
                            "type": "number",
                            "format": "double",
                            "example": 2.50,
                            "description": "cpm to apply for rtbiq publishes",
                        },
                        "segmentId": {
                            "type": "integer",
                            "format": "int32",
                            "example": 12345,
                            "description": "Segment Id",
                        },
                    },
                },
                "LotameSegment": {
                    "type": "object",
                    "properties": {
                        "files": {
                            "type": "array",
                            "description": "S3 path to file containing maid's",
                            "items": {"type": "string"},
                        },
                        "name": {"type": "string", "description": "Segment name"},
                        "id": {
                            "type": "integer",
                            "format": "int32",
                            "description": "Segment id",
                        },
                    },
                },
                "FeatureBase": {
                    "properties": {
                        "geometry": {"$ref": "#/components/schemas/Geometry"},
                        "properties": {"$ref": "#/components/schemas/PropertiesBase"},
                        "type": {
                            "enum": ["Feature"],
                            "example": "Feature",
                            "type": "string",
                        },
                    },
                    "required": ["geometry", "properties", "type"],
                    "type": "object",
                },
                "FeatureSegment": {
                    "properties": {
                        "geometry": {"$ref": "#/components/schemas/Geometry"},
                        "properties": {
                            "$ref": "#/components/schemas/PropertiesSegment"
                        },
                        "type": {
                            "enum": ["Feature"],
                            "example": "Feature",
                            "type": "string",
                        },
                    },
                    "required": ["geometry", "properties", "type"],
                    "type": "object",
                },
                "FeatureFilesBase": {
                    "type": "object",
                    "required": ["paths", "properties", "type"],
                    "properties": {
                        "type": {
                            "type": "string",
                            "example": "Files",
                            "enum": ["Files"],
                        },
                        "paths": {
                            "type": "array",
                            "example": ["s3://mybucket/my/file.csv"],
                            "items": {"type": "string"},
                        },
                        "properties": {
                            "$ref": "#/components/schemas/PropertiesFilesBase"
                        },
                    },
                    "description": "An API Input, either Feature or File",
                },
                "FilesSocialNetwork": {
                    "type": "object",
                    "required": ["paths", "properties", "type"],
                    "properties": {
                        "type": {
                            "type": "string",
                            "example": "Files",
                            "enum": ["Files"],
                        },
                        "paths": {
                            "type": "array",
                            "example": ["s3://mybucket/my/file.csv"],
                            "items": {"type": "string"},
                        },
                        "properties": {
                            "$ref": "#/components/schemas/PropertiesFilesSocialNetwork"
                        },
                    },
                    "description": "An API Input, either Feature or File",
                },
                "FilesPoliticalSave": {
                    "type": "object",
                    "required": ["paths", "properties", "type"],
                    "properties": {
                        "type": {
                            "type": "string",
                            "example": "Files",
                            "enum": ["Files"],
                        },
                        "paths": {
                            "type": "array",
                            "example": ["s3://mybucket/my/file.csv"],
                            "items": {"type": "string"},
                        },
                        "properties": {
                            "$ref": "#/components/schemas/PropertiesFilesPoliticalSave"
                        },
                    },
                },
                "FilesHouseholdAddressesSave": {
                    "type": "object",
                    "required": ["paths", "properties", "type"],
                    "properties": {
                        "type": {
                            "type": "string",
                            "example": "Files",
                            "enum": ["Files"],
                        },
                        "paths": {
                            "type": "array",
                            "example": ["s3://mybucket/my/file.csv"],
                            "items": {"type": "string"},
                        },
                        "properties": {
                            "$ref": "#/components/schemas/PropertiesFilesHouseholdAddressesSave"
                        },
                    },
                },
                "FilesWithMinMaxDevicesSave": {
                    "type": "object",
                    "required": ["paths", "properties", "type"],
                    "properties": {
                        "type": {
                            "type": "string",
                            "example": "Files",
                            "enum": ["Files"],
                        },
                        "paths": {
                            "type": "array",
                            "example": ["s3://mybucket/my/file.csv"],
                            "items": {"type": "string"},
                        },
                        "properties": {
                            "$ref": "#/components/schemas/PropertiesFilesWithMinMaxDevicesSave"
                        },
                    },
                },
                "FeatureDemographics": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesDemographics"
                                }
                            },
                        },
                    ]
                },
                "FeatureGroupedByDay": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesGroupedByDay"
                                }
                            },
                        },
                    ]
                },
                "FeatureGroupedByInterval": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesGroupedByInterval"
                                }
                            },
                        },
                    ]
                },
                "FeaturePoliticalWithSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesPoliticalWithSave"
                                }
                            },
                        },
                    ]
                },
                "FeaturePoliticalAggregate": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesPoliticalAggregate"
                                }
                            },
                        },
                    ]
                },
                "FeatureSocialNetwork": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesSocialNetwork"
                                }
                            },
                        },
                    ]
                },
                "FeatureTradeArea": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesTradeArea"
                                }
                            },
                        },
                    ]
                },
                "FeatureWithMinMax": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesWithMinMax"
                                }
                            },
                        },
                    ]
                },
                "FeatureWithMinMaxDevices": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesWithMinMaxDevices"
                                }
                            },
                        },
                    ]
                },
                "FeatureWithMinMaxDevicesSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesWithMinMaxDevicesSave"
                                }
                            },
                        },
                    ]
                },
                "FeatureWithDevices": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesWithDevices"
                                }
                            },
                        },
                    ]
                },
                "FeatureWithDevicesSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesWithDevicesSave"
                                }
                            },
                        },
                    ]
                },
                "FeatureWithHeadersSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesWithHeadersSave"
                                }
                            },
                        },
                    ]
                },
                "FeatureWithWorkplaceHeadersSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesWithWorkplaceHeadersSave"
                                }
                            },
                        },
                    ]
                },
                "FeatureWithObservationsSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FeatureBase"},
                        {
                            "type": "object",
                            "properties": {
                                "properties": {
                                    "$ref": "#/components/schemas/PropertiesWithObservationsSave"
                                }
                            },
                        },
                    ]
                },
                "FileFormat": {
                    "properties": {
                        "delimiter": {
                            "type": "string",
                            "example": "csv",
                            "description": "Format/delimiter of the output file",
                        },
                        "quoteEncapsulate": {
                            "type": "boolean",
                            "example": True,
                            "description": "Whether or not to quote encapsulate the output file",
                        },
                        "compressionType": {
                            "type": "string",
                            "example": "gzip",
                            "description": "How to compress the output file.",
                        },
                    },
                    "type": "object",
                },
                "FilesBase": {
                    "properties": {
                        "type": {"enum": ["FeatureCollection"], "type": "string"},
                        "features": {
                            "items": {"$ref": "#/components/schemas/FeatureFilesBase"},
                            "type": "array",
                        },
                    },
                    "required": ["features", "type"],
                    "type": "object",
                },
                "GeoJsonBase": {
                    "properties": {
                        "type": {"enum": ["FeatureCollection"], "type": "string"},
                        "features": {
                            "items": {"$ref": "#/components/schemas/FeatureBase"},
                            "type": "array",
                        },
                    },
                    "required": ["features", "type"],
                    "type": "object",
                },
                "GeoJsonSegment": {
                    "properties": {
                        "type": {"enum": ["FeatureCollection"], "type": "string"},
                        "features": {
                            "items": {"$ref": "#/components/schemas/FeatureSegment"},
                            "type": "array",
                        },
                    },
                    "required": ["features", "type"],
                    "type": "object",
                },
                "GeoJsonDemographics": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureDemographics"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonGroupedByDay": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureGroupedByDay"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonGroupedByInterval": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureGroupedByInterval"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonPoliticalWithSave": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeaturePoliticalWithSave"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonPoliticalAggregate": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeaturePoliticalAggregate"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonSocialNetwork": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureSocialNetwork"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonTradeArea": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureTradeArea"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonWithMinMax": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureWithMinMax"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonWithMinMaxDevices": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureWithMinMaxDevices"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonWithMinMaxDevicesSave": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureWithMinMaxDevicesSave"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonWithDevices": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureWithDevices"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonWithDevicesSave": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureWithDevicesSave"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonWithHeadersSave": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureWithHeadersSave"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonWithWorkplaceHeadersSave": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureWithWorkplaceHeadersSave"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "GeoJsonWithObservationsSave": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "features": {
                                    "items": {
                                        "$ref": "#/components/schemas/FeatureWithObservationsSave"
                                    },
                                    "type": "array",
                                }
                            },
                        }
                    ]
                },
                "Geometry": {
                    "discriminator": {
                        "mapping": {
                            "MultiPolygon": "#/components/schemas/MultiPolygon",
                            "PointAndRadius": "#/components/schemas/PointAndRadius",
                            "Polygon": "#/components/schemas/Polygon",
                        },
                        "propertyName": "type",
                    },
                    "oneOf": [
                        {"$ref": "#/components/schemas/MultiPolygon"},
                        {"$ref": "#/components/schemas/PointAndRadius"},
                        {"$ref": "#/components/schemas/Polygon"},
                    ],
                    "properties": {
                        "type": {
                            "enum": ["MultiPolygon", "PointAndRadius", "Polygon"],
                            "example": "MultiPolygon",
                            "type": "string",
                        }
                    },
                    "required": ["type"],
                    "type": "object",
                },
                "MinMaxProperties": {
                    "properties": {
                        "max": {
                            "default": 20,
                            "description": "Max devices found in a household that will contribute to results (30 is the max allowed value)",
                            "example": 20,
                            "format": "int32",
                            "type": "integer",
                        },
                        "min": {
                            "default": 1,
                            "description": "Min devices found in a household that will contribute to results (1 is the min allowed value)",
                            "example": 1,
                            "format": "int32",
                            "type": "integer",
                        },
                    },
                    "type": "object",
                },
                "MultiPolygon": {
                    "properties": {
                        "coordinates": {
                            "description": "Arrays of arrays of lng, lat coordinates that form the polygons, ex [[[[-122.46182830799995, 47.18459146300006],[-122.46182976799997, 47.18455096800005],...]]]",
                            "items": {
                                "items": {
                                    "example": [
                                        [-123.46478462219238, 39.07166187346857],
                                        [-123.4659218788147, 39.06921298141872],
                                        [-123.46278905868529, 39.0687964947246],
                                        [-123.4616732597351, 39.07079560844253],
                                        [-123.46478462219238, 39.07166187346857],
                                    ],
                                    "items": {
                                        "format": "double",
                                        "maxItems": 2,
                                        "minItems": 2,
                                        "type": "number",
                                    },
                                    "type": "array",
                                },
                                "type": "array",
                            },
                            "type": "array",
                        },
                        "type": {
                            "enum": ["MultiPolygon"],
                            "example": "MultiPolygon",
                            "type": "string",
                        },
                    },
                    "required": ["coordinates", "type"],
                    "type": "object",
                },
                "Observation": {
                    "type": "object",
                    "required": ["did", "timestamp"],
                    "properties": {
                        "did": {
                            "description": "Device Id",
                            "example": "FYKO2ISHHHPJVVYQRXOZRMIHNDQUZX6XHXW3BYJ62PCILOVHKVQBI4TFGV4MYI4P2R5MK56VQCQ2W===",
                            "type": "string",
                        },
                        "timestamp": {
                            "description": "Timestamp in ms of observation",
                            "example": 1498250334000,
                            "format": "int64",
                            "type": "integer",
                        },
                    },
                },
                "Point": {
                    "description": "Describes a geographic point",
                    "properties": {
                        "lat": {
                            "description": "Latitude",
                            "example": -88.04865002632141,
                            "format": "double",
                            "type": "number",
                        },
                        "lng": {
                            "description": "Longitude",
                            "example": 42.92723705298488,
                            "format": "double",
                            "type": "number",
                        },
                    },
                    "required": ["lat", "lng"],
                    "type": "object",
                },
                "PointAndRadius": {
                    "properties": {
                        "point": {"$ref": "#/components/schemas/Point"},
                        "points": {
                            "description": "Number of points to use when generating the circle, defaults to 16",
                            "example": 16,
                            "format": "int32",
                            "type": "integer",
                        },
                        "radius": {
                            "description": "Radius of the circle around the point in meters, maximum 200.0 meters",
                            "example": 10.0,
                            "format": "double",
                            "type": "number",
                        },
                        "type": {
                            "enum": ["PointAndRadius"],
                            "example": "PointAndRadius",
                            "type": "string",
                        },
                    },
                    "required": ["point", "radius", "type"],
                    "type": "object",
                },
                "Polygon": {
                    "properties": {
                        "coordinates": {
                            "description": "Arrays of lng, lat coordinates that form the polygons, ex [[[-122.46182830799995, 47.18459146300006],[-122.46182976799997, 47.18455096800005],...]]",
                            "items": {
                                "example": [
                                    [-123.46478462219238, 39.07166187346857],
                                    [-123.4659218788147, 39.06921298141872],
                                    [-123.46278905868529, 39.0687964947246],
                                    [-123.4616732597351, 39.07079560844253],
                                    [-123.46478462219238, 39.07166187346857],
                                ],
                                "items": {
                                    "format": "double",
                                    "maxItems": 2,
                                    "minItems": 2,
                                    "type": "number",
                                },
                                "type": "array",
                            },
                            "type": "array",
                        },
                        "type": {
                            "enum": ["Polygon"],
                            "example": "Polygon",
                            "type": "string",
                        },
                    },
                    "required": ["coordinates", "type"],
                    "type": "object",
                },
                "PropertiesBase": {
                    "description": "Collection of properties for this feature",
                    "properties": {
                        "callback": {
                            "example": "https://yourapi.com/callback",
                            "description": "Callback, response will be delivered to this url",
                            "type": "string",
                        },
                        "end": {
                            "example": "2020-06-19T23:59:59",
                            "description": "End date (UTC) for this feature in the format of YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD. End date must be before 5 days ago, ex. if today is 2018-05-08 the end date must be before 2018-05-03T00:00:00",
                            "type": "string",
                        },
                        "name": {
                            "example": "Feature Example",
                            "description": "Name for this feature",
                            "type": "string",
                        },
                        "start": {
                            "example": "2020-05-08T00:00:00",
                            "description": "Start date (UTC) for this feature in the format of YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD. Start date must be after 1st of the month 1 year ago, ex. if today is 2018-05-08 the start date must be after 2017-05-01T00:00:00",
                            "type": "string",
                        },
                        "validate": {
                            "default": True,
                            "description": "Validate on submission, will validate some input params on submission to the api, if False validation will be done as part of the async processing",
                            "example": True,
                            "type": "boolean",
                        },
                    },
                    "required": ["callback", "end", "name", "start"],
                    "type": "object",
                },
                "PropertiesSegment": {
                    "type": "object",
                    "required": ["partner", "organization", "segments"],
                    "properties": {
                        "callback": {
                            "example": "https://yourapi.com/callback",
                            "description": "Callback, response will be delivered to this url",
                            "type": "string",
                        },
                        "end": {
                            "example": "2020-06-19T23:59:59",
                            "description": "End date (UTC) for this feature in the format of YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD. End date must be before 5 days ago, ex. if today is 2018-05-08 the end date must be before 2018-05-03T00:00:00",
                            "type": "string",
                        },
                        "name": {
                            "example": "Feature Example",
                            "description": "Name for this feature",
                            "type": "string",
                        },
                        "start": {
                            "example": "2020-05-08T00:00:00",
                            "description": "Start date (UTC) for this feature in the format of YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD. Start date must be after 1st of the month 1 year ago, ex. if today is 2018-05-08 the start date must be after 2017-05-01T00:00:00",
                            "type": "string",
                        },
                        "validate": {
                            "default": True,
                            "description": "Validate on submission, will validate some input params on submission to the api, if False validation will be done as part of the async processing",
                            "example": True,
                            "type": "boolean",
                        },
                        "partner": {
                            "type": "string",
                            "example": "thetradedesk",
                            "enum": [
                                "appnexus",
                                "appnexusbidder",
                                "eltoro",
                                "facebook",
                                "liquidm",
                                "resetdigital",
                                "rtbiq",
                                "thetradedesk",
                            ],
                        },
                        "segments": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "description": "Segment name",
                                "example": "My Segment",
                            },
                        },
                        "organization": {
                            "type": "string",
                            "example": "onspot",
                            "description": "Organization Name",
                        },
                        "ttl": {
                            "type": "integer",
                            "format": "int32",
                            "example": 43200,
                            "description": "TTL for segment, not all partners accept this.",
                        },
                        "extend": {
                            "type": "boolean",
                            "default": False,
                            "description": "Should request include household extension?",
                            "example": True,
                        },
                        "advertiserId": {
                            "type": "string",
                            "example": "myid",
                            "description": "Advertiser Id, only applies to TheTradeDesk segments",
                        },
                        "secret": {
                            "type": "string",
                            "example": "shhh",
                            "description": "Secret, applies to TheTradeDesk segments",
                        },
                        "username": {
                            "type": "string",
                            "example": "username",
                            "description": "Username, applies to AppNexus segments",
                        },
                        "password": {
                            "type": "string",
                            "example": "shhh",
                            "description": "Password, applies to AppNexus segments",
                        },
                        "memberid": {
                            "type": "integer",
                            "example": 12345,
                            "format": "int32",
                            "description": "Member id, applies to AppNexus segments",
                        },
                        "orgId": {
                            "type": "string",
                            "description": "OrgId passed when publishing to eltoro",
                            "example": "myorgid",
                        },
                        "cpm": {
                            "type": "number",
                            "format": "double",
                            "example": 2.50,
                            "description": "cpm to apply for rtbiq publishes",
                        },
                        "segmentId": {
                            "type": "integer",
                            "format": "int32",
                            "example": 12345,
                            "description": "Segment Id",
                        },
                    },
                },
                "PropertiesFilesBase": {
                    "description": "Collection of properties for this feature",
                    "properties": {
                        "callback": {
                            "example": "https://yourapi.com/callback",
                            "description": "Callback, response will be delivered to this url",
                            "type": "string",
                        },
                        "name": {
                            "example": "Feature Example",
                            "description": "Name for this feature",
                            "type": "string",
                        },
                    },
                    "required": ["name", "callback"],
                },
                "PropertiesFilesSocialNetwork": {
                    "allOf": [{"$ref": "#/components/schemas/PropertiesSocialNetwork"}]
                },
                "PropertiesFilesPoliticalSave": {
                    "type": "object",
                    "required": ["name", "callback"],
                    "properties": {
                        "name": {
                            "type": "string",
                            "example": "Feature Example",
                            "description": "Name for this feature",
                        },
                        "max": {
                            "type": "integer",
                            "format": "int32",
                            "example": 20,
                            "description": "Max devices found in a household that will contribute to results",
                        },
                        "min": {
                            "type": "integer",
                            "format": "int32",
                            "example": 1,
                            "description": "Min devices found in a household that will contribute to results",
                        },
                        "callback": {
                            "type": "string",
                            "example": "https://yourapi.com/callback",
                            "description": "Callback, response will be delivered to this url",
                        },
                        "validate": {
                            "type": "boolean",
                            "example": True,
                            "description": "Validate on submission, will validate some input params on submission to the api, if False validation will be done as part of the async processing.",
                        },
                        "organization": {
                            "type": "string",
                            "example": "onspot",
                            "description": "Organization Name",
                        },
                        "outputProvider": {
                            "type": "string",
                            "example": "s3",
                            "default": "s3",
                            "enum": ["s3", "gs"],
                            "description": "s3 (aws) is the default, gs (google storage) also available but proper permissions must be configured with OnSpot",
                        },
                        "outputLocation": {
                            "type": "string",
                            "example": "s3://mybucket/my/path/",
                            "description": "Output path, if provided will write output to path provided, can also use gs prefix instead of s3",
                        },
                        "fileName": {
                            "type": "string",
                            "example": "myfile.csv",
                            "description": "Allow override of filename written out to output path",
                        },
                        "fileFormat": {"$ref": "#/components/schemas/FileFormat"},
                        "headers": {
                            "type": "array",
                            "example": [
                                "state",
                                "city",
                                "zipcode",
                                "zip4",
                                "deviceId",
                                "observationsCount",
                            ],
                            "default": [
                                "state",
                                "city",
                                "zipcode",
                                "zip4",
                                "deviceId",
                                "observationsCount",
                                "congressional_district",
                                "state_senate_district",
                                "state_assembly_district",
                                "conservative_party_probability",
                                "democratic_party_probability",
                                "green_party_probability",
                                "independent_party_probability",
                                "libertarian_party_probability",
                                "liberal_party_probability",
                                "republican_party_probability",
                                "state_donation_amount_percentile",
                                "state_donation_amount_prediction",
                                "state_donor_probability",
                                "federal_donation_amount_percentile",
                                "federal_donation_amount_prediction",
                                "federal_donor_probability",
                                "turnout_probability_midterm_general",
                                "turnout_probability_midterm_primary",
                                "turnout_probability_presidential_general",
                                "turnout_probability_presidential_primary",
                            ],
                            "description": "Headers to include in output. Possible values: state, city, zipcode, zip4, deviceId, observationsCount, congressional_district, state_senate_district, state_assembly_district, conservative_party_probability, democratic_party_probability, green_party_probability, independent_party_probability, libertarian_party_probability, liberal_party_probability, republican_party_probability, state_donation_amount_percentile, state_donation_amount_prediction, state_donor_probability, federal_donation_amount_percentile, federal_donation_amount_prediction, federal_donor_probability, turnout_probability_midterm_general, turnout_probability_midterm_primary, turnout_probability_presidential_general, turnout_probability_presidential_primary",
                            "uniqueItems": True,
                            "items": {"type": "string"},
                        },
                    },
                },
                "PropertiesFilesHouseholdAddressesSave": {
                    "type": "object",
                    "required": ["name", "callback"],
                    "properties": {
                        "name": {
                            "type": "string",
                            "example": "Feature Example",
                            "description": "Name for this feature",
                        },
                        "max": {
                            "type": "integer",
                            "format": "int32",
                            "example": 20,
                            "description": "Max devices found in a household that will contribute to results",
                        },
                        "min": {
                            "type": "integer",
                            "format": "int32",
                            "example": 1,
                            "description": "Min devices found in a household that will contribute to results",
                        },
                        "callback": {
                            "type": "string",
                            "example": "https://yourapi.com/callback",
                            "description": "Callback, response will be delivered to this url",
                        },
                        "validate": {
                            "type": "boolean",
                            "example": True,
                            "description": "Validate on submission, will validate some input params on submission to the api, if False validation will be done as part of the async processing.",
                        },
                        "organization": {
                            "type": "string",
                            "example": "onspot",
                            "description": "Organization Name",
                        },
                        "outputProvider": {
                            "type": "string",
                            "example": "s3",
                            "default": "s3",
                            "enum": ["s3", "gs"],
                            "description": "s3 (aws) is the default, gs (google storage) also available but proper permissions must be configured with OnSpot",
                        },
                        "outputLocation": {
                            "type": "string",
                            "example": "s3://mybucket/my/path/",
                            "description": "Output path, if provided will write output to path provided, can also use gs prefix instead of s3",
                        },
                        "fileName": {
                            "type": "string",
                            "example": "myfile.csv",
                            "description": "Allow override of filename written out to output path",
                        },
                        "fileFormat": {"$ref": "#/components/schemas/FileFormat"},
                    },
                },
                "PropertiesFilesWithMinMaxDevicesSave": {
                    "type": "object",
                    "required": ["name", "callback"],
                    "properties": {
                        "name": {
                            "type": "string",
                            "example": "Feature Example",
                            "description": "Name for this feature",
                        },
                        "max": {
                            "type": "integer",
                            "format": "int32",
                            "example": 20,
                            "description": "Max devices found in a household that will contribute to results",
                        },
                        "min": {
                            "type": "integer",
                            "format": "int32",
                            "example": 1,
                            "description": "Min devices found in a household that will contribute to results",
                        },
                        "callback": {
                            "type": "string",
                            "example": "https://yourapi.com/callback",
                            "description": "Callback, response will be delivered to this url",
                        },
                        "validate": {
                            "type": "boolean",
                            "example": True,
                            "description": "Validate on submission, will validate some input params on submission to the api, if False validation will be done as part of the async processing.",
                        },
                        "organization": {
                            "type": "string",
                            "example": "onspot",
                            "description": "Organization Name",
                        },
                        "outputProvider": {
                            "type": "string",
                            "example": "s3",
                            "default": "s3",
                            "enum": ["s3", "gs"],
                            "description": "s3 (aws) is the default, gs (google storage) also available but proper permissions must be configured with OnSpot",
                        },
                        "outputLocation": {
                            "type": "string",
                            "example": "s3://mybucket/my/path/",
                            "description": "Output path, if provided will write output to path provided, can also use gs prefix instead of s3",
                        },
                        "fileName": {
                            "type": "string",
                            "example": "myfile.csv",
                            "description": "Allow override of filename written out to output path",
                        },
                        "fileFormat": {"$ref": "#/components/schemas/FileFormat"},
                        "includeNonMatchedDevices": {
                            "type": "object",
                            "example": "False",
                            "description": "Include device Ids found within the polygon but where there is no match against the household or secondary address databases",
                        },
                    },
                },
                "PropertiesDemographics": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/MinMaxProperties"},
                        {
                            "properties": {
                                "demographics": {
                                    "type": "array",
                                    "example": ["estimated_age"],
                                    "description": "Demographic keys to provide data for, contact support@onspotdata.com for a complete list of valid keys",
                                    "items": {"type": "string"},
                                }
                            },
                            "type": "object",
                            "required": ["demographics"],
                        },
                    ]
                },
                "PropertiesGroupedByDay": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {
                            "properties": {
                                "interval": {
                                    "default": 24,
                                    "description": "Set interval to define uniques",
                                    "example": 24,
                                    "format": "int32",
                                    "type": "integer",
                                }
                            },
                            "type": "object",
                        },
                    ]
                },
                "PropertiesGroupedByInterval": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {
                            "type": "object",
                            "required": ["interval"],
                            "properties": {
                                "interval": {
                                    "type": "integer",
                                    "format": "int32",
                                    "example": 4,
                                    "description": "Interval size in hours",
                                }
                            },
                        },
                    ]
                },
                "PropertiesPoliticalWithSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                        {
                            "type": "object",
                            "properties": {
                                "headers": {
                                    "type": "array",
                                    "example": [
                                        "state",
                                        "city",
                                        "zipcode",
                                        "zip4",
                                        "deviceId",
                                        "observationsCount",
                                    ],
                                    "default": [
                                        "state",
                                        "city",
                                        "zipcode",
                                        "zip4",
                                        "deviceId",
                                        "observationsCount",
                                        "congressional_district",
                                        "state_senate_district",
                                        "state_assembly_district",
                                        "conservative_party_probability",
                                        "democratic_party_probability",
                                        "green_party_probability",
                                        "independent_party_probability",
                                        "libertarian_party_probability",
                                        "liberal_party_probability",
                                        "republican_party_probability",
                                        "state_donation_amount_percentile",
                                        "state_donation_amount_prediction",
                                        "state_donor_probability",
                                        "federal_donation_amount_percentile",
                                        "federal_donation_amount_prediction",
                                        "federal_donor_probability",
                                        "turnout_probability_midterm_general",
                                        "turnout_probability_midterm_primary",
                                        "turnout_probability_presidential_general",
                                        "turnout_probability_presidential_primary",
                                    ],
                                    "description": "Headers to include in output. Possible values: state, city, zipcode, zip4, deviceId, observationsCount, congressional_district, state_senate_district, state_assembly_district, conservative_party_probability, democratic_party_probability, green_party_probability, independent_party_probability, libertarian_party_probability, liberal_party_probability, republican_party_probability, state_donation_amount_percentile, state_donation_amount_prediction, state_donor_probability, federal_donation_amount_percentile, federal_donation_amount_prediction, federal_donor_probability, turnout_probability_midterm_general, turnout_probability_midterm_primary, turnout_probability_presidential_general, turnout_probability_presidential_primary",
                                    "uniqueItems": True,
                                    "items": {"type": "string"},
                                }
                            },
                        },
                    ]
                },
                "PropertiesPoliticalAggregate": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {
                            "type": "object",
                            "properties": {
                                "headers": {
                                    "type": "array",
                                    "example": [
                                        "congressional_district",
                                        "state_senate_district",
                                        "state_assembly_district",
                                        "conservative_party_probability",
                                        "democratic_party_probability",
                                    ],
                                    "default": [
                                        "congressional_district",
                                        "state_senate_district",
                                        "state_assembly_district",
                                        "conservative_party_probability",
                                        "democratic_party_probability",
                                        "green_party_probability",
                                        "independent_party_probability",
                                        "libertarian_party_probability",
                                        "liberal_party_probability",
                                        "republican_party_probability",
                                        "state_donation_amount_percentile",
                                        "state_donation_amount_prediction",
                                        "state_donor_probability",
                                        "federal_donation_amount_percentile",
                                        "federal_donation_amount_prediction",
                                        "federal_donor_probability",
                                        "turnout_probability_midterm_general",
                                        "turnout_probability_midterm_primary",
                                        "turnout_probability_presidential_general",
                                        "turnout_probability_presidential_primary",
                                    ],
                                    "description": "Headers to include in output. Possible values: congressional_district, state_senate_district, state_assembly_district, conservative_party_probability, democratic_party_probability, green_party_probability, independent_party_probability, libertarian_party_probability, liberal_party_probability, republican_party_probability, state_donation_amount_percentile, state_donation_amount_prediction, state_donor_probability, federal_donation_amount_percentile, federal_donation_amount_prediction, federal_donor_probability, turnout_probability_midterm_general, turnout_probability_midterm_primary, turnout_probability_presidential_general, turnout_probability_presidential_primary",
                                    "uniqueItems": True,
                                    "items": {"type": "string"},
                                }
                            },
                        },
                    ]
                },
                "PropertiesSocialNetwork": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                        {
                            "type": "object",
                            "properties": {
                                "countMin": {
                                    "type": "integer",
                                    "description": "Count minimum limit of number of times two devices were seen close together, value must be >= 5",
                                    "example": 5,
                                },
                                "levels": {
                                    "type": "integer",
                                    "description": "Number of levels to query for connections within the social network graph, only values of 1 and 2 are allowed",
                                    "enum": [1, 2],
                                    "example": 1,
                                },
                            },
                        },
                    ]
                },
                "PropertiesTradeArea": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                        {
                            "type": "object",
                            "properties": {
                                "radius": {
                                    "type": "number",
                                    "description": "Radius from center point to construct trade area polygon",
                                    "example": 5.0,
                                    "format": "float",
                                },
                                "includeHouseholds": {
                                    "type": "boolean",
                                    "description": "Whether or not to include households",
                                    "example": True,
                                },
                                "includeWorkplaces": {
                                    "type": "boolean",
                                    "description": "Whether or not to include workplaces",
                                    "example": True,
                                },
                            },
                        },
                    ]
                },
                "PropertiesWithMinMax": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/MinMaxProperties"},
                    ]
                },
                "PropertiesWithMinMaxDevices": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/MinMaxProperties"},
                    ]
                },
                "PropertiesWithDevices": {
                    "allOf": [{"$ref": "#/components/schemas/PropertiesBase"}]
                },
                "PropertiesWithDevicesSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                    ]
                },
                "PropertiesWithMinMaxDevicesSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/MinMaxProperties"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                    ]
                },
                "PropertiesWithHeadersSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                        {
                            "type": "object",
                            "properties": {
                                "headers": {
                                    "type": "array",
                                    "example": ["location", "deviceid", "city"],
                                    "default": [
                                        "location",
                                        "deviceid",
                                        "city",
                                        "state",
                                        "zipcode",
                                        "observationsDayCount",
                                        "observationsDay",
                                    ],
                                    "description": "Headers to include in output. Possible values: location,datestart,dateend,deviceid,addresshash,city,state,zipcode,zip4,dates visited,observations by date,observations,count,zip4latitude,zip4longitude,censusblock",
                                    "uniqueItems": True,
                                    "items": {"type": "string"},
                                }
                            },
                        },
                    ]
                },
                "PropertiesWithWorkplaceHeadersSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                        {
                            "type": "object",
                            "properties": {
                                "headers": {
                                    "type": "array",
                                    "example": ["location", "deviceid", "city"],
                                    "default": [
                                        "location",
                                        "deviceid",
                                        "address",
                                        "city",
                                        "state",
                                        "zipcode",
                                        "zip4",
                                        "observations",
                                        "observationsCount",
                                    ],
                                    "description": "Headers to include in output. Possible values: location, dateStart, dateEnd, deviceId, observationsCount, address, city, state, zipcode, zip4, latitude, longitude, geoidBlock, centroidLatitude, centroidLongitude, observations",
                                    "uniqueItems": True,
                                    "items": {"type": "string"},
                                }
                            },
                        },
                    ]
                },
                "PropertiesWithObservationsSave": {
                    "allOf": [
                        {"$ref": "#/components/schemas/PropertiesBase"},
                        {"$ref": "#/components/schemas/PropertiesWithSave"},
                        {
                            "type": "object",
                            "properties": {
                                "headers": {
                                    "type": "array",
                                    "example": ["location", "deviceid", "timestamp"],
                                    "default": [
                                        "location",
                                        "deviceid",
                                        "timestamp",
                                        "date",
                                        "time",
                                        "dayofweek",
                                        "lat",
                                        "lng",
                                    ],
                                    "description": "Headers to include in output. Possible values: location,deviceid,timestamp,date,time,Day of Week,latitude,longitude",
                                    "uniqueItems": True,
                                    "items": {"type": "string"},
                                }
                            },
                        },
                    ]
                },
                "PropertiesWithSave": {
                    "type": "object",
                    "properties": {
                        "organization": {
                            "type": "string",
                            "example": "onspot",
                            "description": "Organization Name",
                        },
                        "outputProvider": {
                            "type": "string",
                            "example": "s3",
                            "default": "s3",
                            "enum": ["s3", "gs"],
                            "description": "s3 (aws) is the default, gs (google storage) also available but proper permissions must be configured with OnSpot",
                        },
                        "outputLocation": {
                            "type": "string",
                            "example": "s3://mybucket/my/path/",
                            "description": "Output path, if provided will write output to path provided, can also use gs prefix instead of s3",
                        },
                        "fileName": {
                            "type": "string",
                            "example": "myfile.csv",
                            "description": "Allow override of filename written to output path",
                        },
                        "fileFormat": {"$ref": "#/components/schemas/FileFormat"},
                    },
                },
                "ResponseAttribution": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "type": "object",
                            "properties": {
                                "data": {"$ref": "#/components/schemas/AttributionData"}
                            },
                        },
                    ]
                },
                "ResponseAllLocations": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "type": "object",
                            "properties": {
                                "deviceLocations": {
                                    "description": "Location of devices found within this feature",
                                    "items": {"$ref": "#/components/schemas/Point"},
                                    "type": "array",
                                }
                            },
                            "required": ["deviceLocations"],
                        },
                    ]
                },
                "ResponseBase": {
                    "properties": {
                        "cbinfo": {"$ref": "#/components/schemas/CallbackResponse"},
                        "name": {
                            "description": "Name of the feature that this result was generated from",
                            "example": "Feature Example",
                            "type": "string",
                        },
                    },
                    "required": ["cbinfo", "name"],
                },
                "ResponseDemographicsAggregate": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "totalMatched": {
                                    "description": "total matched devices (includes those without a household match)",
                                    "example": 1234,
                                    "format": "int32",
                                    "type": "integer",
                                },
                                "demographics": {
                                    "type": "object",
                                    "example": {
                                        "estimated_age": {"A": 122, "B": 20, "C": 80}
                                    },
                                    "description": "Demographic results",
                                    "additionalProperties": {
                                        "type": "object",
                                        "additionalProperties": {"type": "number"},
                                    },
                                },
                            },
                            "required": ["demographics", "totalMatched"],
                            "type": "object",
                        },
                    ]
                },
                "ResponseDemographicsAggregateZipcodes": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "totalMatched": {
                                    "description": "total matched devices (includes those without a household match)",
                                    "example": 1234,
                                    "format": "int32",
                                    "type": "integer",
                                },
                                "demographics": {
                                    "type": "object",
                                    "example": {"zipcode": {"94115": 16, "90042": 2}},
                                    "description": "Zip code results",
                                    "additionalProperties": {
                                        "type": "object",
                                        "additionalProperties": {"type": "number"},
                                    },
                                },
                            },
                            "required": ["demographics", "totalMatched"],
                            "type": "object",
                        },
                    ]
                },
                "ResponsePoliticalAggregate": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "totalMatched": {
                                    "description": "total matched devices",
                                    "example": 1234,
                                    "format": "int32",
                                    "type": "integer",
                                },
                                "demographics": {
                                    "type": "object",
                                    "example": {
                                        "state_donation_amount_percentile": {
                                            "0%-10%": 215,
                                            "11%-20%": 113,
                                        }
                                    },
                                    "description": "Political demographic results",
                                    "additionalProperties": {
                                        "type": "object",
                                        "additionalProperties": {"type": "number"},
                                    },
                                },
                            },
                            "required": ["demographics", "totalMatched"],
                            "type": "object",
                        },
                    ]
                },
                "ResponseDeviceCount": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "type": "object",
                            "properties": {
                                "count": {
                                    "description": "Total distinct devices seen in the feature",
                                    "example": 4792,
                                    "format": "int32",
                                    "type": "integer",
                                }
                            },
                            "required": ["count"],
                        },
                    ]
                },
                "ResponseDeviceCountGroupedByDay": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "type": "object",
                            "required": ["deviceCountByDay"],
                            "properties": {
                                "deviceCountByDay": {
                                    "type": "array",
                                    "description": "Device Counts by day of week",
                                    "items": {
                                        "$ref": "#/components/schemas/DeviceCountGroupedByDay"
                                    },
                                }
                            },
                        },
                    ]
                },
                "ResponseDeviceCountGroupedByDevice": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "deviceCountByDevice": {
                                    "description": "Device count by device",
                                    "items": {
                                        "$ref": "#/components/schemas/DeviceCount"
                                    },
                                    "type": "array",
                                }
                            },
                            "required": ["deviceCountByDevice"],
                            "type": "object",
                        },
                    ]
                },
                "ResponseDeviceCountGroupedByHour": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "deviceCountByHour": {
                                    "type": "array",
                                    "description": "Device Counts by hour of day",
                                    "items": {
                                        "$ref": "#/components/schemas/DeviceCountGroupedByHour"
                                    },
                                }
                            },
                            "required": ["deviceCountByHour"],
                            "type": "object",
                        },
                    ]
                },
                "ResponseDeviceCountGroupedByInterval": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "deviceCountByInterval": {
                                    "type": "array",
                                    "description": "Device count by interval",
                                    "items": {
                                        "$ref": "#/components/schemas/DeviceCountByIntervalDatum"
                                    },
                                }
                            }
                        },
                    ]
                },
                "DeviceCountFrequencyDatum": {
                    "type": "object",
                    "properties": {
                        "one": {
                            "type": "integer",
                            "format": "int32",
                            "example": 12,
                            "description": "Count of devices seen once",
                        },
                        "two": {
                            "type": "integer",
                            "format": "int32",
                            "example": 24,
                            "description": "Count of devices seen twice",
                        },
                        "three": {
                            "type": "integer",
                            "format": "int32",
                            "example": 36,
                            "description": "Count of devices seen three times",
                        },
                        "fourToSeven": {
                            "type": "integer",
                            "format": "int32",
                            "example": 26,
                            "description": "Count of devices seen four to seven times",
                        },
                        "eightToFifteen": {
                            "type": "integer",
                            "format": "int32",
                            "example": 10,
                            "description": "Count of devices seen eight to fifteen times",
                        },
                        "sixteenToTwentyfive": {
                            "type": "integer",
                            "format": "int32",
                            "example": 2,
                            "description": "Count of devices seen sixteen to twentyfive times",
                        },
                        "twentySixOrMore": {
                            "type": "integer",
                            "format": "int32",
                            "example": 36,
                            "description": "Count of devices seen more than twentyfive times",
                        },
                    },
                    "required": [
                        "eightToFifteen",
                        "fourToSeven",
                        "one",
                        "sixteenToTwentyfive",
                        "three",
                        "twentySixOrMore",
                        "two",
                    ],
                },
                "ResponseDeviceFrequencyCount": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {"$ref": "#/components/schemas/DeviceCountFrequencyDatum"},
                    ]
                },
                "ResponseDeviceLocations": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "deviceLocations": {
                                    "type": "array",
                                    "description": "Location of devices found within this feature",
                                    "items": {"$ref": "#/components/schemas/Point"},
                                }
                            },
                            "required": ["deviceLocations"],
                            "type": "object",
                        },
                    ]
                },
                "ResponseDeviceLocationsByDay": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "deviceLocations": {
                                    "description": "Device locations one per day",
                                    "items": {"$ref": "#/components/schemas/Point"},
                                }
                            },
                            "required": ["deviceLocations"],
                            "type": "object",
                        },
                    ]
                },
                "ResponseDevices": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "properties": {
                                "devices": {
                                    "description": "Device ids",
                                    "items": {
                                        "example": "FYKO2ISHHHPJVVYQRXOZRMIHNDQUZX6XHXW3BYJ62PCILOVHKVQBI4TFGV4MYI4P2R5MK56VQCQ2W===",
                                        "type": "string",
                                    },
                                    "type": "array",
                                    "uniqueItems": True,
                                }
                            },
                            "required": ["devices"],
                            "type": "object",
                        },
                    ]
                },
                "ResponseDevicesInHomeCount": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "type": "object",
                            "properties": {
                                "count": {
                                    "type": "integer",
                                    "format": "int64",
                                    "example": 7000,
                                    "description": "Number of devices found belonging to the households identified by devices within the feature",
                                }
                            },
                        },
                    ]
                },
                "ResponseObservations": {
                    "allOf": [
                        {"$ref": "#/components/schemas/ResponseBase"},
                        {
                            "type": "object",
                            "properties": {
                                "observations": {
                                    "type": "array",
                                    "description": "Observations",
                                    "items": {
                                        "$ref": "#/components/schemas/Observation"
                                    },
                                }
                            },
                            "required": ["observations"],
                        },
                    ]
                },
                "ZipCodeResult": {
                    "type": "object",
                    "required": ["count", "zipcode"],
                    "properties": {
                        "zipcode": {
                            "type": "string",
                            "example": "94111",
                            "description": "5 digit zip code",
                        },
                        "count": {
                            "type": "integer",
                            "format": "int32",
                            "example": 42,
                            "description": "Count of households matched within this zip code",
                        },
                    },
                },
                "ZipCodeResultSet": {
                    "type": "object",
                    "required": ["total", "zipcodes"],
                    "properties": {
                        "total": {
                            "type": "integer",
                            "format": "int32",
                            "example": 100,
                            "description": "Total records that were matched to households",
                        },
                        "zipcodes": {
                            "type": "array",
                            "description": "Top 5 zip codes with their counts",
                            "items": {"$ref": "#/components/schemas/ZipCodeResult"},
                        },
                    },
                },
            },
            "securitySchemes": {
                "apiKey": {"type": "apiKey", "name": "x-api-key", "in": "header"},
                "sigv4": {
                    "type": "apiKey",
                    "name": "Authorization",
                    "in": "header",
                    "x-amazon-apigateway-authtype": "awsSigv4",
                },
            },
        },
        "info": {
            "contact": {"email": "support@onspotdata.com", "name": "API Support"},
            "description": "# Introduction\n<br/>At a high level the OnSpot API operates asynchronously, meaning each request submitted will be responded to at a later time (depending on current system load) to the callback provided. Some important points to remember when working with the API:\n1. The callback URL provided must be publicly accessible and should be an https url. http://localhost/ for example will not be reachable by our servers.\n2. Depending on the API calls being made responses may be over 100mb in size, double check that your allowed POST size is able to accept this.\n3. Review the Authorization doc [AWS API Client Gateway documentation/how-to](https://s3.amazonaws.com/onspot-public/docs/onspot-aws-auth.pdf), each request must be signed using an AWS Signature. OnSpot can provide examples in a number of languages or point to publicly accessible examples.\n4. The postman app (available on all platforms) is a good debugging tool, it can sign requests with an AWS signature, making it quick to test usage of endpoints.\n\n# Requests\nAll requests must include a header **x-api-key**, the api key will be provided by OnSpot and be signed with an AWS Signature.\n\n# Responses\nAll POST requests will return an object that is represented in the documentation below as a 200 response, the callback response is documented in the 201 response.",
            "title": "OnSpot API",
            "version": "2.0.0",
        },
        "openapi": "3.0.3",
        "paths": {
            "/geoframe/all/count": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceCount"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get distinct device count of devices seen within the feature provided.",
                    "operationId": "geoframeAllCount",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get distinct device count",
                    "tags": ["Device Counts"],
                }
            },
            "/geoframe/all/countgroupedbyday": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceCountGroupedByDay"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get (non distinct) count of device observations by day within the feature provided.",
                    "operationId": "geoframeAllCountGroupedByDay",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonGroupedByDay"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get observation count by day",
                    "tags": ["Device Counts"],
                }
            },
            "/geoframe/all/countgroupedbyhour": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceCountGroupedByHour"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get (non distinct) count of device observations by hour within the feature provided.",
                    "operationId": "geoframeAllCountGroupedByHour",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get observation count by hour",
                    "tags": ["Device Counts"],
                }
            },
            "/geoframe/all/countgroupedbyinterval": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceCountGroupedByInterval"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get devices grouped by a specific interval.",
                    "operationId": "geoframeAllCountGroupedByInterval",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonGroupedByInterval"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get devices grouped by interval.",
                    "tags": ["Device Counts"],
                }
            },
            "/geoframe/all/devicefrequencycount": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceFrequencyCount"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get frequency at which we see the same device within the feature, broken down by device seen 1,2,3,4-7,8-15,16-25,26+ times.",
                    "operationId": "geoframeAllDeviceFrequencyCount",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get frequency a device is seen",
                    "tags": ["Device Counts"],
                }
            },
            "/geoframe/all/devicelocationsbyday": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceLocationsByDay"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get one location per device per day that is within the feature provided.",
                    "operationId": "geoframeAllDeviceLocationsByDay",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get one location per device per day",
                    "tags": ["Device Locations"],
                }
            },
            "/geoframe/all/devices": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDevices"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get each device id that was seen within the feature provided.",
                    "operationId": "geoframeAllDevices",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get Devices",
                    "tags": ["Devices"],
                }
            },
            "/geoframe/all/distinctdevicelocations": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceLocations"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get list of distinct locations that devices were seen within the feature provided.",
                    "operationId": "geoframeAllDistinctDeviceLocations",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get distinct locations of devices",
                    "tags": ["Device Locations"],
                }
            },
            "/geoframe/all/countgroupedbydevice": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceCountGroupedByDevice"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get a count associated with each device that was seen within the feature provided.",
                    "operationId": "geoframeAllCountGroupedByDevice",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get count per device",
                    "tags": ["Device Counts"],
                }
            },
            "/geoframe/all/locations": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseAllLocations"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get one location per device that was seen within the feature provided.",
                    "operationId": "geoframeAllLocations",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get one location per device",
                    "tags": ["Device Locations"],
                }
            },
            "/geoframe/all/observations": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseObservations"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get all observations within the given feature with hashed device ids and timestamp",
                    "operationId": "geoframeAllObservations",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithDevices"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get observations",
                    "tags": ["Devices"],
                }
            },
            "/geoframe/common/count": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceCount"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get count of devices that were seen in all features provided. Callback should be the same value in all features provided",
                    "operationId": "geoframeCommonCount",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get common count",
                    "tags": ["Device Counts"],
                }
            },
            "/geoframe/common/devices": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDevices"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get devices that were seen in all features provided",
                    "operationId": "geoframeCommonDevices",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get common devices",
                    "tags": ["Devices"],
                }
            },
            "/geoframe/demographics/aggregate": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDemographicsAggregate"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get aggregate demographics for devices matched within the feature provided. For the complete list of demographic keys available contact support@onspotdata.com.",
                    "operationId": "geoframeDemographicsAggregate",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonDemographics"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get demographics",
                    "tags": ["Demographics"],
                }
            },
            "/geoframe/demographics/aggregate/zipcodes": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDemographicsAggregateZipcodes"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get zipcodes of households matched in the feature. This requires a separate end point as we don't want to match all members of the household in this case, only one",
                    "operationId": "geoframeDemographicsAggregateZipcodes",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithMinMax"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get zipcodes",
                    "tags": ["Demographics"],
                }
            },
            "/geoframe/political/aggregate": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponsePoliticalAggregate"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get political aggregate demographics for devices matched within the feature provided. For the complete list of demographic keys available contact support@onspotdata.com.",
                    "operationId": "geoframePoliticalAggregate",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonPoliticalAggregate"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save all political data",
                    "tags": ["Political", "Demographics"],
                }
            },
            "/geoframe/legacyextension/devicesinhome": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDevices"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get active device ids that were matched to households associated to devices observed 1 year prior to the date range within the feature provided.",
                    "operationId": "geoframeLegacyExtensionDevicesInHome",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithMinMaxDevices"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get legacy household extended devices",
                    "tags": ["Devices"],
                }
            },
            "/geoframe/extension/devicesinhome": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDevices"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get device ids that were matched to households identified by the devices within the feature provided.",
                    "operationId": "geoframeExtensionDevicesInHome",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithMinMaxDevices"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get household extended devices",
                    "tags": ["Devices"],
                }
            },
            "/geoframe/extension/devicesinhomecount": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDevicesInHomeCount"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get number of devices that belong to households identified by the devices within the feature, this does not take into account devices that were not matched to homes.",
                    "operationId": "geoframeExtensionDevicesInHomeCount",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithMinMax"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get extended device count",
                    "tags": ["Device Counts"],
                }
            },
            "/geoframe/household/count": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDeviceCount"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get number of households that were matched from devices found within this geoframe",
                    "operationId": "geoframeHouseholdCount",
                    "requestBody": {"$ref": "#/components/requestBodies/GeoJsonBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get matched device count",
                    "tags": ["Device Households"],
                }
            },
            "/geoframe/household/devices": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseDevices"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get device ids that were matched to households within this geoframe, this does not include additional devices within the household",
                    "operationId": "geoframeWithMinMaxDevices",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithDevices"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get matched device ids",
                    "tags": ["Device Households"],
                }
            },
            "/geoframes/attribution": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/ResponseAttribution"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Gets device list from the targetingFeatureCollection, compares this to the attributionFeatureCollection and bases the response on the intersection of device ids in these 2 sets.",
                    "operationId": "geoframesAttribution",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/AttributionBase"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-Async"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get attribution report",
                    "tags": ["Attribution"],
                }
            },
            "/save/geoframe/all/countgroupedbydevice": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save a count associated with each device that was seen within the feature provided.",
                    "operationId": "saveGeoframeAllCountGroupedByDevice",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithDevicesSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save count per device",
                    "tags": ["Device Counts"],
                }
            },
            "/save/geoframe/all/devices": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save each device id that was seen within the feature provided.",
                    "operationId": "saveGeoframeAllDevices",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithDevicesSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save devices",
                    "tags": ["Devices"],
                }
            },
            "/save/geoframe/all/observations": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save all observations within the given feature with hashed device ids and timestamp.",
                    "operationId": "saveGeoframeAllObservations",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithObservationsSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save observations",
                    "tags": ["Devices"],
                }
            },
            "/save/geoframe/legacyextension/devicesinhome": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save active device ids that were matched to households associated to devices observed 1 year prior to the date range within the feature provided.",
                    "operationId": "saveGeoframeLegacyExtensionDevicesInHome",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithMinMaxDevicesSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save household legacy extended devices",
                    "tags": ["Devices"],
                }
            },
            "/save/geoframe/extension/devicesinhome": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save device ids that were matched to households identified by the devices with the feature provided.",
                    "operationId": "saveGeoframeExtensionDevicesInHome",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithMinMaxDevicesSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save extended devices (Geojson)",
                    "tags": ["Devices"],
                }
            },
            "/save/files/extension/devicesinhome": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save device ids that were matched to households identified by the devices with the feature provided.",
                    "operationId": "saveFilesExtensionDevicesInHome",
                    "requestBody": {"$ref": "#/components/requestBodies/FilesBase"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save extended devices (file)",
                    "tags": ["Devices"],
                }
            },
            "/save/geoframe/all/devicecountataddress": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save household of each device that was matched and number of times each device was seen in the feature provided.",
                    "operationId": "saveGeoframeAllDeviceCountAtAddress",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithHeadersSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save device count at address",
                    "tags": ["Household Locations"],
                }
            },
            "/save/geoframe/all/devicecountatworkplaceaddress": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save workplace of each device that was matched and number of times each device was seen in the feature provided.",
                    "operationId": "saveGeoframeAllDeviceCountAtWorkplaceAddress",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithWorkplaceHeadersSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save device count at address",
                    "tags": ["Workplace Locations"],
                }
            },
            "/save/geoframe/demographics/all": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save all demographic records found within the given feature.",
                    "operationId": "saveGeoframeDemographicsAll",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonWithMinMaxDevicesSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save all demographics",
                    "tags": ["Demographics"],
                }
            },
            "/save/files/political": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save all political data records for the provided MAIDs",
                    "operationId": "saveFilesPolitical",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/FilesPoliticalSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save all political data records for the provided MAIDs",
                    "tags": ["Political"],
                }
            },
            "/save/files/demographics/all": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save all demographic records found within the given feature.",
                    "operationId": "saveFilesDemographicsAll",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/FilesWithMinMaxDevicesSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save all demographics",
                    "tags": ["Demographics"],
                }
            },
            "/save/geoframe/political": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save all political data records found within the given feature.",
                    "operationId": "saveGeoframePolitical",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonPoliticalWithSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save all political data",
                    "tags": ["Political"],
                }
            },
            "/save/geoframes/attribution/locations": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Gets device list from the targetingFeatureCollection, compares this to the attributionFeatureCollection and bases the response on the intersection of device ids in these 2 sets. Saves a csv with city, state and zip of devices within the intersection.",
                    "operationId": "saveGeoframesAttributionLocations",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/AttributionWithSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save attribution locations",
                    "tags": ["Attribution"],
                }
            },
            "/save/geoframes/attribution/journey": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Based on destination also shows all Sources that were visited by a device. Includes dates seen to determine when the device went to each location (buyers journey). Csv contains Destination, ID, city, state, zipcode, zip4, Source Audience, Dates Seen, Date First Seen and Also Seen.",
                    "operationId": "saveGeoframesAttributionJourney",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/AttributionWithSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save attribution journey",
                    "tags": ["Attribution"],
                }
            },
            "/save/geoframes/attribution/devices": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Gets device list from the targetingFeatureCollection, compares this to the attributionFeatureCollection and bases the response on the intersection of device ids in these 2 sets",
                    "operationId": "saveGeoframesAttributionDevices",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/AttributionWithSave"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save attribution devices",
                    "tags": ["Attribution"],
                }
            },
            "/save/addresses/all/devices": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save mobile advertising ids that were found at the addresses provided.",
                    "operationId": "saveAddressesAllDevices",
                    "requestBody": {"$ref": "#/components/requestBodies/Address"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save devices found at addresses",
                    "tags": ["Residential Audiences"],
                }
            },
            "/save/businessaddresses/all/locations": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save business locations to csv with lat,lng appended",
                    "operationId": "saveBusinessAddressesAllLocations",
                    "requestBody": {"$ref": "#/components/requestBodies/Address"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save business locations to csv with lat,lng appended",
                    "tags": ["Commercial Audiences"],
                }
            },
            "/save/businessaddresses/all/devices": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get each device id that was seen within the matched provided locations",
                    "operationId": "saveBusinessAddressesAllDevices",
                    "requestBody": {"$ref": "#/components/requestBodies/Address"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get devices from commercial locations",
                    "tags": ["Commercial Audiences"],
                }
            },
            "/save/businessaddresses/all/demographics": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get aggregate demographics for devices matched within the provided locations. For the complete list of demographic keys available contact support@onspotdata.com.",
                    "operationId": "saveBusinessAddressesAllDemographics",
                    "requestBody": {"$ref": "#/components/requestBodies/Address"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get demographics from commercial locations",
                    "tags": ["Commercial Audiences"],
                }
            },
            "/save/geoframe/extension/socialnetwork": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save devices that are connected to a social graph in the feature provided.",
                    "operationId": "saveGeoframeExtensionSocialNetwork",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonSocialNetwork"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save social network extended devices",
                    "tags": ["Social Network Extension"],
                }
            },
            "/save/files/extension/socialnetwork": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save devices that are connected to a social graph in the deviceId files provided.",
                    "operationId": "saveFilesExtensionSocialNetwork",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/FilesSocialNetwork"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncSave"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save social network extended devices",
                    "tags": ["Social Network Extension"],
                }
            },
            "/geoframe/segment/push": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Publish segments containing the device ids within the geoframe provided to a partner.",
                    "operationId": "geoframeSegmentPush",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/GeoJsonSegment"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncPublish"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Publish geoframe segment",
                    "tags": ["Publish"],
                }
            },
            "/devices/segment/push": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Publish segments containing the device ids within the file provided to a partner.",
                    "operationId": "devicesSegmentPush",
                    "requestBody": {
                        "$ref": "#/components/requestBodies/DevicesSegment"
                    },
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncPublish"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get devices matched to households and workplaces within a specified radius of a geoframe.",
                    "tags": ["Publish"],
                }
            },
            "/geoframe/tradearea": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Get devices matched to households and workplaces within a specified radius of a geoframe.",
                    "operationId": "geoframeTradearea",
                    "requestBody": {"$ref": "#/components/requestBodies/TradeArea"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncPublish"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Get devices matched to households and workplaces within a specified radius of a geoframe.",
                    "tags": ["Trade Area"],
                }
            },
            "/save/geoframe/tradearea": {
                "post": {
                    "callbacks": {
                        "successCallback": {
                            "To callback provided": {
                                "post": {
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": {
                                                    "$ref": "#/components/schemas/CallbackResponse"
                                                }
                                            }
                                        },
                                        "description": "Callback payload",
                                    },
                                    "responses": {
                                        "200": {
                                            "$ref": "#/components/responses/200-Callback-Response"
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "description": "Save devices matched to households and workplaces within a specified radius of a geoframe.",
                    "operationId": "saveGeoframeTradearea",
                    "requestBody": {"$ref": "#/components/requestBodies/TradeArea"},
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/200-ServerResponse-AsyncPublish"
                        },
                        "400": {"$ref": "#/components/responses/400"},
                        "403": {"$ref": "#/components/responses/403"},
                        "415": {"$ref": "#/components/responses/415"},
                        "500": {"$ref": "#/components/responses/500"},
                    },
                    "security": [{"apiKey": [], "sigv4": []}],
                    "summary": "Save devices matched to households and workplaces within a specified radius of a geoframe.",
                    "tags": ["Trade Area"],
                }
            },
        },
        "servers": [
            {"description": "Development Server", "url": "http://localhost:9000"},
            {"description": "QA Server", "url": "https://api.qa.onspotdata.com"},
            {"description": "Production Server", "url": "https://api.onspotdata.com"},
        ],
        "tags": [
            {"name": "Attribution"},
            {"name": "Commercial Audiences"},
            {"name": "Demographics"},
            {"name": "Device Counts"},
            {"name": "Device Locations"},
            {"name": "Device Households"},
            {"name": "Devices"},
            {"name": "Household Locations"},
            {"name": "Political"},
            {"name": "Publish"},
            {"name": "Residential Audiences"},
            {"name": "Social Network Extension"},
            {"name": "Trade Area"},
            {"name": "Workplace Locations"},
        ],
        "x-amazon-apigateway-api-key-source": "HEADER",
    }
