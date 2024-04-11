from config import MAPPING_DATASOURCE


def CETAS_Primary(instance_id, ingress):
    return (
        {
            "instance_id": instance_id,
            **MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]],
            "destination": {
                "conn_str": ingress["working"]["conn_str"],
                "container_name": ingress["working"]["container_name"],
                "blob_prefix": "{}/{}/{}/-1".format(
                    ingress["working"]["blob_prefix"],
                    instance_id,
                    ingress["audience"]["id"],
                ),
                "handle": "sa_esqdevdurablefunctions",  # will need to change at some point
                "format": "CSV",
            },
            "query": """
                    SELECT * FROM {}{}
                    WHERE {}
                """.format(
                (
                    "[{}].".format(
                        MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                            "table"
                        ]["schema"]
                    )
                    if MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                        "table"
                    ].get("schema", None)
                    else ""
                ),
                "["
                + MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]]["table"][
                    "name"
                ]
                + "]",
                ingress["audience"]["dataFilter"],
            ),
        },
    )
