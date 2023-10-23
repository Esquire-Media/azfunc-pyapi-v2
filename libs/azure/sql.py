import os
import struct
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
from .credentials import *


def GenerateAzSQLConnectionString(server, database):
    options = {
        "Driver": "{ODBC Driver 17 for SQL Server}",
        "Server": "tcp:",
        "Database": database
    }

    if os.environ.get(f"sql_instance_{server}"):
        host = os.environ.get(f"sql_instance_{server}")
    else:
        host = server
    if "." not in host:
        host += ".database.windows.net"
    options["Server"] += host

    if os.environ.get(f"sql_port_{server}"):
        options["Server"] += ","+os.environ.get(f"sql_port_{server}")
    else:
        options["Server"] += ",1433"

    if os.environ.get(f"sql_username_{server}"):
        options["UID"] = os.environ.get(f"sql_username_{server}")

    if os.environ.get(f"sql_password_{server}"):
        options["PWD"] = os.environ.get(f"sql_password_{server}")

    connection_string = []
    for key in options.keys():
        connection_string.append(f"{key}={options[key]}")
    params = quote_plus(";".join(connection_string)+";")

    return "mssql+pyodbc:///?odbc_connect=%s" % params

def AzSQLEngine(server, database):
    engine = create_engine(GenerateAzSQLConnectionString(server, database))
    if not os.environ.get(f"sql_password_{server}"):
        @event.listens_for(engine, "do_connect")
        def provide_token(dialect, conn_rec, cargs, cparams):
            # remove the "Trusted_Connection" parameter that SQLAlchemy adds
            cargs[0] = cargs[0].replace(";Trusted_Connection=Yes", "")

            # create token credential
            raw_token = AccountToken("https://database.windows.net/.default").token.encode("utf-16-le")
            token_struct = struct.pack(f"<I{len(raw_token)}s", len(raw_token), raw_token)

            # apply it to keyword arguments
            cparams["attrs_before"] = {1256: token_struct}
    
    return engine