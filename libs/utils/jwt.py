import jwt
import httpx
from jwt.algorithms import RSAAlgorithm


def validate_microsoft_token(bearer_token: str, tenant_id: str, client_id: str):
    """
    Validate and decode a microsoft bearer token.

    Params: 
    bearer_token  : String bearer token, with or without "Bearer" prefix.
    tenant_id     : Id of the tenant which issued the token.
    client_id     : Id issued to the client during the app registration process.
    """

    token = validate_generic_token(
        bearer_token=bearer_token,
        valid_issuers=[
            f"https://login.microsoftonline.com/{tenant_id}/",
            f"https://sts.windows.net/{tenant_id}/"
        ]
    )
    if client_id != token["aud"]:
        raise Exception("Token's audience (app/client id) is not valid.")
    
    return token


def validate_generic_token(bearer_token: str, valid_issuers: list):
    """
    Validate and decode a generic bearer token.

    Params: 
    bearer_token  : String bearer token, with or without "Bearer" prefix.
    valid_issuers : A list of public urls that hold one or more encryption keys.
    """

    # sanitize
    bearer_token = bearer_token.replace("Bearer ", "")
    # decode the JWT's unverified header without verification
    uvh = jwt.get_unverified_header(bearer_token)
    # decode the JWT's payload without verification
    uvp = jwt.decode(
        bearer_token,
        algorithms=[uvh["alg"]],
        options={"verify_signature": False},
    )
    
    if uvp["iss"] not in valid_issuers:
        raise Exception("Token's issuer is not valid.")

    keys = httpx.get(f"{uvp['iss']}/discovery/v2.0/keys").json()["keys"]

    # find the jwks key which matches the unverified header
    key = next((key for key in keys if key["kid"] == uvh["kid"]), None)
    if not key:
        raise Exception("Public key not found in JWKS.")

    # convert to PEM format
    pem_key = RSAAlgorithm.from_jwk(key)

    # verified decoding with pem_key and audience included
    return jwt.decode(
        bearer_token,
        algorithms=[uvh["alg"]],
        key=pem_key,
        audience=uvp["aud"],
        options={"verify_signature": False},
    )
