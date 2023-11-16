import jwt
import httpx
import json
from jwt.algorithms import RSAAlgorithm

def validate_bearer_token(bearer_token:str, tenant_id:str):

    # find the public JWT keys that will be used to validate the headers
    jwks_uri = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
    response = httpx.get(jwks_uri).json()
    keys = response['keys']

    # decode the JWT as an unverified header
    uvh = jwt.get_unverified_header(bearer_token.replace("Bearer ", ""))

    # find the jwks key which matches the unverified header
    key = next((key for key in keys if key["kid"] == uvh["kid"]), None)
    if not key:
        raise Exception("Public key not found in JWKS.")

    # convert to PEM format
    pem_key = RSAAlgorithm.from_jwk(key)

    # unverified decoding to get audience
    audience = jwt.decode(
        bearer_token.replace("Bearer ", ""),
        algorithms=[uvh['alg']],
        options={'verify_signature':False}
    )['aud']

    # verified decoding with pem_key and audience included
    return jwt.decode(
        bearer_token.replace("Bearer ", ""),
        algorithms=[uvh['alg']],
        key=pem_key,
        audience=audience,
        options={'verify_signature':False}
    )