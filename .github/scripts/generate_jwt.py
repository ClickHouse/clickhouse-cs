"""Sign a short-lived JWT for the ClickHouse Cloud JWT authentication tests.

Reads the RSA private key (PEM) from the JWT_PKEY environment variable and prints the
signed token to stdout. The claims must match the JWT provider configured on the Cloud
service; see README.md in this directory.
"""

import argparse
import os
import sys
import time

from jwcrypto.jwk import JWK
from jwcrypto.jwt import JWT


def get_private_key_pem():
    """Return the private key PEM from the JWT_PKEY environment variable."""
    pkey = os.getenv("JWT_PKEY")
    if pkey:
        return pkey
    raise ValueError("Private key not found. Set the JWT_PKEY environment variable.")


def generate_jwt_token(private_key_pem, issuer, audience, subject, expiration_seconds):
    """Return an RS256-signed JWT that expires after expiration_seconds."""
    jwk_key = JWK.from_pem(private_key_pem.encode("utf-8"))

    now = int(time.time())
    claims = {
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "iat": now,
        "exp": now + expiration_seconds,
    }
    header = {
        "alg": "RS256",
        "typ": "JWT",
        # JWK.from_pem sets kid to the key thumbprint, which is also the kid in jwks.json.
        "kid": jwk_key.get("kid"),
    }

    token = JWT(header=header, claims=claims)
    token.make_signed_token(jwk_key)
    return token.serialize()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issuer", default="mydomain.com", help="the iss claim")
    parser.add_argument("--audience", default="ci-test-service", help="the aud claim")
    parser.add_argument("--subject", default="ci-test", help="the sub claim")
    parser.add_argument("--ttl", type=int, default=1800, help="token lifetime in seconds")
    args = parser.parse_args()

    token = generate_jwt_token(get_private_key_pem(), args.issuer, args.audience, args.subject, args.ttl)
    sys.stdout.write(token)


if __name__ == "__main__":
    main()
