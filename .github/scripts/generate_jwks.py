"""Print the public JWKS (jwks.json) for the private key in the JWT_PKEY environment variable.

The output holds only the public key and a self-signed x5c certificate. It never holds the
private key. See README.md in this directory for how to use it.
"""

import base64
import datetime
import json
import os

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.x509.oid import NameOID
from jwcrypto.jwk import JWK


def get_private_key_pem():
    """Return the private key PEM from the JWT_PKEY environment variable."""
    pkey = os.getenv("JWT_PKEY")
    if pkey:
        return pkey
    raise ValueError("Private key not found. Set the JWT_PKEY environment variable.")


def generate_public_jwks_json(private_key_pem, issuer="mydomain.com"):
    """Return the public JWKS JSON with the x5c certificate chain and the public key properties."""
    private_key = serialization.load_pem_private_key(private_key_pem.encode("utf-8"), password=None)

    cert_subject = cert_issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "My Organization"),
            x509.NameAttribute(NameOID.COMMON_NAME, issuer),
        ]
    )

    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(cert_subject)
        .issuer_name(cert_issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(private_key=private_key, algorithm=hashes.SHA256())
    )
    cert_der_base64 = base64.b64encode(certificate.public_bytes(serialization.Encoding.DER)).decode("utf-8")

    jwk = JWK.from_pem(private_key_pem.encode("utf-8"))
    jwk["x5c"] = [cert_der_base64]
    jwk["use"] = "sig"

    # export_public() leaves out the private key parameters (d, p, q, ...).
    public_jwk = json.loads(jwk.export_public())
    return json.dumps({"keys": [public_jwk]}, indent=2)


if __name__ == "__main__":
    print(generate_public_jwks_json(get_private_key_pem()))
