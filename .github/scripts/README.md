# JWT authentication tests: setup

`ClickHouse.Driver.Tests/ADO/BearerAuthenticationTests.cs` tests JWT authentication against
ClickHouse Cloud. The tests run in the Cloud job (`.github/workflows/tests-cloud.yml`). The job
signs a new token for each run with `generate_jwt.py` and gives it to the tests in the
`CLICKHOUSE_CLOUD_JWT` environment variable. If the `JWT_PKEY` secret is not set, the job shows a
warning and the JWT tests are skipped.

The parts:

| Part | Where | Secret? |
|---|---|---|
| RSA private key (PEM) | repository secret `JWT_PKEY` | yes |
| Public key set | `.github/jwks.json` in this repository (step 2 adds it) | no |
| JWT provider | the Cloud service of the `CLICKHOUSE_CLOUD_HOST` secret | no |

The provider and the token must agree on these claims (the defaults of `generate_jwt.py`):

| Claim | Value |
|---|---|
| `iss` (Issuer) | `mydomain.com` |
| `aud` (Audience) | `ci-test-service` |
| `sub` | `ci-test` |

These are the values that the clickhouse-java tests use, so one provider can serve both
repositories if they use the same Cloud service and key.

## Setup

Do these steps outside of the repository directory, so that the private key cannot be committed.
Add the secret last (step 5): when the secret is set, the Cloud job uses it, and the JWT tests fail
until the provider accepts the tokens.

1. Make the private key:

   ```bash
   openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out pkcs8_key.pem
   ```

2. Install the script dependencies and make `jwks.json`:

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install --require-hashes -r <repo>/.github/scripts/requirements.txt
   export JWT_PKEY="$(cat pkcs8_key.pem)"
   .venv/bin/python <repo>/.github/scripts/generate_jwks.py > <repo>/.github/jwks.json
   ```

   Commit and push `.github/jwks.json`. It contains only the public key.

3. In the ClickHouse Cloud console, open the service of the `CLICKHOUSE_CLOUD_HOST` secret. Go to
   **Settings** > **Security** > **JWT authentication** > **Set up JWT providers**, and add a
   provider:

   - **Name**: `clickhouse-cs-ci` (any name; you cannot change it later)
   - **Issuer**: `mydomain.com`
   - **Audience**: `ci-test-service`
   - **JWKS URL**: `https://raw.githubusercontent.com/ClickHouse/clickhouse-cs/main/.github/jwks.json`
     (before the file is on `main`, use the branch name instead of `main`, and change the URL
     after the merge)
   - **Roles claim**: empty

4. Make sure that the service accepts a token:

   ```bash
   TOKEN="$(.venv/bin/python <repo>/.github/scripts/generate_jwt.py)"
   curl -H "Authorization: Bearer $TOKEN" \
     "https://<CLICKHOUSE_CLOUD_HOST>:8443/?query=SELECT+currentUser()"
   ```

   The result must be `JWT::ci-test::<hash>`.

5. Add the private key as the repository secret `JWT_PKEY`:

   ```bash
   gh secret set JWT_PKEY --repo ClickHouse/clickhouse-cs < pkcs8_key.pem
   ```

6. Run the Cloud job and make sure that the `BearerAuthenticationTests` pass. Then delete
   `pkcs8_key.pem`, or keep it only in a secret store.

## Notes

- Keep `JWT_PKEY` as safe as `CLICKHOUSE_CLOUD_PASSWORD`. A person who has the key can sign a token
  with `clickhouse:grants` or `clickhouse:roles` claims, and Cloud gives those rights up to the
  provider's permission limit (the `default` user).
- The tokens that `generate_jwt.py` signs have no `clickhouse:grants` or `clickhouse:roles` claims,
  so their ephemeral user has no grants. The JWT tests only run queries that need no grants
  (`SELECT 1`, `SELECT currentUser()`).
- A token is valid for 30 minutes (`--ttl`). The Cloud job times out after 15 minutes.
- The `x5c` certificate that `generate_jwks.py` writes is valid for 365 days. To rotate the key, do
  the setup again with a new key.
- `requirements.txt` pins every package with hashes. To change a version, edit `requirements.in`
  and run the `uv pip compile` command at the top of `requirements.txt`.
