# JWT authentication tests: setup

The JWT tests run against ClickHouse Cloud, in the Cloud jobs of both clients:

| Client | Tests | Job |
|---|---|---|
| HTTP | `ClickHouse.Driver.Tests/ADO/BearerAuthenticationTests.cs` | `.github/workflows/tests-cloud.yml` |
| TCP | `ClickHouse.Driver.Tcp.Tests/Integration/BearerAuthenticationIntegrationTests.cs` | `.github/workflows/tests-cloud-tcp.yml` |

Each job signs two new tokens for each run with `jwt.cs`: a valid one in the
`CLICKHOUSE_CLOUD_JWT` environment variable, and one that expired 5 minutes ago (`--expired`) in
`CLICKHOUSE_CLOUD_JWT_EXPIRED`. The token refresh tests give the expired token first, and check that
the client asks its credentials provider again after the rejection. If the `JWT_PKEY` secret is not
set, the job shows a warning and the JWT tests are skipped. A TCP run that the `tests:run-cloud`
label starts does not get `JWT_PKEY`, so its JWT tests are skipped.

The parts:

| Part | Where | Secret? |
|---|---|---|
| RSA private key (PEM) | repository secret `JWT_PKEY` | yes |
| Public key set | `.github/jwks.json` in this repository (step 2 adds it) | no |
| JWT provider | the Cloud service of the `CLICKHOUSE_CLOUD_HOST` secret | no |

The provider and the token must agree on these claims (the defaults of `jwt.cs`):

| Claim | Value |
|---|---|
| `iss` (Issuer) | `mydomain.com` |
| `aud` (Audience) | `ci-test-service` |
| `sub` | `ci-test` |

These are the values that the clickhouse-java tests use, so one provider can serve both
repositories if they use the same Cloud service and key.

## Setup

Do these steps outside of the repository directory, so that the private key cannot be committed.
Add the secret last (step 5): when the secret is set, the Cloud jobs use it, and the JWT tests fail
until the provider accepts the tokens.

1. Make the private key:

   ```bash
   openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out pkcs8_key.pem
   ```

2. Make `jwks.json`. `jwt.cs` needs the .NET 10 SDK.

   ```bash
   export JWT_PKEY="$(cat pkcs8_key.pem)"
   dotnet run <repo>/.github/scripts/jwt.cs -- jwks > <repo>/.github/jwks.json
   ```

   Commit and push `.github/jwks.json`. It contains only the public key.

3. In the ClickHouse Cloud console, open the service of the `CLICKHOUSE_CLOUD_HOST` secret. Go to
   **Settings** > **Security** > **JWT authentication** > **Set up JWT providers**, and add a
   provider:

   - **Name**: `dotnet-driver-ci` (any name; you cannot change it later). The name must not start
     with a reserved prefix: `cloud`, `clickhouse`, `ch`, `internal`, `system` or `default`.
   - **Issuer**: `mydomain.com`
   - **Audience**: `ci-test-service`
   - **JWKS URL**: `https://raw.githubusercontent.com/ClickHouse/clickhouse-cs/main/.github/jwks.json`
     (before the file is on `main`, use the branch name instead of `main`, and change the URL
     after the merge)
   - **Roles claim**: empty

4. Make sure that the service accepts a token:

   ```bash
   TOKEN="$(dotnet run <repo>/.github/scripts/jwt.cs -- token)"
   curl -H "Authorization: Bearer $TOKEN" \
     "https://<CLICKHOUSE_CLOUD_HOST>:8443/?query=SELECT+currentUser()"
   ```

   The result must be `JWT::ci-test::<hash>`.

5. Add the private key as the repository secret `JWT_PKEY`:

   ```bash
   gh secret set JWT_PKEY --repo ClickHouse/clickhouse-cs < pkcs8_key.pem
   ```

6. Run the Cloud jobs and make sure that the JWT tests pass. Then delete `pkcs8_key.pem`, or keep
   it only in a secret store.

## Notes

- Keep `JWT_PKEY` as safe as `CLICKHOUSE_CLOUD_PASSWORD`. A person who has the key can sign a token
  with `clickhouse:grants` or `clickhouse:roles` claims, and Cloud gives those rights up to the
  provider's permission limit (the `default` user).
- The tokens that `jwt.cs` signs have no `clickhouse:grants` or `clickhouse:roles` claims,
  so their ephemeral user has no grants. The JWT tests only run queries that need no grants
  (`SELECT 1`, `SELECT currentUser()`).
- A token is valid for 30 minutes (`--ttl`). The HTTP Cloud job times out after 15 minutes. The TCP
  Cloud job times out after 30 minutes, so it signs its valid token with `--ttl 2700` (45 minutes).
- The `x5c` certificate that `jwt.cs jwks` writes is valid for 365 days. To rotate the key, do
  the setup again with a new key.
- `jwt.cs` is a .NET file-based app. It uses only the .NET base class library, so it has no
  packages to pin.
