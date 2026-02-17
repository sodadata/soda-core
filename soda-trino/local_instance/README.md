# Trino JWT Testing

Use the `docker-compose.yml` and associated config files to launch a local Trino instance configured for JWT authentication and username/password.  The existing password.db has one entry, username `soda-test`, password `soda-test`.

To generate the keys, run from within the `local_instance` directory:

```
./generate_keys.sh
```

To start Trino:

```
docker compose up
```
If the following runs without error, your instance is up
```
curl -k https://localhost:8443/v1/statement -u soda-test:soda-test -d 
"SELECT 1" 
```




To generate a JWT token:

```
./generate_jwt_token.sh
```

If the following runs without error, your token is valid:
```
curl -k https://localhost:8443/v1/query   -H "Authorization: Bearer {token}"
```


Copy the token into your .env file along with these env vars
```
TRINO_HOST="localhost"
TRINO_PORT=8443
TRINO_CATALOG="system"
TRINO_JWT_TOKEN="{token}"
```

Uncomment and run the `real_jwt_token` test in `test_trino.py`.  The test should pass.
