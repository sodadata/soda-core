# Trino JWT Testing

Use the `docker-compose.yml` and associated config files to launch a local Trino instance configured for JWT authentication.  

To generate the keys, run from within the `local_instance` directory:
                                 
```
openssl genrsa -out jwt-private.pem 2048  
openssl rsa -in jwt-private.pem -pubout -out jwt-public.pem
keytool -genkeypair -alias trino -keyalg RSA -keystore trino-config/keystore.jks \
-storepass changeit -keypass changeit -dname "CN=localhost" -validity 365
cp jwt-public.pem trino-config/
```

To start Trino:

```
docker compose up
```
If the following runs without error, your instance is up
```
curl -k https://localhost:8443/v1/info "                                               
```




To generate a JWT token:

```
jwt -sign - \
  -alg RS256 \
  -key jwt-private.pem <<'EOF'
{
  "sub": "test-user",
  "iss": "local",
  "aud": "trino"
}
EOF
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