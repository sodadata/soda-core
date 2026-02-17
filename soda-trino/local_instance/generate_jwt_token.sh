#!/bin/bash
jwt -sign - \
  -alg RS256 \
  -key jwt-private.pem <<'EOF'
{
  "sub": "test-user",
  "iss": "local",
  "aud": "trino"
}
EOF