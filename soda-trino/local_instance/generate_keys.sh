#!/bin/bash
openssl genrsa -out jwt-private.pem 2048
openssl rsa -in jwt-private.pem -pubout -out jwt-public.pem
keytool -genkeypair -alias trino -keyalg RSA -keystore trino-config/keystore.jks \
-storepass changeit -keypass changeit -dname "CN=localhost" -validity 365
cp jwt-public.pem trino-config/