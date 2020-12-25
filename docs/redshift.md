# Redshift

## username / password authentication

`username`
`password`

## aws credentials authentication

`access_key_id`
`secret_access_key`
`session_token`
`region` (default `eu-west-1`)
`role_arn`

## Obtaining cluster credentials

This authentication mechanism is used if `username` is specified and `password` is not

optionally aws credentials as above

temp password is obtained with get_cluster_credentials: 
https://docs.aws.amazon.com/redshift/latest/APIReference/API_GetClusterCredentials.html
