# Contract check identities

TODO explain a bit better:

Every check in a contract has an identity.  

* why uniqueness required
* relation to schedule
* used in correlation for sodacl
* used in correlation on soda cloud

format:
`//{schedule}/{dataset}/{column}/{check_type}/{identity_suffix}`

One-way: A check identity is a composite key.  But we don't expect we ever need to decompose a check identity into it's parts.
