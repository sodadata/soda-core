# Data source configuration reference

### Postgres

To configure a connection to a postgres database, use this YAML
```yaml
name: your_data_source_name
type: postgres
```

### Format regexes

In each data source, a list of regex formats can be configured.  In the contracts, 
the formats can be referred to by name and when parsed, the contract format names 
will be translated to the regex as configured in the data source.

For example the following regexes are the regexes as (hard coded) in 
the older version 3 of Soda Core:
```yaml
format_regexes:
  integer: ^ *[-+]? *[0-9]+ *$
  positive_integer: ^ *\+? *[0-9]+ *$
  negative_integer: ^ *(\- *[0-9]+|0+) *$
  decimal: ^ *[-+]? *(\d+([\.,]\d+)?|([\.,]\d+)) *$
  positive_decimal: ^ *\+? *(\d+([\.,]\d+)?|([\.,]\d+)) *$
  negative_decimal: ^ *(\- *(\d+([\.,]\d+)?|([\.,]\d+))|(0+([\.,]0+)?|([\.,]0+))) *$
  decimal_point: ^ *[-+]? *(\d+(\.\d+)?|(\.\d+)) *$
  positive_decimal_point: ^ *\+? *(\d+(\.\d+)?|(\.\d+)) *$
  negative_decimal_point: ^ *(\- *(\d+(\.\d+)?|(\.\d+))|(0+(\.0+)?|(\.0+))) *$
  decimal_comma: ^ *[-+]? *(\d+(,\d+)?|(,\d+)) *$
  positive_decimal_comma: ^ *\+? *(\d+(,\d+)?|(,\d+)) *$
  negative_decimal_comma: ^ *(\- *(\d+(,\d+)?|(,\d+))|(0+(,0+)?|(,0+))) *$
  percentage: ^ *[-+]? *(\d+([\.,]\d+)?|([\.,]\d+)) *% *$
  positive_percentage: ^ *\+? *(\d+([\.,]\d+)?|([\.,]\d+)) *% *$
  negative_percentage: ^ *(\- *(\d+([\.,]\d+)?|([\.,]\d+))|(0+([\.,]0+)?|([\.,]0+))) *% *$
  percentage_point: ^ *[-+]? *(\d+(\.\d+)?|(\.\d+)) *% *$
  positive_percentage_point: ^ *\+? *(\d+(\.\d+)?|(\.\d+)) *% *$
  negative_percentage_point: ^ *(\- *(\d+(\.\d+)?|(\.\d+))|(0+(\.0+)?|(\.0+))) *% *$
  percentage_comma: ^ *[-+]? *(\d+(,\d+)?|(,\d+)) *% *$
  positive_percentage_comma: ^ *\+? *(\d+(,\d+)?|(,\d+)) *% *$
  negative_percentage_comma: ^ *(\- *(\d+(,\d+)?|(,\d+))|(0+(,0+)?|(,0+))) *% *$
  money: ^ *[-+]? *(\d+([\.,]\d+)?|([\.,]\d+)) *([A-Z]{3}|[a-z]{3}) *$
  money_point: ^ *[-+]? *\d{1,3}(\,\d\d\d)*(.\d+)? *([A-Z]{3}|[a-z]{3}) *$
  money_comma: ^ *[-+]? *\d{1,3}(\.\d\d\d)*(,\d+)? *([A-Z]{3}|[a-z]{3}) *$
  date_us: ^ *(0?[1-9]|1[012])[-\./](0?[1-9]|[12][0-9]|3[01])[-\./]([12][0-9])?\d\d *$
  date_eu: ^ *(0?[1-9]|[12][0-9]|3[01])[-\./](0?[1-9]|1[012])[-\./]([12][0-9])?\d\d *$
  date_inverse: ^ *([12][0-9])?\d\d[-\./](0?[1-9]|1[012])[-\./](0?[1-9]|[12][0-9]|3[01]) *$
  date_iso 8601: ^ *[12][0-9]{3}-?((0[1-9]|1[012])-?(0[1-9]|[12][0-9]|3[01])|W[0-5]\d(-?[1-7])?|[0-3]\d\d)([ T](0[0-9]|1[012])(:?[0-5][0-9](:?[0-5][0-9]([.,]\d+)?)?)?([+-](0[0-9]|1[012]):?[0-5][0-9]|Z)?)? *$
  time_24h: ^ *(0?[0-9]|[01]\d|2[0-3]):[0-5]?[0-9](:[0-5]?[0-9]([.,]\d+)?)? *$
  time_24h nosec: ^ *(0?[0-9]|[01]\d|2[0-3]):[0-5]?[0-9] *$
  time_12h: ^ *(0?[0-9]|1[0-2]):[0-5]?[0-9](:[0-5]?[0-9]([.,]\d+)?)? *$
  time_12h nosec: ^ *(0?[0-9]|1[0-2]):[0-5]?[0-9] *$
  timestamp 24h: ^ *(0?[0-9]|[01]\d|2[0-3]):[0-5]?[0-9]:[0-5]?[0-9]([.,]\d+)? *$
  timestamp 12h: ^ *(0?[0-9]|1[0-2]):[0-5]?[0-9]:[0-5]?[0-9]([.,]\d+)? *$
  uuid: ^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}$
  ip_address: ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$
  ipv4_address: ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$
  ipv6_address: ^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$
  email: ^[a-zA-ZÀ-ÿĀ-ſƀ-ȳ0-9.\-_%+]+@[a-zA-ZÀ-ÿĀ-ſƀ-ȳ0-9.\-_%]+\.[A-Za-z]{2,4}$
  phone_number: ^((\+[0-9]{1,2}\s)?\(?[0-9]{3}\)?[\s.-])?[0-9]{3}[\s.-][0-9]{4}$
  credit_card_number: ^[0-9]{14}|[0-9]{15}|[0-9]{16}|[0-9]{17}|[0-9]{18}|[0-9]{19}|([0-9]{4}-){3}[0-9]{4}|([0-9]{4} ){3}[0-9]{4}$
