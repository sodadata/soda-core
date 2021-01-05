# SQL metrics

Soda will soon support user defined SQL metrics like et

```yaml
id: customers_with_expired_zip_code
name: Customers with expired zip code
type: failed_rows
sql: |

SELECT
  MD.id,
  MD.name,
  MD.zipcode,
FROM my_dataset as MD
  JOIN zip_codes as ZC on MD.zipcode = ZC.code
WHERE ZC.date_expired IS NOT NULL
```