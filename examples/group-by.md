# Group check results by category with Soda Core

You can use a SQL query in a failed row check to group failed check results by one or more categories using Soda Core. 

Use a SQL editor to build and test a SQL query with your data source, then add the query to a failed rows check to execute it during a Soda scan.

The following example illustrates how to build a query that identifies the countries where the average age of people is less than 25.

1. Begining with a basic query, the output shows the data this example works with.
```sql
SELECT * FROM Customers;
```
![group-by-1](/docs/assets/images/group-by-1.png){:height="600px" width="600px"}
2. Build a query to select groups with the relevant aggregations.
```sql
SELECT country, AVG(age) as avg_age
FROM Customers
GROUP BY country
```
![group-by-2](/docs/assets/images/group-by-2.png){:height="600px" width="600px"}
3. Identify the "bad" group (where the average age is less than 25) from among the grouped results.
```sql
	SELECT country, AVG(age) as avg_age
	    FROM Customers
	    GROUP BY country
    	    HAVING AVG(age) < 25
```
![group-by-3](/docs/assets/images/group-by-3.png){:height="600px" width="600px"}
4. Now that the query yields the expected results, add the query to a failed row check, as per the following example.
```yaml
checks for dim_customers:
  - failed rows:
      name: Average age of citizens is less than 25
      fail query: |
	    SELECT country, AVG(age) as avg_age
	        FROM Customers
	        GROUP BY country
          	HAVING AVG(age) < 25
```