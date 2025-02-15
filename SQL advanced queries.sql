-- Task 1.1
-- You’ve been tasked to create a detailed overview of all individual customers 
-- (these are defined by customerType = ‘I’ and/or stored in an individual table) - 
-- it is actually stored in Customer table.
-- Write a query that provides:

-- Identity information : CustomerId, Firstname, Last Name, FullName (First Name & Last Name).
-- An Extra column called addressing_title i.e. (Mr. Achong), if the title is missing - Dear Achong.
-- Contact information : Email, phone, account number, CustomerType.
-- Location information : City, State & Country, address.
-- Sales: number of orders, total amount (with Tax), date of the last order.

-- COMMENTS
--I identified all infromation required, creating addressing title using CONCAT and COALESCE (in case of NULL) 
--the same as FullName. I used window functions for number of orders, total amount and last order date (MAX), 
--partitioning by customerID. Then Joinig a lot of tables together. Limiting result by 200 rows.

SELECT
  DISTINCT customer.CustomerId,
  contact.Firstname,
  contact.Lastname,
  CONCAT ((COALESCE (contact.Title, 'Dear')), ' ', contact.Lastname) AS addressing_title,
  CONCAT (contact.FirstName, ' ', contact.Lastname) AS FullName,
  contact.EmailAddress,
  contact.Phone,
  customer.AccountNumber,
  customer.CustomerType,
  MAX(customeraddress.AddressID) OVER (PARTITION BY customeraddress.CustomerID) AS address_ID,
  address.City,
  address.AddressLine1,
  address.Addressline2,
  stateprovince.name AS state,
  countryregion.name AS country,
  COUNT(*) OVER (PARTITION BY salesorderheader.CustomerID) AS number_of_orders,
  SUM(salesorderheader.TotalDue) OVER (PARTITION BY customer.CustomerID) AS total_amount,
  MAX(salesorderheader.OrderDate) OVER (PARTITION BY salesorderheader.CustomerID) AS last_order_date
FROM
  adwentureworks_db.individual
JOIN
  adwentureworks_db.customer
ON
  individual.CustomerID = customer.CustomerID
JOIN
  adwentureworks_db.contact
ON
  individual.ContactID = contact.ContactID
JOIN
  adwentureworks_db.customeraddress
ON
  customer.CustomerID = customeraddress.CustomerID
JOIN
  adwentureworks_db.address
ON
  customeraddress.AddressID = address.AddressID
JOIN
  adwentureworks_db.stateprovince
ON
  address.StateProvinceID = stateprovince.StateProvinceID
JOIN
  adwentureworks_db.countryregion
ON
  stateprovince.CountryRegionCode = countryregion.CountryRegionCode
JOIN
  adwentureworks_db.salesorderheader
ON
  customer.CustomerID = salesorderheader.CustomerID
WHERE
  customer.CustomerType = 'I'
ORDER BY
  total_amount
LIMIT 200;




	  
 --Task 1.2
 --Business finds the original query valuable to analyze customers and now want to get the 
 --data from the first query for the top 200 customers with the highest total amount (with tax) 
 --who have not ordered for the last 365 days. How would you identify this segment?
-- Hints:

-- You can use temp table, cte and/or subquery of the 1.1 select.
-- Note that the database is old and the current date should be defined by 
--finding the latest order date in the orders table.
 
--COMMENTS.
--I created CTE from table in task 1.1. Only one thing I added - I distinguished last day of order - in general (from all orders) 
--but also identified last day of ordder for each customer. Then I created a new query referencing my CTE, where
--I selected 200 customers with highest (total amount DESC) total amount who have not ordered in last 365 days 
--using WHERE clause and DATE_SUB function
--

WITH
  table_1 AS (
  SELECT
    DISTINCT customer.CustomerID,
    contact.Firstname,
    contact.Lastname,
    CONCAT ((COALESCE (contact.Title, 'Dear')), ' ', contact.Lastname) AS addressing_title,
    CONCAT (contact.FirstName, ' ', contact.Lastname) AS FullName,
    contact.EmailAddress,
    contact.Phone,
    customer.AccountNumber,
    customer.CustomerType,
    MAX(customeraddress.AddressID) OVER (PARTITION BY customeraddress.CustomerID) AS address_ID,
    address.City,
    address.AddressLine1,
    address.Addressline2,
    stateprovince.name AS state,
    countryregion.name AS country,
    COUNT(*) OVER (PARTITION BY salesorderheader.CustomerID) AS number_of_orders,
    SUM(salesorderheader.TotalDue) OVER (PARTITION BY customer.CustomerID) AS total_amount,
    MAX(salesorderheader.OrderDate) OVER () AS general_last_order_date,
    MAX (salesorderheader.OrderDate) OVER (PARTITION BY customer.CustomerID) AS ind_last_order_date
  FROM
    adwentureworks_db.individual
  JOIN
    adwentureworks_db.customer
  ON
    individual.CustomerID = customer.CustomerID
  JOIN
    adwentureworks_db.contact
  ON
    individual.ContactID = contact.ContactID
  JOIN
    adwentureworks_db.customeraddress
  ON
    customer.CustomerID = customeraddress.CustomerID
  JOIN
    adwentureworks_db.address
  ON
    customeraddress.AddressID = address.AddressID
  JOIN
    adwentureworks_db.stateprovince
  ON
    address.StateProvinceID = stateprovince.StateProvinceID
  JOIN
    adwentureworks_db.countryregion
  ON
    stateprovince.CountryRegionCode = countryregion.CountryRegionCode
  JOIN
    adwentureworks_db.salesorderheader
  ON
    customer.CustomerID = salesorderheader.CustomerID
  WHERE
    customer.CustomerType = 'I')
SELECT
  CustomerID,
  Firstname,
  Lastname,
  addressing_title,
  FullName,
  EmailAddress,
  Phone,
  AccountNumber,
  CustomerType,
  address_ID,
  City,
  AddressLine1,
  Addressline2,
  number_of_orders,
  total_amount,
  ind_last_order_date
FROM
  table_1
WHERE
  ind_last_order_date < DATE_SUB(general_last_order_date, INTERVAL 365 DAY)
ORDER BY
  total_amount DESC
LIMIT
  200;
	 
	 
	 

-- Task 1.3
-- Enrich your original 1.1 SELECT by creating a new column in the view that marks 
-- active & inactive customers based on whether they have ordered anything during the last 365 days.
-- Copy only the top 500 rows from your written select ordered by CustomerId desc.

-- COMMENTS:
-- In order to distinguish active vs non active customers I implemented CASE statement.
	
WITH table_1 AS (
  SELECT
    DISTINCT customer.CustomerID,
    contact.Firstname,
    contact.Lastname,
    CONCAT ((COALESCE (contact.Title, 'Dear')), ' ', contact.Lastname) AS addressing_title,
    CONCAT (contact.FirstName, ' ', contact.Lastname) AS FullName,
    contact.EmailAddress,
    contact.Phone,
    customer.AccountNumber,
    customer.CustomerType,
    MAX(customeraddress.AddressID) OVER (PARTITION BY customeraddress.CustomerID) AS address_ID,
    address.City,
    address.AddressLine1,
    address.Addressline2,
    stateprovince.name AS state,
    countryregion.name AS country,
    COUNT(*) OVER (PARTITION BY salesorderheader.CustomerID) AS number_of_orders,
    SUM(salesorderheader.TotalDue) OVER (PARTITION BY customer.CustomerID) AS total_amount,
    MAX(salesorderheader.OrderDate) OVER () AS general_last_order_date,
    MAX (salesorderheader.OrderDate) OVER (PARTITION BY customer.CustomerID) AS ind_last_order_date
  FROM
    adwentureworks_db.individual
  JOIN
    adwentureworks_db.customer
  ON
    individual.CustomerID = customer.CustomerID
  JOIN
    adwentureworks_db.contact
  ON
    individual.ContactID = contact.ContactID
  JOIN
    adwentureworks_db.customeraddress
  ON
    customer.CustomerID = customeraddress.CustomerID
  JOIN
    adwentureworks_db.address
  ON
    customeraddress.AddressID = address.AddressID
  JOIN
    adwentureworks_db.stateprovince
  ON
    address.StateProvinceID = stateprovince.StateProvinceID
  JOIN
    adwentureworks_db.countryregion
  ON
    stateprovince.CountryRegionCode = countryregion.CountryRegionCode
  JOIN
    adwentureworks_db.salesorderheader
  ON
    customer.CustomerID = salesorderheader.CustomerID
  WHERE
    customer.CustomerType = 'I')


SELECT
  CustomerID,
  Firstname,
  Lastname,
  addressing_title,
  FullName,
  EmailAddress,
  Phone,
  AccountNumber,
  CustomerType,
  address_ID,
  City,
  AddressLine1,
  Addressline2,
  number_of_orders,
  total_amount,
  ind_last_order_date,
  CASE
    WHEN ind_last_order_date < DATE_SUB(general_last_order_date, INTERVAL 365 DAY) THEN 'Inactive'
    ELSE 'Active'
  END AS Status
FROM
  table_1
ORDER BY
  CustomerID DESC
LIMIT
  500;
  
  
  
  
-- Task 1.4 
-- Business would like to extract data on all active customers from North America. 
-- Only customers that have either ordered no less than 2500 in total amount (with Tax) or ordered 5 + times should be presented.
-- In the output for these customers divide their address line into two columns
-- Order the output by country, state and date_last_order.

--COMMENTS:
--In order to divide the address column I used REGEXP_EXTRACT and REGEXP_REPLACE functions.
--To filter on the rest of conditions WHERE clause was used.

	
	 WITH table_1 AS (
    SELECT
    DISTINCT customer.CustomerID,
    contact.Firstname,
    contact.Lastname,
    CONCAT ((COALESCE (contact.Title, 'Dear')), ' ', contact.Lastname) AS addressing_title,
    CONCAT (contact.FirstName, ' ', contact.Lastname) AS FullName,
    contact.EmailAddress,
    contact.Phone,
    customer.AccountNumber,
    customer.CustomerType,
    MAX(customeraddress.AddressID) OVER (PARTITION BY customeraddress.CustomerID) AS address_ID,
    address.City,
    address.AddressLine1,
    address.Addressline2,
    stateprovince.name AS state,
    countryregion.name AS country,
    salesterritory.group AS country_group,
    COUNT(*) OVER (PARTITION BY salesorderheader.CustomerID) AS number_of_orders,
    SUM(salesorderheader.TotalDue) OVER (PARTITION BY customer.CustomerID) AS total_amount,
    MAX(salesorderheader.OrderDate) OVER () AS general_last_order_date,
    MAX (salesorderheader.OrderDate) OVER (PARTITION BY customer.CustomerID) AS ind_last_order_date,
    CASE
    WHEN (MAX (salesorderheader.OrderDate) OVER (PARTITION BY customer.CustomerID)) < DATE_SUB((MAX(salesorderheader.OrderDate) OVER ()), INTERVAL 365 DAY) THEN 'Inactive'
    ELSE 'Active'
  END AS Status
  FROM
    adwentureworks_db.individual
  JOIN
    adwentureworks_db.customer
  ON
    individual.CustomerID = customer.CustomerID
  JOIN
    adwentureworks_db.contact
  ON
    individual.ContactID = contact.ContactID
  JOIN
    adwentureworks_db.customeraddress
  ON
    customer.CustomerID = customeraddress.CustomerID
  JOIN
    adwentureworks_db.address
  ON
    customeraddress.AddressID = address.AddressID
  JOIN
    adwentureworks_db.stateprovince
  ON
    address.StateProvinceID = stateprovince.StateProvinceID
  JOIN
    adwentureworks_db.countryregion
  ON
    stateprovince.CountryRegionCode = countryregion.CountryRegionCode
  JOIN
    adwentureworks_db.salesorderheader
  ON
    customer.CustomerID = salesorderheader.CustomerID
  JOIN
    adwentureworks_db.salesterritory
  ON 
    salesorderheader.territoryID = salesterritory.territoryID
  WHERE
    customer.CustomerType = 'I')

  SELECT
  CustomerID,
  Firstname,
  Lastname,
  addressing_title,
  FullName,
  EmailAddress,
  Phone,
  AccountNumber,
  CustomerType,
  address_ID,
  City,
  country,
  state,
  country_group,
  REGEXP_EXTRACT(AddressLine1, r"^\d+") AS address_no,
  REGEXP_REPLACE(AddressLine1, r"^\d+\s+", "") AS Address_st,
  Addressline2,
  number_of_orders,
  total_amount,
  ind_last_order_date,
  Status
FROM
  table_1
WHERE country_group = 'North America'
  AND Status = 'Active'
  AND (total_amount >= 2500 OR number_of_orders >= 5)
ORDER BY
  country,state, ind_last_order_date;
  
  
  
  
  
-- Task 2.1 
-- Create a query of monthly sales numbers in each Country & region.
-- Include in the query a number of orders, customers and sales persons in each month with a total amount with tax earned. 
-- Sales numbers from all types of customers are required.

--COMMENTS:
  --I used LAST_DAY function to get the last day of each month togethter (monthly data)
  -- with CAST function (to turn TIMESTAMP value of OrderDate column into DATE value)
  -- I used aggregated functions COUNT (added Distinct to avoid nulls) and SUM to
  -- calculate number of orders, customers, sales people and total amount.
  -- GROUP BY by non-aggregated columns at the end.

SELECT
  DATE(LAST_DAY(CAST(OrderDate AS DATE))) AS order_month,
  salesterritory.CountryRegionCode as CountryRegionCode,
  salesterritory.Name AS Region,
  COUNT(salesorderheader.SalesOrderID) AS number_orders,
  COUNT(DISTINCT salesorderheader.customerID) AS number_customers,
  COUNT(DISTINCT salesorderheader.SalesPersonID) AS no_salesPersons,
  SUM(ROUND (salesorderheader.TotalDue, 0)) AS Total_W_tax
FROM
  adwentureworks_db.salesorderheader
JOIN
  adwentureworks_db.salesterritory
ON
  salesorderheader.TerritoryID = salesterritory.TerritoryID
GROUP BY
  order_month,
  CountryRegionCode,
  Region;
  
  
  
  
-- Task 2.2
-- Enrich 2.1 query with the cumulative_sum of
-- the total amount with tax earned per country & region.
-- Hint: use CTE or subquery.

-- COMMENTS:
-- I created CTE for the derived table of task 2.1 and called it table_2.
-- In new query I added running totals using window function,
-- where I paritioned byt country and region and ordered by month (to ensure 
-- we have cumulative sum/amount with each month). I implemented CAST
-- function to get integer with no decimals for the total amount with tax 
-- (I've noticed it in the hint provided)

WITH
  table_2 AS (
  SELECT
    DATE(LAST_DAY(CAST(OrderDate AS DATE))) AS order_month,
    salesterritory.CountryRegionCode AS CountryRegionCode,
    salesterritory.Name AS Region,
    COUNT(salesorderheader.SalesOrderID) AS number_orders,
    COUNT(DISTINCT salesorderheader.customerID) AS number_customers,
    COUNT(DISTINCT salesorderheader.SalesPersonID) AS no_salesPersons,
    SUM (salesorderheader.TotalDue) AS total_w_tax
  FROM
    adwentureworks_db.salesorderheader
  JOIN
    adwentureworks_db.salesterritory
  ON
    salesorderheader.TerritoryID = salesterritory.TerritoryID
  GROUP BY
    order_month,
    CountryRegionCode,
    Region)
SELECT
  order_month,
  CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  CAST (total_w_tax AS integer) AS total_w_tax,
  SUM (CAST (total_w_tax AS integer)) OVER (PARTITION BY CountryRegionCode, region ORDER BY order_month) AS cumulative_sum
FROM
  table_2
ORDER BY
  CountryRegionCode,
  Region,
  order_month;
  
  
  
  
  
-- Task 2.3 
-- Enrich 2.2 query by adding ‘sales_rank’ column that ranks rows
-- from best to worst for each country based on total amount with tax earned 
-- each month. I.e. the month where the (US, Southwest) region made the highest
-- total amount with tax earned will be ranked 1 for that region and vice versa.

-- COMMENTS:
-- I used RANK window function, partitioned by country, region (we need 
-- best and worst sales for each country and then ORDER BY total amount DESC -
-- the highest to lowest, so the highest ranked 1 etc.) 


WITH
  table_2 AS (
  SELECT
    DATE(LAST_DAY(CAST(OrderDate AS DATE))) AS order_month,
    salesterritory.CountryRegionCode AS CountryRegionCode,
    salesterritory.Name AS Region,
    COUNT(salesorderheader.SalesOrderID) AS number_orders,
    COUNT(DISTINCT salesorderheader.customerID) AS number_customers,
    COUNT(DISTINCT salesorderheader.SalesPersonID) AS no_salesPersons,
    SUM (salesorderheader.TotalDue) AS total_w_tax
  FROM
    adwentureworks_db.salesorderheader
  JOIN
    adwentureworks_db.salesterritory
  ON
    salesorderheader.TerritoryID = salesterritory.TerritoryID
  GROUP BY
    order_month,
    CountryRegionCode,
    Region)

SELECT
  order_month,
  CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  CAST (total_w_tax AS integer) AS total_w_tax,
  RANK() OVER (PARTITION BY CountryRegionCode, Region ORDER BY total_w_tax DESC) AS country_sales_rank,
  SUM (CAST (total_w_tax AS integer)) OVER (PARTITION BY CountryRegionCode, region ORDER BY order_month) AS cumulative_sum
FROM
  table_2
ORDER BY
  CountryRegionCode,
  Region,
  country_sales_rank;
  
  
  


-- Task 2.4
-- Enrich 2.3 query by adding taxes on a country level:
-- As taxes can vary in country based on province, the needed column is ‘mean_tax_rate’ -> average tax rate in a country.
-- Also, as not all regions have data on taxes, you also want to be transparent and show the ‘perc_provinces_w_tax’ -> 
-- a column representing the percentage of provinces with available tax rates for each country 
-- (i.e. If US has 53 provinces, and 10 of them have tax rates, then for US it should show 0,19)
-- Hint: If a state has multiple tax rates, choose the higher one. Do not double count a state in country average rate calculation if it has multiple tax rates.
-- Hint: Ignore the isonlystateprovinceFlag rate mechanic, it is beyond the scope of this exercise. Treat all tax rates as equal.

--COMMENTS:
--I created one more CTE to calculate percentage of provinces with available tax rates (table_3).
-- Then in my last query I joined this 2 CTE together to created a table with additional 
-- columns for AVG tax rate and percentage of provinces with tax. I do not really know 
-- how to identify higher tax rates for states with multiple tax, I tried HAVING function etc.
-- but really could not implement them into my code, therefore my results are different
-- from the hint provided. Also I had some duplicate data in my resulting table,
-- after checking everything I am not sure where these duplicates come from.
-- But I really tried:))) and did the best I could.

WITH
  table_2 AS (
  SELECT
    DISTINCT DATE(LAST_DAY(CAST(OrderDate AS DATE))) AS order_month,
    salesterritory.CountryRegionCode AS CountryRegionCode,
    salesterritory.Name AS Region,
    COUNT(DISTINCT salesorderheader.SalesOrderID) AS number_orders,
    COUNT(DISTINCT salesorderheader.customerID) AS number_customers,
    COUNT(DISTINCT salesorderheader.SalesPersonID) AS no_salesPersons,
    SUM (salesorderheader.TotalDue) AS total_w_tax,
    salestaxrate.TaxRate AS tax_rate

  FROM
    adwentureworks_db.salesorderheader
  JOIN
    adwentureworks_db.salesterritory
  ON
    salesorderheader.TerritoryID = salesterritory.TerritoryID
  JOIN
    adwentureworks_db.stateprovince
  ON
    salesterritory.CountryRegionCode = stateprovince.CountryRegionCode
    AND salesterritory.TerritoryID = stateprovince.TerritoryID
  JOIN
    adwentureworks_db.salestaxrate
  ON
    stateprovince.StateProvinceID = salestaxrate.StateProvinceID

  GROUP BY
    CountryRegionCode,
    Region,
    order_month,
    tax_rate),

  table_3 AS (
  SELECT
    DISTINCT salesterritory.CountryRegionCode,
    COUNT (DISTINCT salestaxrate.StateProvinceID) AS province_with_tax,
    COUNT (DISTINCT stateprovince.StateProvinceID) AS province,
    ROUND (COUNT (DISTINCT salestaxrate.StateProvinceID) / COUNT (DISTINCT stateprovince.StateProvinceID),2) AS perc_provinces_w_tax
  FROM
    adwentureworks_db.salestaxrate
  RIGHT JOIN
    adwentureworks_db.stateprovince
  ON
    salestaxrate.StateProvinceID = stateprovince.StateProvinceID
  JOIN
    adwentureworks_db.salesterritory
  ON
    salesterritory.TerritoryID = stateprovince.TerritoryID
    AND salesterritory.CountryRegionCode = stateprovince.CountryRegionCode
  GROUP BY
    salesterritory.CountryRegionCode)
    
SELECT
  DISTINCT order_month,
  table_2.CountryRegionCode AS CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  CAST (total_w_tax AS integer) AS total_w_tax,
  DENSE_RANK() OVER (PARTITION BY table_2.CountryRegionCode, Region ORDER BY total_w_tax DESC) AS country_sales_rank,
  SUM (CAST (total_w_tax AS integer)) OVER (PARTITION BY table_2.CountryRegionCode, Region ORDER BY order_month) AS cumulative_sum,
  AVG (tax_rate) OVER (PARTITION BY table_2.CountryRegionCode) AS mean_tax_rate,
  perc_provinces_w_tax
FROM
  table_2
JOIN
  table_3
ON
  table_2.CountryRegionCode = table_3.CountryRegionCode
ORDER BY
  table_2.CountryRegionCode,
  Region,
  country_sales_rank;