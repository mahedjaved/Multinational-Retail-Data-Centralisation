-- First, create a new table with the desired data type
CREATE TABLE 
    new_dim_store_details AS
SELECT 
    CAST(longitude AS FLOAT) AS longitude,
    CAST(locality AS VARCHAR(255)) AS locality,
    CAST(store_code AS VARCHAR(12)) AS store_code,
    CAST(staff_numbers AS SMALLINT) AS staff_numbers,
    CAST(opening_date AS DATE) AS opening_date,
    CAST(store_type AS VARCHAR(255)) AS store_type,
    CAST(latitude AS FLOAT) AS latitude,
    CAST(country_code AS VARCHAR(2)) AS country_code,
    CAST(continent AS VARCHAR(255)) AS continent
FROM 
    dim_store_details;
DROP TABLE
    dim_store_details;
ALTER TABLE new_dim_store_details
    RENAME TO dim_store_details;