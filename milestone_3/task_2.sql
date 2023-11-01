-- First, create a new table with the desired data type
CREATE TABLE 
    new_dim_users AS
SELECT 
    CAST(first_name AS VARCHAR(255)) AS first_name,
    CAST(last_name AS VARCHAR(255)) AS last_name,
    CAST(date_of_birth AS DATE) AS date_of_birth,
    CAST(country_code AS VARCHAR(2)) AS country_code,
    CAST(user_uuid AS UUID) AS user_uuid,
    CAST(join_date AS DATE) AS join_date
FROM 
    dim_users;
DROP TABLE
    dim_users;
ALTER TABLE new_dim_users
    RENAME TO dim_users;

