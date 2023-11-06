-- First, create a new table with the desired data type
CREATE TABLE 
    new_orders_table AS
SELECT 
    CAST(date_uuid AS UUID) AS date_uuid,
    CAST(user_uuid AS UUID) AS user_uuid,
    CAST(card_number AS VARCHAR(20)) AS card_number,
    CAST(store_code AS VARCHAR(12)) AS store_code,
    CAST(product_code AS VARCHAR(12)) AS product_code,
    CAST(product_quantity AS SMALLINT) AS product_quantity
FROM 
    orders_table;
DROP TABLE
    orders_table;
ALTER TABLE new_orders_table
    RENAME TO orders_table;


-- SELECT column_name,
--     data_type
-- FROM information_schema.columns
-- WHERE table_name = 'new_orders_table';