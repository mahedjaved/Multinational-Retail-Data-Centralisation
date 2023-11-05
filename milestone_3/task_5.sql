-- After all the columns are created and cleaned, change the data types of the products table.
-- You will want to rename the removed column to still_available before changing its data type.
-- Make the changes to the columns to cast them to the following data types:

CREATE TABLE 
    new_dim_products AS
SELECT
    CAST(index AS SMALLINT)
    ,CAST(product_name AS VARCHAR(255)) AS product_name
    ,CAST("product_price (£)" AS FLOAT) AS "product_price (£)"
    ,CAST("weight (kg)" AS FLOAT) AS "weight (kg)"
    ,CAST(category AS VARCHAR(255)) AS category
    ,CAST(weight_class AS VARCHAR(15))
    ,CAST("EAN" AS VARCHAR(15)) AS "EAN"
    ,CAST(date_added AS DATE) AS date_added
    ,CAST(uuid AS UUID) AS uuid
    ,CASE 
        WHEN "removed"='Still_avaliable' THEN TRUE
        ELSE FALSE
    END AS "still_available"
    ,CAST(product_code AS VARCHAR(12)) AS product_code
FROM 
    dim_products
DROP TABLE dim_products;
ALTER TABLE new_dim_products
    RENAME TO dim_products;
