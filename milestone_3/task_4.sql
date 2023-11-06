-- The team that handles the deliveries would like a new human-readable column added for the weight so they can quickly make decisions on delivery weights.

-- Add a new column weight_class which will contain human-readable values based on the weight range of the product.

CREATE TABLE 
    new_dim_products AS
SELECT 
    index
    ,product_name
    ,"product_price (£)"
    ,"weight (kg)"
    ,CASE 
        WHEN "weight (kg)" >= 2 AND "weight (kg)" < 40 THEN 'Mid_Sized'
        WHEN "weight (kg)" >= 40 AND "weight (kg)" < 140 THEN 'Heavy'
        WHEN "weight (kg)" >= 140 THEN 'Truck_Required'
        ELSE 'Light'
    
    END AS weight_class
    ,category
    ,"EAN"
    ,date_added
    ,uuid
    ,removed
    ,product_code
FROM 
    dim_products;
DROP TABLE
    dim_products;
ALTER TABLE new_dim_products
    RENAME TO dim_products;


