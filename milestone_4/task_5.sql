-- Finding percentage of sales per store type
SELECT  
    DSD.store_type
    ,ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2)  AS numbers_of_sales
    ,ROUND((ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2) / (
        SELECT  
            ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2)  AS numbers_of_sales
        FROM    
            orders_table AS OT
        INNER JOIN 
            dim_products AS DP
            ON OT.product_code = DP.product_code
        INNER JOIN
            dim_store_details AS DSD
            ON DSD.store_code = OT.store_code
        ) ) * 100::numeric, 2) AS "percentage_total (%)"
FROM    
    orders_table AS OT
INNER JOIN 
    dim_products AS DP
    ON OT.product_code = DP.product_code
INNER JOIN
    dim_store_details AS DSD
    ON DSD.store_code = OT.store_code
GROUP BY    
    DSD.store_type
ORDER BY
    SUM(DP."product_price (£)" * OT.product_quantity) DESC