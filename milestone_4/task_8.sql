-- finding stores with highest sales in Germany
SELECT  
    ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2)  AS numbers_of_sales
    ,DSD.store_type
    ,DSD.country_code
FROM    
    orders_table AS OT
INNER JOIN 
    dim_products AS DP
    ON OT.product_code = DP.product_code
INNER JOIN
    dim_store_details AS DSD
    ON DSD.store_code = OT.store_code
WHERE
    DSD.country_code LIKE 'DE'
GROUP BY    
    DSD.store_type
    ,DSD.country_code
ORDER BY
    SUM(DP."product_price (£)" * OT.product_quantity) ASC