-- Finding which months in each year produce the highest cost of sales 
SELECT  
    ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2)  AS total_sales
    ,DDT.year
    ,DDT.month
FROM    
    orders_table AS OT
LEFT JOIN dim_products AS DP
    ON OT.product_code = DP.product_code
LEFT JOIN dim_date_times AS DDT
    ON OT.date_uuid = DDT.date_uuid
GROUP BY    
    DDT.month
    ,DDT.year
ORDER BY
    ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2)  DESC
LIMIT   
    (10)