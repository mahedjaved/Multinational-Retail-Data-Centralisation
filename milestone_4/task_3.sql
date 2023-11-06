-- Finding which months produce the highest cost of sales typically
SELECT  
    ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2)  AS purchases
    ,DDT.month
FROM    
    orders_table AS OT
LEFT JOIN dim_products AS DP
    ON OT.product_code = DP.product_code
LEFT JOIN dim_date_times AS DDT
    ON OT.date_uuid = DDT.date_uuid
-- WHERE
--     DP.still_available = FALSE
GROUP BY    
    DDT.month
ORDER BY
    SUM(DP."product_price (£)" * OT.product_quantity)  DESC
LIMIT   
    (6)