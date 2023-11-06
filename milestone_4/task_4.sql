-- Finding which months produce the highest cost of sales typically
SELECT  
    ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2)  AS numbers_of_sales
    ,ROUND(SUM(OT.product_quantity)::numeric, 2) AS product_quantity_count
    ,CASE
        WHEN TRUE THEN 'Web'
        ELSE 'Web'
    END AS store_type
FROM    
    orders_table AS OT
LEFT JOIN 
    dim_products AS DP
    ON OT.product_code = DP.product_code
LEFT JOIN
    dim_store_details AS DSD
    ON DSD.store_code = OT.store_code
WHERE
    DSD.store_type LIKE '%Web Portal%'
    --AND DP.still_available = FALSE
GROUP BY    
    DSD.store_type


UNION 


SELECT  
    SUM(SUB1.numbers_of_sales)
    ,SUM(SUB1.product_quantity_count)
    , CASE
        WHEN TRUE THEN 'Offline'
        ELSE 'Offline'
    END AS store_type
FROM
    (
        SELECT  
            ROUND(SUM(DP."product_price (£)" * OT.product_quantity)::numeric, 2)  AS numbers_of_sales
            ,ROUND(SUM(OT.product_quantity)::numeric, 2) AS product_quantity_count
            ,DSD.store_type
        FROM    
            orders_table AS OT
        LEFT JOIN 
            dim_products AS DP
            ON OT.product_code = DP.product_code
        LEFT JOIN
            dim_store_details AS DSD
            ON DSD.store_code = OT.store_code
        WHERE
            DSD.store_type NOT LIKE '%Web Portal%'
            --AND DP.still_available = FALSE
        GROUP BY    
            DSD.store_type
    ) AS SUB1