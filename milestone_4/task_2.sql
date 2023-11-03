SELECT 
    locality, COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY
    locality
ORDER BY
    COUNT(*) DESC
LIMIT
    (7)


