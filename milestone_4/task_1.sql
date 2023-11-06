-- Fimding the amount of stores in each of the countries
SELECT 
    country_code, COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY
    country_code
ORDER BY
    COUNT(*) DESC


