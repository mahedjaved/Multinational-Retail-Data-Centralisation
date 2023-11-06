--  finds the overall staff numbers in each location around the world
SELECT
    DISTINCT SUM(DSD.staff_numbers) AS total_staff_numbers
    ,DSD.country_code
FROM
    dim_store_details AS DSD
GROUP BY    
    DSD.country_code
ORDER BY
    SUM(DSD.staff_numbers) DESC