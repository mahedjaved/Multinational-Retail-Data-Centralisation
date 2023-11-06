-- computing the average time taken between each sale grouped by year
-- experimenting with LAG
SELECT
    sub1.year
    ,AVG(sub1.time_diff)
FROM (
    SELECT
        year
        ,(datetime - LAG(datetime) OVER (PARTITION BY year ORDER BY datetime)) AS time_diff
    FROM dim_date_times
) sub1
GROUP BY  
    sub1.year
ORDER BY  
    AVG(sub1.time_diff) DESC
LIMIT(5)


-- experimenting with LEAD
SELECT
    sub1.year
    ,AVG(sub1.time_diff)
FROM (
    SELECT
        year
        ,(LEAD(datetime) OVER (PARTITION BY year ORDER BY datetime) - datetime) AS time_diff
    FROM dim_date_times
) sub1
GROUP BY
    sub1.year
ORDER BY
    AVG(sub1.time_diff) DESC
LIMIT(5)