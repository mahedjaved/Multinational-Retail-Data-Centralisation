CREATE TABLE 
    new_dim_date_times AS
SELECT
    timestamp
    ,CAST(month AS VARCHAR(2)) AS month
    ,CAST(year AS VARCHAR(4)) AS year
    ,CAST(day AS VARCHAR(2)) AS day
    ,CAST(time_period AS VARCHAR(15))
    ,CAST(date_uuid AS UUID) AS date_uuid
    ,datetime
FROM 
    dim_date_times;

DROP TABLE  
    dim_date_times;

ALTER TABLE new_dim_date_times
    RENAME TO dim_date_times;