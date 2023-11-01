CREATE TABLE 
    new_dim_card_details AS
SELECT
    CAST(card_number AS VARCHAR(20)) AS card_number
    ,CAST(expiry_date AS VARCHAR(20)) AS expiry_date
    ,CAST(card_provider AS VARCHAR(255)) AS card_provider
    ,CAST(date_payment_confirmed AS DATE) AS date_payment_confirmed
    ,CAST("card_number expiry_date" AS VARCHAR(255)) AS "card_number expiry_date" 
FROM 
    dim_card_details

DROP TABLE  
    dim_card_details

ALTER TABLE new_dim_card_details
    RENAME TO dim_card_details;