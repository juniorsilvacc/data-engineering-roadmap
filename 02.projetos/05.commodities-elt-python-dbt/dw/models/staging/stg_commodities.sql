-- import

with source as (
    SELECT 
        "Date",
        "Close",
        simbolo
    FROM 
        {{source ('commodities_python_dbt', 'commodities')}}
),

renamed as (
    SELECT 
        CAST("Date" as date) as data,
        "Close" as valor_fechamento,
        simbolo
    FROM 
        source
)

SELECT * FROM renamed