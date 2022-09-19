CREATE OR REPLACE VIEW public.vw_exchange_rate_gaps
 AS
 WITH all_dates AS (
        SELECT t.exchange_rate_date::date AS exchange_rate_date
        FROM generate_series('2016-01-01 00:00:00'::timestamp without time zone::timestamp with time zone
                            ,CURRENT_TIMESTAMP, '1 day'::interval) t(exchange_rate_date)
    )
    ,currencies AS (
        SELECT DISTINCT exchange_rates.local_currency
        FROM exchange_rates
    )
    ,currencies_dates AS (
        SELECT l.local_currency
            ,r.exchange_rate_date
        FROM currencies AS l
        CROSS JOIN 
            all_dates AS r
    )

    ,date_gaps AS (
        SELECT l.local_currency
            ,l.exchange_rate_date,
            ,COALESCE(r.exchange_rate, 0) AS exchange_rate
        FROM currencies_dates AS l
        LEFT JOIN ( 
                SELECT DISTINCT exchange_rate_date,
                                ,local_currency
                                ,1 AS exchange_rate
                FROM exchange_rates
                ) AS r 
        ON l.exchange_rate_date = r.exchange_rate_date 
        AND l.local_currency = r.local_currency
    )

SELECT local_currency
       ,exchange_rate_date
FROM date_gaps
WHERE date_gaps.exchange_rate = 0;

ALTER TABLE public.exchange_rate_gaps
    OWNER TO postgres;

