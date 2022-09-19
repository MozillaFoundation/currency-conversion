CREATE OR REPLACE VIEW public.vw_latest_exchange_rate
 AS
 WITH all_dates AS (
    SELECT t.exchange_rate_date::date AS exchange_rate_date
    FROM generate_series('2016-01-01 00:00:00'::timestamp without time zone::timestamp with time zone
                         , CURRENT_TIMESTAMP, '1 day'::interval) t(exchange_rate_date)
    )
    , dates_joined AS (
        SELECT l.exchange_rate_date,
        COALESCE(r.exchange_rate, 0) AS exchange_rate
        FROM all_dates AS l
        LEFT JOIN ( 
                   SELECT DISTINCT exchange_rate_date
                          ,1 AS exchange_rate
                   FROM exchange_rates) AS r 
        ON l.exchange_rate_date = r.exchange_rate_date
        )

SELECT max(exchange_rate_date) AS max_exchange_rate_date
FROM dates_joined
WHERE exchange_rate = 1;

ALTER TABLE public.latest_exchange_rate
    OWNER TO postgres;

