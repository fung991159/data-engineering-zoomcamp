select passenger_count, count(*) as cnt
FROM public.ny_taxi_2019
where lpep_pickup_datetime::date = date '2019-01-01'
group by passenger_count