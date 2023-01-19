-- Q4 = 2019-01-10
select lpep_pickup_datetime::date, sum(trip_distance) as total_trip_distance
FROM public.ny_taxi_2019
where lpep_pickup_datetime::date in (date '2019-01-15', date '2019-01-18', date '2019-01-28', date '2019-01-10')
group by lpep_pickup_datetime::date
order by total_trip_distance desc