-- Q3 = 20689
SELECT count(*)
	FROM public.ny_taxi_2019
	where lpep_pickup_datetime::date = date '2019-01-15'