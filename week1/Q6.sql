select dp."Zone"
		, round(max(tip_amount)) as total_tip
from ny_taxi_2019 fact
left join ny_taxi_lookup dp
	on fact."DOLocationID" = dp."LocationID"	
where fact."PULocationID" = 7
group by dp."Zone"
order by total_tip desc
