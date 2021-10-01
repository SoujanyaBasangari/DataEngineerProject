

--To determine the correctness of your output datasets, your colleagues want you to write the following queries:

--The average distance driven by yellow and green taxis per hour

SELECT avg(trip_distance) AS Average_Distance,pickup_hour AS Hour
FROM TLC_data 
GROUP BY(pickup_hour)
ORDER BY pickup_hour DESC

--Day of the week in 2019 and 2020 which has the lowest number of single rider trips

WITH cte2019(day_ofweek,year,singlerides) AS
(SELECT DATEPART(dw, pickup_datetime),DATEPART(year, pickup_datetime)  AS year, count(VendorID)
    FROM TLC_data
    WHERE passenger_count=1 and RatecodeID = 1 and DATEPART(year, pickup_datetime) = '2019'
    GROUP BY DATEPART(dw, pickup_datetime),DATEPART(year, pickup_datetime) )
SELECT top 1 day_ofweek,year,singlerides from cte2019 ORDER BY singlerides ASC 
	
with cte2020(day_ofweek,year,singlerides) as
(select DATEPART(dw, pickup_datetime),DATEPART(year, pickup_datetime)  as year, count(VendorID)
    from TLC_data
    where passenger_count=1 and RatecodeID = 1 and DATEPART(year, pickup_datetime) = '2020'
    GROUP BY DATEPART(dw, pickup_datetime),DATEPART(year, pickup_datetime) )
SELECT top 1 day_ofweek,year,singlerides from cte2020 ORDER BY singlerides ASC 

--The top 3 of the busiest hours


WITH cte_busiest_hour(Hour,total_count) AS
(
((SELECT pickup_hour AS time, count(*) AS Pickupcount 
                        FROM TLC_data 
                        GROUP BY pickup_hour)
UNION
(SELECT dropoff_hour as dropoff_hour, count(*) AS Dropoffcount 
                        FROM TLC_data 
                        GROUP BY  dropoff_hour))
						)

SELECT TOP 3 Hour,total_count FROM cte_busiest_hour ORDER BY total_count DESC