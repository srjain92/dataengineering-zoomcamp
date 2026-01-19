# dataengineering-zoomcamp
Data Engineering ZoomCamp by DataTalksClub

#**Question 1. Understanding Docker images**
docker run --rm python:3.13 pip --version

**Question 3. Counting short trips**
SELECT COUNT(1)
FROM public.green_tripdata
WHERE DATE(lpep_pickup_datetime) >= '2025-11-01'
AND DATE(lpep_pickup_datetime) < '2025-12-01'
AND trip_distance <= 1

**Question 4. Longest trip for each day**
SELECT DATE(lpep_pickup_datetime)
FROM public.green_tripdata
WHERE trip_distance IN (
SELECT MAX(trip_distance)
FROM public.green_tripdata
WHERE trip_distance < 100)

**Question 5. Biggest pickup zone**
SELECT b.zone
FROM public.green_tripdata a
LEFT JOIN public.taxi_zone_lookup b ON b.locationid = a.pulocationid
WHERE DATE(a.lpep_pickup_datetime) = '2025-11-18'
GROUP BY b.zone
ORDER BY SUM(a.total_amount) DESC
limit 1;

**Question 6. Largest tip**
SELECT b.zone
FROM public.green_tripdata a
LEFT JOIN public.taxi_zone_lookup b ON b.locationid = a.dolocationid
WHERE b.zone = 'East Harlem North'
AND DATE(lpep_pickup_datetime) >= '2025-11-01'
AND DATE(lpep_pickup_datetime) < '2025-12-01'
GROUP BY b.zone
ORDER BY MAX(a.tip_amount)
