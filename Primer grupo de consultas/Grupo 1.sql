
SET enable_hashjoin = off;
SET enable_memoize = off;
SET enable_material = off;
SET enable_parallel_append = off;
SET enable_parallel_hash = off;
SET max_parallel_workers_per_gather = 0;

----------------- Consulta 1 ------------------
EXPLAIN (ANALYZE)
SELECT
f.aircraft_code,
f.scheduled_departure,
bp.boarding_no,
bp.seat_no,
t.passenger_name,
t.book_ref,
tf.ticket_no,
tf.amount
FROM
bookings.flights f
JOIN
bookings.boarding_passes bp ON bp.flight_id = f.flight_id
JOIN
bookings.tickets t ON t.ticket_no = bp.ticket_no
JOIN
bookings.ticket_flights tf ON tf.ticket_no = t.ticket_no
WHERE
f.aircraft_code = '773'
ORDER BY
f.scheduled_departure DESC;

ANALYZE bookings.flights;
ANALYZE bookings.boarding_passes;
ANALYZE q1_aircraft_code_flights

--experimento 1
DROP INDEX q1_aircraft_code_flights
CREATE INDEX q1_aircraft_code_flights ON bookings.flights USING HASH (aircraft_code);

--experimento 2
DROP INDEX q1_aircraft_code_scheduled_departure_flights
CREATE INDEX q1_aircraft_code_scheduled_departure_flights ON bookings.flights (aircraft_code,scheduled_departure);

--experimento 3
DROP INDEX q1_aircraft_code_flight_id_scheduled_departure_flights
CREATE INDEX q1_aircraft_code_flight_id_scheduled_departure_flights ON bookings.flights (aircraft_code,flight_id,scheduled_departure);

--experimento 4
DROP INDEX q1_aircraft_code_flights 
CREATE INDEX q1_aircraft_code_flights ON bookings.flights USING HASH (aircraft_code);
DROP INDEX q1_ticket_no_tickect_flights
CREATE INDEX q1_ticket_no_tickect_flights ON bookings.ticket_flights USING HASH (ticket_no);


----------------- Consulta 2 ------------------
EXPLAIN (ANALYZE)
SELECT 
	f.aircraft_code,
	f.status,
	f.scheduled_departure,
	f.scheduled_arrival,
	bp.boarding_no, 
	bp.seat_no,
	t.passenger_name,
	t.book_ref, 
	tf.ticket_no,
	tf.amount
FROM 
bookings.flights f
JOIN 
bookings.airports_data da ON f.departure_airport = da.airport_code
JOIN 
bookings.airports_data aa ON f.arrival_airport = aa.airport_code
JOIN 
boarding_passes bp ON bp.flight_id  = f.flight_id 
JOIN 
tickets t ON t.ticket_no  = bp.ticket_no 
JOIN 
ticket_flights tf ON tf.ticket_no = t.ticket_no
WHERE 
f.status = 'Arrived' 
AND f.aircraft_code = '319'
AND f.scheduled_departure BETWEEN '2016-08-14' AND '2016-12-24'
ORDER BY f.scheduled_departure DESC;


--experimento 1: indice B+
DROP INDEX q2_status_aircraft_code_scheduled_departure_flights
CREATE INDEX q2_status_aircraft_code_scheduled_departure_flights ON bookings.flights (status,aircraft_code,scheduled_departure);

--experimento 2

-- estudio selectividad
SELECT DISTINCT(f.aircraft_code  ), 
COUNT(*),
(SELECT COUNT(*) FROM bookings.flights f ) AS cardinality, 
COUNT(*) * 1.0 / (SELECT COUNT(*) FROM bookings.flights f ) * 100 AS percentage
FROM bookings.flights f 
GROUP BY f.aircraft_code 

SELECT DISTINCT(f.status), 
COUNT(*),
(SELECT COUNT(*) FROM bookings.flights f ) AS cardinality, 
COUNT(*) * 1.0 / (SELECT COUNT(*) FROM bookings.flights f ) * 100 AS percentage
FROM bookings.flights f 
GROUP BY f.status

SELECT DISTINCT(date(scheduled_departure)), 
COUNT(*),
(SELECT COUNT(*) FROM bookings.flights f ) AS cardinality, 
COUNT(*) * 1.0 / (SELECT COUNT(*) FROM bookings.flights f ) * 100 AS percentage
FROM bookings.flights f 
GROUP BY date(scheduled_departure) 

--indice
DROP INDEX q2_aircraft_code_flights
CREATE INDEX q2_aircraft_code_flights ON bookings.flights USING HASH (aircraft_code);

--experimento 3
DROP INDEX q2_aircraft_code_scheduled_departure_flights
CREATE INDEX q2_aircraft_code_scheduled_departure_flights ON bookings.flights (aircraft_code,scheduled_departure);


------------- Consulta 3 ---------------
EXPLAIN (ANALYZE)
SELECT 
	f.flight_no,
 	bp.ticket_no,
	bp.flight_id, 
	bp.boarding_no,
  	b.book_date,
  	f.scheduled_departure,
  	f.scheduled_arrival,
  	f.status,
  	f.arrival_airport,
  	f.departure_airport,
	bp.seat_no,
	tf.fare_conditions,
	t.book_ref,
	t.passenger_name 
FROM 
	boarding_passes bp
JOIN 
	ticket_flights tf 
	ON bp.ticket_no = tf.ticket_no AND bp.flight_id = tf.flight_id
JOIN 
	tickets t
	ON tf.ticket_no = t.ticket_no
JOIN 
	flights f 
	ON f.flight_id = bp.flight_id 
JOIN 
	bookings b 
	ON b.book_ref  = t.book_ref 
WHERE 
	b.book_date < (f.scheduled_departure - INTERVAL '5 DAYS')
ORDER BY 
	f.flight_no ASC, 
	bp.boarding_no ASC;

--experimento 1
DROP INDEX q3_bookings_bookdate
CREATE INDEX q3_bookings_bookdate ON bookings.bookings (book_date);
DROP INDEX q3_flights_scheduleddeparture
CREATE INDEX q3_flights_scheduleddeparture ON bookings.flights (scheduled_departure);

--experimento 2
DROP INDEX q3_book_ref_tickets
CREATE INDEX q3_book_ref_tickets ON tickets(book_ref);

--experimento 3
DROP INDEX q3_book_ref_tickets
CREATE INDEX q3_book_ref_tickets ON tickets USING HASH(book_ref);


---------Consulta 4-----------
EXPLAIN (ANALYZE)
SELECT 
	tf.flight_id,
	tf.fare_conditions, 
	COUNT(t.ticket_no) AS tickets,
	COUNT(b.book_ref) AS bookings,
	SUM(amount) AS total_amount
FROM 
	ticket_flights tf
JOIN 
	tickets t
	ON tf.ticket_no = t.ticket_no
JOIN 
	flights f 
	ON f.flight_id = tf.flight_id 
JOIN 
	bookings b 
	ON b.book_ref  = t.book_ref 
WHERE 
	f.status  = 'Arrived'
GROUP BY
	tf.flight_id,
	tf.fare_conditions;

--experimento 1
DROP INDEX q4_book_ref_tickets
CREATE INDEX q4_book_ref_tickets ON tickets USING HASH(book_ref);

---------Consulta 5-----------
EXPLAIN (ANALYZE)
SELECT
	t.ticket_no,
	t.book_ref,
	f.flight_no,
	t.passenger_name,
	tf.fare_conditions,
	b.book_date,
	b.total_amount,
	f.scheduled_departure,
	f.scheduled_arrival
FROM bookings.tickets t
JOIN bookings.bookings b ON t.book_ref = b.book_ref
JOIN bookings.ticket_flights tf ON t.ticket_no = tf.ticket_no
JOIN bookings.flights f ON tf.flight_id = f.flight_id
WHERE 
	b.book_date BETWEEN '2016-08-14' AND '2016-12-18'
	AND f.aircraft_code = '733' 
	AND b.total_amount > 50000
	AND tf.fare_conditions = 'Economy'
ORDER BY
	t.passenger_name ;

--experimento 1
DROP INDEX q5_book_date_total_amount_bookings
CREATE INDEX q5_book_date_total_amount_bookings ON bookings.bookings (book_date,total_amount);
DROP INDEX q5_aircraft_code_flights
CREATE INDEX q5_aircraft_code_flights ON bookings.flights USING HASH (aircraft_code);

--experimento 2

--estudio selectividad
SELECT DISTINCT(total_amount), 
COUNT(*),
(SELECT COUNT(*) FROM bookings.bookings) AS cardinality, 
COUNT(*) * 1.0 / (SELECT COUNT(*) FROM bookings.bookings) * 100 AS percentage
FROM bookings.bookings
GROUP BY total_amount
order BY total_amount desc

SELECT DISTINCT(total_amount), 
COUNT(*),
(SELECT COUNT(*) FROM bookings.bookings) AS cardinality, 
COUNT(*) * 1.0 / (SELECT COUNT(*) FROM bookings.bookings) * 100 AS percentage
FROM bookings.bookings
GROUP BY total_amount
order BY total_amount

SELECT DISTINCT(f.aircraft_code), 
COUNT(*),
(SELECT COUNT(*) FROM bookings.flights f ) AS cardinality, 
COUNT(*) * 1.0 / (SELECT COUNT(*) FROM bookings.flights f ) * 100 AS percentage
FROM bookings.flights f 
GROUP BY f.aircraft_code

SELECT DISTINCT(fare_conditions), 
COUNT(*),
(SELECT COUNT(*) FROM bookings.ticket_flights ) AS cardinality, 
COUNT(*) * 1.0 / (SELECT COUNT(*) FROM bookings.ticket_flights) * 100 AS percentage
FROM bookings.ticket_flights
GROUP by fare_conditions

SELECT DISTINCT(date(book_date)), 
COUNT(*),
(SELECT COUNT(*) FROM bookings.bookings) AS cardinality, 
COUNT(*) * 1.0 / (SELECT COUNT(*) FROM bookings.bookings) * 100 AS percentage
FROM bookings.bookings
GROUP BY date(book_date) 

--indice
DROP INDEX q5_aircraft_code_flights
CREATE INDEX q5_aircraft_code_flights ON bookings.flights USING HASH (aircraft_code);

--experimento 3
DROP INDEX q5_book_ref_bookings
CREATE INDEX q5_book_ref_bookings ON bookings USING HASH (book_ref);
DROP INDEX q5_aircraft_code_flights
CREATE INDEX q5_aircraft_code_flights ON bookings.flights USING HASH (aircraft_code);
