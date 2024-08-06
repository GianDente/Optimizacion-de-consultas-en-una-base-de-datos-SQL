SET enable_hashjoin = off;
SET enable_memoize = off;
SET enable_material = off;
SET enable_parallel_append = off;
SET enable_parallel_hash = off;
SET max_parallel_workers_per_gather = 0;

----------------- Consulta 1 ------------------
EXPLAIN (ANALYZE) 
SELECT
  t.passenger_name,
  COUNT(DISTINCT(tf.flight_id)) total_flight_count,
  SUM(tf.amount) historical_amount_collected,
  MAX(bp.boarding_no) max_boarding_number
FROM 
  ticket_flights tf
JOIN 
  tickets t ON t.ticket_no = tf.ticket_no
JOIN 
  boarding_passes bp ON bp.ticket_no = t.ticket_no
JOIN 
  flights f ON bp.flight_id = f.flight_id
WHERE
  tf.amount =(SELECT MAX(tf.amount) FROM ticket_flights tf) AND DATE(f.scheduled_departure) BETWEEN '2017-01-26' AND '2017-06-26'
GROUP BY
  t.passenger_name
HAVING
  COUNT(DISTINCT(tf.flight_id)) > 3
ORDER BY
  COUNT(DISTINCT(tf.flight_id)) DESC;

 
ANALYZE bookings.ticket_flights;
ANALYZE bookings.flights;
ANALYZE bookings.boarding_passes;

--Estudio selectividad
SELECT DISTINCT(tf.amount) AS valor ,
COUNT(*) AS cantidad,
(count(*) * 1.0/ (SELECT count(*) FROM bookings.ticket_flights )) AS Porcentaje
FROM bookings.ticket_flights AS tf
GROUP BY amount;


SELECT DISTINCT(DATE(f.scheduled_departure)) AS valor ,
COUNT(*) AS cantidad,
(count(*) * 1.0/ (SELECT count(*) FROM bookings.flights )) AS Porcentaje
FROM bookings.flights  AS f
GROUP BY DATE(scheduled_departure)
HAVING  DATE(f.scheduled_departure) BETWEEN '2017-01-26' AND '2017-06-26'

--experimento 1
CREATE INDEX q1_tf_amount_hash  ON bookings.ticket_flights USING HASH (amount);
DROP INDEX IF EXISTS q1_tf_amount_hash;

--experimento 2
CREATE INDEX q1_tf_amount  ON bookings.ticket_flights (amount);
DROP INDEX IF EXISTS q1_tf_amount;

--experimento 3
CREATE INDEX q1_tf_amount  ON bookings.ticket_flights(amount);
DROP INDEX IF EXISTS q1_tf_amount;
CREATE INDEX q1_f_scheduled_departure ON bookings.flights(scheduled_departure);
DROP INDEX IF EXISTS q1_f_scheduled_departure;

--experimento 4
CREATE INDEX q1_f_flight_id  ON bookings.flights USING HASH (flight_id);
DROP INDEX IF EXISTS q1_f_flight_id;

--experimento 5
CREATE INDEX q1_f_flight_id_scheduled_departure ON bookings.flights(flight_id,scheduled_departure);
DROP INDEX IF EXISTS q1_f_flight_id_scheduled_departure;

--experimento 6
CREATE INDEX q1_tf_amount  ON bookings.ticket_flights(amount);
DROP INDEX IF EXISTS q1_tf_amount;
CREATE INDEX q1_f_flight_id_scheduled_departure ON bookings.flights(flight_id,scheduled_departure);
DROP INDEX IF EXISTS q1_f_flight_id_scheduled_departure;

----------------- Consulta 2 ------------------
EXPLAIN (ANALYZE) 
SELECT
  f.flight_id,
  f.scheduled_departure,
  COUNT(DISTINCT(bp.ticket_no)) count_tickets,
  SUM(tf.amount) amount_collected
  FROM bookings.flights f
  JOIN bookings.boarding_passes bp ON bp.flight_id = f.flight_id
  JOIN bookings.tickets t ON t.ticket_no = bp.ticket_no
  JOIN bookings.ticket_flights tf ON tf.ticket_no = t.ticket_no
  JOIN bookings.aircrafts_data ad ON ad.aircraft_code = f.aircraft_code
WHERE
DATE(f.scheduled_departure) IN (
  SELECT 
    scheduled_departure_date
  FROM (
    SELECT
      DATE(f2.scheduled_departure) AS scheduled_departure_date,
      COUNT(DISTINCT(f2.flight_id))
    FROM bookings.flights f2
    WHERE f2.status IN ('Arrived', 'Scheduled') AND f2.scheduled_departure = f2.actual_departure
    GROUP BY DATE(f2.scheduled_departure)
    HAVING COUNT(DISTINCT(f2.flight_id)) >= 30
    ORDER BY COUNT(DISTINCT(f2.flight_id)) DESC
  ) AS arrived_high_count
)
AND ad.aircraft_code IN ('773', '763', '733', '319')
GROUP BY f.flight_id
ORDER BY SUM(tf.amount) DESC, COUNT(DISTINCT(bp.ticket_no)) DESC
limit 2000;

ANALYZE bookings.ticket_flights;
ANALYZE bookings.flights;
ANALYZE bookings.boarding_passes;
ANALYZE bookings.aircrafts_data;

--estudio selectividad
-- atributo aircraft_code de aircrafts_data
SELECT DISTINCT(ad.aircraft_code) AS valor ,
COUNT(*) AS cantidad,
(count(*) * 1.0/ (SELECT count(*) FROM bookings.aircrafts_data )) AS Porcentaje
FROM bookings.aircrafts_data AS ad
GROUP BY aircraft_code
having aircraft_code IN ('773', '763', '733', '319');

--atributo scheduled_departure de la tabla flights.
SELECT DISTINCT(DATE(f.scheduled_departure)) AS valor ,
COUNT(*) AS cantidad,
(count(*) * 1.0/ (SELECT count(*) FROM bookings.flights )) AS Porcentaje
FROM bookings.flights  AS f
GROUP BY DATE(scheduled_departure)
HAVING  DATE(f.scheduled_departure) IN (
  SELECT 
    scheduled_departure_date
  FROM (
    SELECT
      DATE(f2.scheduled_departure) AS scheduled_departure_date,
      COUNT(DISTINCT(f2.flight_id))
    FROM bookings.flights f2
    WHERE f2.status IN ('Arrived', 'Scheduled') AND f2.scheduled_departure = f2.actual_departure
    GROUP BY DATE(f2.scheduled_departure)
    HAVING COUNT(DISTINCT(f2.flight_id)) >= 30
    ORDER BY COUNT(DISTINCT(f2.flight_id)) DESC
  ) AS arrived_high_count
);

--experimento 1
CREATE INDEX q2_ad_aircraft_code ON bookings.aircrafts_data USING HASH (aircraft_code);
DROP INDEX IF EXISTS q2_ad_aircraft_code;

--experimento 2
CREATE INDEX q2_f_scheduled_departure ON bookings.flights USING HASH (scheduled_departure);
DROP INDEX IF EXISTS q2_f_scheduled_departure;

--experimento 3
CREATE INDEX q2_f_flight_id_aircraft_code_scheduled_departure ON bookings.flights (flight_id,aircraft_code,scheduled_departure);
DROP INDEX IF EXISTS  q2_f_flight_id_aircraft_code_scheduled_departure;

--experimento 4
CREATE INDEX q2_t_tickect_no ON bookings.tickets USING HASH (ticket_no);
DROP INDEX IF EXISTS q2_t_tickect_no;

--experimento 5
CREATE INDEX q2_t_tickect_no ON bookings.tickets USING HASH (ticket_no);
DROP INDEX IF EXISTS q2_t_tickect_no;
CREATE INDEX q2_ad_aircraft_code ON bookings.aircrafts_data USING HASH (aircraft_code);
DROP INDEX IF EXISTS q2_ad_aircraft_code;

--experimento 6
CREATE INDEX q2_f_flight_id_aircraft_code_scheduled_departure ON bookings.flights (flight_id,aircraft_code,scheduled_departure);
DROP INDEX IF EXISTS  q2_f_flight_id_aircraft_code_scheduled_departure;
CREATE INDEX q2_t_tickect_no ON bookings.tickets USING HASH (ticket_no);
DROP INDEX IF EXISTS q2_t_tickect_no;
CREATE INDEX q2_ad_aircraft_code ON bookings.aircrafts_data USING HASH (aircraft_code);
DROP INDEX IF EXISTS q2_ad_aircraft_code;

----------------- Consulta 3 ------------------
EXPLAIN(ANALYZE)
SELECT
	t.passenger_name,
	SUM(b.total_amount) total_amount,
	COUNT(DISTINCT(b.book_ref)) count_bookings
FROM tickets t
JOIN bookings b ON b.book_ref = t.book_ref
GROUP BY t.passenger_name
limit 2000;

--experimento 1
DROP INDEX q3_bookings_bookref
CREATE INDEX q3_bookings_bookref ON bookings.bookings USING HASH (book_ref);

--experimento 2
DROP INDEX q3_tickets
CREATE INDEX q3_tickets ON bookings.tickets (book_ref,passenger_name);

--experimento 3
DROP INDEX q3_t_book_ref
CREATE INDEX q3_t_book_ref ON bookings.tickets (book_ref);

DROP INDEX q3_t_passenger_name
CREATE INDEX q3_t_passenger_name ON bookings.tickets (passenger_name);

--experimento 4
DROP INDEX q3_bookings
CREATE INDEX q3_bookings ON bookings.bookings (book_ref,total_amount);

--experimento 5
DROP INDEX q3_tickets
CREATE INDEX q3_tickets ON bookings.tickets (book_ref,passenger_name);
DROP INDEX q3_bookings
CREATE INDEX q3_bookings ON bookings.bookings (book_ref,total_amount);

----------------- Consulta 4 ------------------
EXPLAIN(ANALYZE)
SELECT
	f.aircraft_code,
	s.seat_no,
	SUM(tf.amount) total_amount
FROM seats s
JOIN flights f ON f.aircraft_code = s.aircraft_code
JOIN ticket_flights tf ON tf.flight_id = f.flight_id
WHERE
DATE(f.scheduled_departure) BETWEEN '2017-01-26' AND '2017-02-26'
GROUP BY f.aircraft_code, s.seat_no;

--experimento 1
DROP INDEX q4_flights_scheduled_departure 
CREATE INDEX q4_flights_scheduled_departure ON bookings.flights (scheduled_departure);

--experimento 2

-- selectividad joins
-- join 1
select COUNT(*)
FROM seats s
JOIN flights f ON f.aircraft_code = s.aircraft_code 

select COUNT(*)
FROM seats s

select COUNT(*)
from flights f

--join externo
select COUNT(*)
FROM flights f
JOIN ticket_flights tf ON tf.flight_id = f.flight_id

select COUNT(*)
from ticket_flights tf

--indice
DROP INDEX q4_flights_flight_id
CREATE INDEX q4_flights_flight_id ON bookings.flights USING HASH (flight_id);
DROP INDEX q4_flights_scheduled_departure 
CREATE INDEX q4_flights_scheduled_departure ON bookings.flights (scheduled_departure);

--experimento 3
DROP INDEX q4_flights
CREATE INDEX q4_flights ON bookings.flights (flight_id, aircraft_code);

--experimento 4
DROP INDEX q4_ticket_flights
CREATE INDEX q4_ticket_flights ON bookings.ticket_flights (flight_id);

--experimento 5
DROP INDEX q4_ticket_flights
CREATE INDEX q4_ticket_flights ON bookings.ticket_flights (flight_id,amount);

--experimento 6
DROP INDEX q4_ticket_flights
CREATE INDEX q4_ticket_flights ON bookings.ticket_flights (flight_id,amount);
DROP INDEX q4_flights
CREATE INDEX q4_flights ON bookings.flights (aircraft_code);

----------------- Consulta 5 ------------------
EXPLAIN(ANALYZE)
SELECT
	DATE(b.book_date),
	SUM(b.total_amount) revenue,
	COUNT(DISTINCT(t.passenger_id)) count_passengers
FROM bookings b
JOIN tickets t ON t.book_ref = b.book_ref
GROUP BY
	DATE(b.book_date)
ORDER BY
	COUNT(DISTINCT(t.passenger_id)) DESC,
	SUM(b.total_amount) DESC
	
--experimento 1
DROP INDEX q5_tickets
CREATE INDEX q5_tickets ON bookings.tickets USING HASH (book_ref);

--experimento 2
DROP INDEX q5_bookings
CREATE INDEX q5_bookings ON bookings.bookings (book_date,total_amount);

--experimento 3
DROP INDEX q5_tickets
CREATE INDEX q5_tickets ON bookings.tickets (book_ref,passenger_id);

--experimento 4
DROP INDEX q5_tickets
CREATE INDEX q5_tickets ON bookings.tickets (book_ref,passenger_id);
DROP INDEX q5_bookings
CREATE INDEX q5_bookings ON bookings.bookings (book_date,total_amount);

--experimento 5
DROP INDEX q5_t_book_ref_passenger_id
CREATE INDEX q5_t_book_ref_passenger_id ON bookings.tickets (book_ref,passenger_id);
DROP INDEX q5_b_book_ref_book_date_total_amount
CREATE INDEX q5_b_book_ref_book_date_total_amount ON bookings.bookings (book_ref,book_date,total_amount);
