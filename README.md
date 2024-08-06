
# Optimización de consultas en una base de datos SQL

Decisiones de diseño físico para la optimización del tiempo de ejecución de un conjunto de consultas sobre una base de datos sql utilizando índices. 


## Dado el esquema de la base de datos de un aeropuerto:

```
CREATE TABLE bookings.aircrafts_data (
    aircraft_code bpchar(3) NOT NULL,
    model jsonb NOT NULL,
    "range" int4 NOT NULL,
    CONSTRAINT aircrafts_pkey PRIMARY KEY (aircraft_code),
    CONSTRAINT aircrafts_range_check CHECK ((range > 0))
);

CREATE TABLE bookings.airports_data (
    airport_code bpchar(3) NOT NULL,
    airport_name jsonb NOT NULL,
    city jsonb NOT NULL,
    coordinates point NOT NULL,
    timezone text NOT NULL,
    CONSTRAINT airports_data_pkey PRIMARY KEY (airport_code)
);

CREATE TABLE bookings.bookings (
    book_ref bpchar(6) NOT NULL,
    book_date timestamptz NOT NULL,
    total_amount numeric(10, 2) NOT NULL,
    CONSTRAINT bookings_pkey PRIMARY KEY (book_ref)
);

CREATE TABLE bookings.flights (
    flight_id serial4 NOT NULL,
    flight_no bpchar(6) NOT NULL,
    scheduled_departure timestamptz NOT NULL,
    scheduled_arrival timestamptz NOT NULL,
    departure_airport bpchar(3) NOT NULL,
    arrival_airport bpchar(3) NOT NULL,
    status varchar(20) NOT NULL,
    aircraft_code bpchar(3) NOT NULL,
    actual_departure timestamptz NULL,
    actual_arrival timestamptz NULL,
    CONSTRAINT flights_check CHECK ((scheduled_arrival > scheduled_departure)),
    CONSTRAINT flights_check1 CHECK (((actual_arrival IS NULL) OR ((actual_departure IS NOT NULL) AND (actual_arrival IS NOT NULL) AND (actual_arrival > actual_departure)))),
    CONSTRAINT flights_flight_no_scheduled_departure_key UNIQUE (flight_no, scheduled_departure),
    CONSTRAINT flights_pkey PRIMARY KEY (flight_id),
    CONSTRAINT flights_status_check CHECK (((status)::text = ANY (ARRAY[('On Time'::character varying)::text, ('Delayed'::character varying)::text, ('Departed'::character varying)::text, ('Arrived'::character varying)::text, ('Scheduled'::character varying)::text, ('Cancelled'::character varying)::text]))),
    CONSTRAINT flights_aircraft_code_fkey FOREIGN KEY (aircraft_code) REFERENCES bookings.aircrafts_data(aircraft_code),
    CONSTRAINT flights_arrival_airport_fkey FOREIGN KEY (arrival_airport) REFERENCES bookings.airports_data(airport_code),
    CONSTRAINT flights_departure_airport_fkey FOREIGN KEY (departure_airport) REFERENCES bookings.airports_data(airport_code)
);

CREATE TABLE bookings.seats (
    aircraft_code bpchar(3) NOT NULL,
    seat_no varchar(4) NOT NULL,
    fare_conditions varchar(10) NOT NULL,
    CONSTRAINT seats_fare_conditions_check CHECK (((fare_conditions)::text = ANY
        (ARRAY[('Economy'::character varying)::text, ('Comfort'::character varying)::text,
        ('Business'::character varying)::text]))),
    CONSTRAINT seats_pkey PRIMARY KEY (aircraft_code, seat_no),
    CONSTRAINT seats_aircraft_code_fkey FOREIGN KEY (aircraft_code) REFERENCES
        bookings.aircrafts_data(aircraft_code) ON DELETE CASCADE
);

CREATE TABLE bookings.tickets (
    ticket_no bpchar(13) NOT NULL,
    book_ref bpchar(6) NOT NULL,
    passenger_id varchar(20) NOT NULL,
    passenger_name text NOT NULL,
    contact_data jsonb NULL,
    CONSTRAINT tickets_pkey PRIMARY KEY (ticket_no),
    CONSTRAINT tickets_book_ref_fkey FOREIGN KEY (book_ref) REFERENCES bookings.bookings(book_ref)
);

CREATE TABLE bookings.ticket_flights (
    ticket_no bpchar(13) NOT NULL,
    flight_id int4 NOT NULL,
    fare_conditions varchar(10) NOT NULL,
    amount numeric(10, 2) NOT NULL,
    CONSTRAINT ticket_flights_amount_check CHECK ((amount >= (0)::numeric)),
    CONSTRAINT ticket_flights_fare_conditions_check CHECK (((fare_conditions)::text = ANY
        (ARRAY[('Economy'::character varying)::text, ('Comfort'::character varying)::text,
        ('Business'::character varying)::text]))),
    CONSTRAINT ticket_flights_pkey PRIMARY KEY (ticket_no, flight_id),
    CONSTRAINT ticket_flights_flight_id_fkey FOREIGN KEY (flight_id) REFERENCES
        bookings.flights(flight_id),
    CONSTRAINT ticket_flights_ticket_no_fkey FOREIGN KEY (ticket_no) REFERENCES
        bookings.tickets(ticket_no)
);

CREATE TABLE bookings.boarding_passes (
ticket_no bpchar(13) NOT NULL,
flight_id int4 NOT NULL,
boarding_no int4 NOT NULL,
seat_no varchar(4) NOT NULL,
CONSTRAINT boarding_passes_flight_id_boarding_no_key UNIQUE (flight_id, boarding_no),
CONSTRAINT boarding_passes_flight_id_seat_no_key UNIQUE (flight_id, seat_no),
CONSTRAINT boarding_passes_pkey PRIMARY KEY (ticket_no, flight_id),
CONSTRAINT boarding_passes_ticket_no_fkey FOREIGN KEY (ticket_no,flight_id) REFERENCES
bookings.ticket_flights(ticket_no,flight_id)
);
```
## Dadas las siguientes consultas:

### Primer grupo de consultas

**1.** Reporte de vuelos por aircraft code.

```
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

```

**2.** Reporte de vuelos por aircraft code.

```
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
    boarding_passes bp ON bp.flight_id = f.flight_id
JOIN
    tickets t ON t.ticket_no = bp.ticket_no
JOIN
    ticket_flights tf ON tf.ticket_no = t.ticket_no
WHERE
    f.status = 'Arrived'
    AND f.aircraft_code = '319'
    AND f.scheduled_departure BETWEEN '2016-08-14' AND '2016-12-24'
ORDER BY
    f.scheduled_departure DESC;

```
**3.** Reporte de vuelos ordenados por número de boarding tal que la reserva fue comprada con al menos 5 días de anticipación.

```
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
```

**4.** Reporte de revenue por vuelos que arribaron a destino con conteo de tickets y booking por fare (tramo).

```
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
```

**5.** Reporte de vuelos con tickets y bookings por fechas filtrado por un aircraft con cierto precio y con cierto tramo ordenado por el passenger name.

```
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
```

### Segundo grupo de consultas

**1.** Reporte de revenue por pasajero frecuente y su boarding order.

```
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
  tf.amount = (SELECT MAX(tf.amount) FROM ticket_flights tf) AND DATE(f.scheduled_departure) BETWEEN '2017-01-26' AND '2017-06-26'
GROUP BY
  t.passenger_name
HAVING
  COUNT(DISTINCT(tf.flight_id)) > 3
ORDER BY
  COUNT(DISTINCT(tf.flight_id)) DESC;
```

**2.** Reporte de revenue por vuelo que salen cuando se esperaba.

```
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
ORDER BY SUM(tf.amount) DESC, COUNT(DISTINCT(bp.ticket_no)) DESC;
```

**3.** Reporte de pasajeros con sus bookings.

```
SELECT
	t.passenger_name,
	SUM(b.total_amount) total_amount,
	COUNT(DISTINCT(b.book_ref)) count_bookings
FROM tickets t
JOIN bookings b ON b.book_ref = t.book_ref
GROUP BY t.passenger_name;
```

**4.** Reporte de dinero recaudado por asiento en un mes de vuelos.

```
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
```

**5.** Reporte de dinero recaudado en reservas por dia.

```
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
	SUM(b.total_amount) DESC;
```

Sistema gestor utilizado: *```PostgreSQL```*

Software para la administración de la base de datos recomendado: *```DBeaver```*

