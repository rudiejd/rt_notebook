import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        INSTALL AWS;

        LOAD ICU;

        LOAD aws;

        CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER credential_chain);

        ATTACH 's3://mbta-ctd-dataplatform-archive/lamp/catalog.db' AS lamp;

        CREATE OR REPLACE MACRO TO_HUMAN_TIME (unix_timestamp) AS TO_TIMESTAMP(unix_timestamp)::TIMESTAMPTZ AT TIME ZONE 'US/Eastern'
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    1. Start with the trainsheets from Glides, listing all scheduled Boston College pull-outs
       - We actually only care about the trip after the actual pull-out since the trip from the yard to BC doesn't get predictions
    3. For each trip after pull-out, check against LAMP trip updates to see if it was predicted
       - If there were predictions before the vehicle left the terminal (stop was no longer `

    5. Count the number that were predicted, and the number that weren't
    6. For trip that wasn't predicted, figure out the reason it wasn't predicted
       - Vehicle did not exist
       - Trainsheet was not updated before vehicle's departure
       - Consist was not correct
       - Other
    """)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE TABLE trainsheet AS
        SELECT
            *
        FROM
            read_csv(
                '~/Downloads/trainsheet-2026-02-26-2026-03-05.csv', all_varchar=true
            );
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE TABLE trips AS
        SELECT
            *
        FROM
            read_csv(
                '~/git/gtfs_creator/output/developer/trips.txt',
                sample_size = -1
            )
        WHERE
            route_id = 'Green-B';

        CREATE TABLE stop_times AS
        SELECT * FROM
        read_csv('~/git/gtfs_creator/output/developer/stop_times.txt', sample_size=-1) st
        JOIN trips t ON st.trip_id = t.trip_id;
        """
    )
    return


@app.cell
def _(mo, stop_times):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE pullout_trips (trip_id VARCHAR, service_type VARCHAR);

        -- LOL NEVERMIND, let's just do it manually
        INSERT INTO
            pullout_trips
        VALUES
            ('73220606', 'Weekday'),
            ('73221467', 'Weekday'),
            ('73220604', 'Weekday'),
            ('73220602', 'Weekday'),
            ('73220600', 'Weekday'),
            ('73220598', 'Weekday'),
            ('73220596', 'Weekday'),
            ('73220358', 'Weekday'),
            ('73220394', 'Weekday'),
            ('73220419', 'Weekday'),
            ('73220426', 'Weekday'),
            ('73220482', 'Weekday');

        CREATE OR REPLACE TABLE pullout_trips AS
        SELECT pt.trip_id, MIN(departure_time) bc_departure FROM pullout_trips pt
        JOIN stop_times st ON st.trip_id = pt.trip_id
        GROUP BY pt.trip_id;
        """
    )
    return


@app.cell
def _(mo, pullout_trips):
    _df = mo.sql(
        f"""
        CREATE TABLE pullout_trips_with_date AS
        SELECT
            GENERATE_SERIES AS service_date,
            trip_id,
            strptime(bc_departure, '%H:%M:%S') dep_time,
            MAKE_TIMESTAMP(YEAR(service_date), MONTH(service_date), DAY(service_date), HOUR(dep_time), MINUTE(dep_time), SECOND(dep_time)) departure
        FROM
            GENERATE_SERIES(
                TIMESTAMP '2026-02-26',
                TIMESTAMP '2026-03-05',
                INTERVAL '1 DAY'
            )
            CROSS JOIN pullout_trips
        WHERE
            DAY(service_date)
            AND DAYOFWEEK(service_date) BETWEEN 1 AND 4;

        -- 60 records - 12 pullout trips each day for 5 days.
        SELECT
            *
        FROM
            pullout_trips_with_date;
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE TABLE trip_updates AS
        SELECT * FROM lamp.read_ymd (
            'DEV_GREEN_RT_TRIP_UPDATES',
            DATE('2026-02-26'),
            DATE('2026-03-06')
        ) tu
        -- only boston college or south st inbound predictions
        WHERE tu."trip_update.stop_time_update.stop_id" = '70106' AND tu."trip_update.trip.direction_id" = '1'
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE TABLE vehicle_positions AS
        SELECT * FROM lamp.read_ymd (
            'DEV_GREEN_RT_VEHICLE_POSITIONS',
            DATE('2026-02-26'),
            DATE('2026-03-06')
        ) vp
        WHERE vp."vehicle.stop_id" IN ('70106');
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE TABLE vehicle_first_departure AS
        SELECT
            vp."vehicle.vehicle.id" vehicle_id,
            day,
            month,
            TO_HUMAN_TIME (MIN(vp.feed_timestamp)) first_departure
        FROM
            lamp.read_ymd (
                'DEV_GREEN_RT_VEHICLE_POSITIONS',
                DATE('2026-02-26'),
                DATE('2026-03-06')
            ) vp
        WHERE
            vp."vehicle.stop_id" = '70110'
            AND "vehicle.trip.direction_id" = 1
            AND HOUR(TO_HUMAN_TIME (vp.feed_timestamp)) >= 4 
            AND HOUR(TO_HUMAN_TIME(vp.feed_timestamp)) <= 8
        GROUP BY
            vehicle_id,
            day,
        	month
        """
    )
    return


@app.cell
def _(mo, vehicle_first_departure):
    _df = mo.sql(
        f"""
        SELECT * FROM vehicle_first_departure
        """
    )
    return


@app.cell
def _(mo, pullout_trips_with_date, trip_updates, vehicle_first_departure):
    _df = mo.sql(
        f"""
        CREATE TABLE first_prediction_by_day AS
        SELECT
            trip_id,
            service_date,
            tu."trip_update.trip.start_time" start_time,
            tu."trip_update.vehicle.id" vehicle_id,
            -- First Boston College terminal prediction
            TO_HUMAN_TIME (MIN(tu.feed_timestamp)) first_prediction,
            first_departure,
            first_prediction IS NOT NULL
            and first_departure IS NOT NULL
            AND first_prediction + INTERVAL 5 MINUTES < first_departure AS predicted,

        FROM
            pullout_trips_with_date pt
            LEFT JOIN trip_updates tu ON tu."trip_update.trip.trip_id" = pt.trip_id
            JOIN vehicle_first_departure dp ON dp.vehicle_id = tu."trip_update.vehicle.id"
        WHERE
            DAY(service_date) = tu.day
            AND MONTH(service_date) = tu.month
            AND YEAR(service_date) = tu.year
            AND DAY(service_date) = dp.day
            AND MONTH(service_date) = dp.month
        GROUP BY
            trip_id,
            service_date,
            first_departure,
            tu."trip_update.vehicle.id",
            start_time;

        SELECT
            *
        FROM
            first_prediction_by_day;
        """
    )
    return


@app.cell
def _(mo, pullout_trips_with_date):
    _df = mo.sql(
        f"""
        -- What's the vehicle and consist a trip eventually got when that trip did get predictions?
        CREATE TABLE vehicles_for_unpredicted_trips AS SELECT
            trip_id,
            departure,
            service_date,
            MIN_BY(tu."trip_update.vehicle.id", tu.feed_timestamp) vehicle_id,
            MIN_BY(tu."trip_update.vehicle.label", tu.feed_timestamp) consist,
            TO_HUMAN_TIME(MIN(tu.feed_timestamp)) first_prediction
        FROM
            pullout_trips_with_date
            LEFT JOIN lamp.read_ymd (
                'DEV_GREEN_RT_TRIP_UPDATES',
                DATE('2026-02-26'),
                DATE('2026-03-05')
            ) tu ON trip_id = tu."trip_update.trip.trip_id"
        WHERE
            DAY(service_date) = tu.day
            OR tu.day IS NULL
            AND MONTH(service_date) = tu.month
            OR tu.day IS NULL
            AND YEAR(service_date) = tu.year
            OR tu.day IS NULL
        GROUP BY
            trip_id, service_date, departure
        ORDER BY service_date;
        """
    )
    return


@app.cell
def _(mo, vehicles_for_unpredicted_trips):
    _df = mo.sql(
        f"""
        -- for the vehicles that ended up making the pullout trip, WHEN did they first exist (regardless of what trip they were assigned to)
        CREATE TABLE vehicle_creation_data AS SELECT
            trip_id,
            vehicle_id,
            service_date,
            TO_HUMAN_TIME (MIN(vp.feed_timestamp)) AS creation_time,
            departure,
            -- the consist when the vehicle spawned in   
            MIN_BY(vp."vehicle.vehicle.label", vp.feed_timestamp) AS first_consist,
            -- the consist when we actually started predicting the trip
            consist AS correct_consist,
            first_consist == correct_consist had_correct_consist,
            creation_time < departure AS vehicle_existed,
            creation_time + INTERVAL 5 MINUTES < departure AS vehicle_existed_5min
        FROM
            vehicles_for_unpredicted_trips
            INNER JOIN lamp.read_ymd (
                'DEV_GREEN_RT_VEHICLE_POSITIONS',
                DATE('2026-02-26'),
                DATE('2026-03-05')
            ) vp ON vp."vehicle.vehicle.id" = vehicle_id
        WHERE
            DAY(service_date) = DAY(TO_HUMAN_TIME (vp.feed_timestamp))
            AND MONTH(service_date) = MONTH(TO_HUMAN_TIME (vp.feed_timestamp))
            AND YEAR(service_date) = YEAR(TO_HUMAN_TIME (vp.feed_timestamp))
            AND vp."vehicle.stop_id" = '70106'
            -- ignore positions from previous service date
            AND HOUR(TO_HUMAN_TIME (vp.feed_timestamp)) >= 4
        GROUP BY
            trip_id,
            vehicle_id,
            departure,
            service_date,
            correct_consist;
        """
    )
    return


@app.cell
def _(mo, vehicle_creation_data, vehicles_for_unpredicted_trips):
    _df = mo.sql(
        f"""
        -- now read in the consist data from Glides. For the trips that did get assigned, when was a consist first entered for that trip?
        SELECT
            vut.trip_id,
            vut.service_date,
            first_consist,
            correct_consist,
            vut.departure,
            creation_time,
            MIN(time) - INTERVAL 5 HOURS first_consist_entered,
            COALESCE(vehicle_existed_5min, FALSE) AS vehicle_existed_5min_before_departure,
            first_consist_entered < vut.departure had_consist_entry_before,
            first_consist_entered + INTERVAL 5 MINUTES < vut.departure had_consist_entry_5min_before
        FROM
            vehicles_for_unpredicted_trips vut
            LEFT JOIN vehicle_creation_data vcd ON vut.trip_id = vcd.trip_id
            AND vut.service_date = vcd.service_date
            LEFT JOIN lamp.main.trip_updates tug ON tug."data.tripUpdates.tripKey.tripId" = vut.trip_id
            AND DATE(
                strptime(
                    tug."data.tripUpdates.tripKey.serviceDate",
                    '%Y-%m-%d'
                )
            ) = DATE(vut.service_date)
        WHERE
            tug."data.tripUpdates.cars" IS NOT NULL
        GROUP BY
            vut.trip_id,
            vut.service_date,
            vut.departure,
            first_consist,
            correct_consist,
            creation_time,
            vehicle_existed_5min,
        ORDER BY
            vut.service_date
        """
    )
    return


@app.cell
def _(first_prediction_by_day, mo):
    _df = mo.sql(
        f"""
        -- for only the departures that did get predictions, when was their consist entered into glides?
        WITH deps AS (SELECT
            trip_id,
            service_date,
            MIN(time) - INTERVAL 5 HOURS first_consist_entered,
            fp.first_prediction
        FROM
            first_prediction_by_day fp
            LEFT JOIN lamp.main.trip_updates tug ON tug."data.tripUpdates.tripKey.tripId" = trip_id
            AND DATE(strptime(tug."data.tripUpdates.tripKey.serviceDate", '%Y-%m-%d')) = DATE(service_date) 
        WHERE
            glides_consist IS NOT NULL
        GROUP BY trip_id, service_date, first_prediction,
        ORDER BY service_date)
        SELECT AVG(first_prediction - first_consist_entered)
        FROM deps
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE TABLE gtfs_trips AS SELECT * FROM read_csv('~/git/gtfs_creator/output/developer/trips.txt', sample_size=-1)
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE gtfs_trip_last_stop AS SELECT MAX_BY(stop_id, stop_sequence) expected_last_stop, trip_id 
        FROM read_csv('~/git/gtfs_creator/output/developer/stop_times.txt', sample_size=-1)
        GROUP BY trip_id
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE stops AS SELECT *
        FROM read_csv('~/git/gtfs_creator/output/developer/stops.txt', sample_size=-1);
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT "trip_update.trip.route_id" route_id, "trip_update.stop_time_update.schedule_relationship" schedule_relationship, MAX("trip_update.trip.start_time"), COUNT(*), FROM lamp.read_ymd (
                'RT_TRIP_UPDATES',
                DATE('2026-03-17'),
                DATE('2026-03-18')
            )
        WHERE "trip_update.vehicle.id" IS NULL AND schedule_relationship IS NULL
        GROUP BY route_id, schedule_relationship
        ORDER BY route_id, COUNT(*) DESC
        """
    )
    return


app._unparsable_cell(
    r"""
    _df = mo.sql(
        f\"\"\"
        SELECT DISTINCT \"trip_update.route_id\" FROM {df
        \"\"\",
        output=False
    )
    """,
    name="_"
)


if __name__ == "__main__":
    app.run()
