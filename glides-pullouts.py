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
            trip_id
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
def _(mo, pullout_trips):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE bc_departures AS (
            SELECT
                "vehicle.vehicle.id" vehicle_id,
                "vehicle.trip.trip_id" trip_id,
                TO_HUMAN_TIME (MIN(feed_timestamp)) AS departure,
                year,
                month,
                day
            FROM
                lamp.read_ymd (
                    'DEV_GREEN_RT_VEHICLE_POSITIONS',
                    DATE('2026-02-26'),
                    DATE('2026-03-05')
                )
            INNER JOIN pullout_trips pt ON pt.trip_id = "vehicle.trip.trip_id"
            WHERE
                -- boston college park st platform - we only care about arrival and departure here
                "vehicle.stop_id" != '70106'
            GROUP BY
                "vehicle.trip.trip_id",
                vehicle_id,
                year,
                month,
                day
        );
        """
    )
    return


@app.cell
def _(bc_departures, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM bc_departures LIMIT 5;
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
            DATE('2026-03-05')
        ) tu ON pt.trip_id = tu."trip_update.trip.trip_id"
        """
    )
    return


@app.cell
def _(bc_departures, mo, pullout_trips_with_date):
    _df = mo.sql(
        f"""
        SET disabled_optimizers = 'join_order,build_side_probe_side';
        SELECT
            pt.trip_id,
            service_date,
            tu."trip_update.trip.trip_id" tu_trip_id,
            strptime(
                "trip_update.trip.start_date" || ' ' || "trip_update.trip.start_time",
                '%Y%m%d %I:%M:%S'
            ) scheduled_time,
            -- time of first prediction FOR THE TRIP
            MIN(TO_HUMAN_TIME (tu.feed_timestamp)) first_prediction,
            -- time of first vehicle ping FOR THE VEHICLE ON THE PULLOUT TRIP
            MIN(TO_HUMAN_TIME (vp.feed_timestamp)) first_vehicle_position,
            concat(tu.year, '-', tu.month, '-', tu.day) date,
            -- departure = time that the pullout trip, or another
            first_prediction < departure,
            -- vehicle existed 5 minutes before departure from BC
            first_vehicle_position + INTERVAL 5 minutes < departure vehicle_existed,
            departure
        FROM
            pullout_trips_with_date pt
            LEFT JOIN lamp.read_ymd (
                'DEV_GREEN_RT_TRIP_UPDATES',
                DATE('2026-02-26'),
                DATE('2026-03-05')
            ) tu ON pt.trip_id = tu."trip_update.trip.trip_id"
            LEFT JOIN lamp.read_ymd (
                'DEV_GREEN_RT_VEHICLE_POSITIONS',
                DATE('2026-02-26'),
                DATE('2026-03-05')
            ) vp ON vp."vehicle.vehicle.id" = tu."trip_update.vehicle.id"
                AND vp.year = tu.year
                AND vp.month = tu.month
                AND vp.day = tu.day
            LEFT JOIN bc_departures dp ON dp.trip_id = pt.trip_id
            	AND dp.year = tu.year
                AND dp.month = tu.month
                AND dp.day = tu.day
        WHERE
            -- Boston College
            vp."vehicle.stop_id" = '70106' AND
            tu."trip_update.stop_time_update.stop_id" = '70106'
        GROUP BY
            pt.trip_id,
            service_date,
            tu_trip_id,
            scheduled_time,
            date,
            departure
        HAVING
            first_vehicle_position >= first_prediction - INTERVAL 20 MINUTES
            AND departure <= first_prediction + INTERVAL 20 MINUTES
        ORDER BY
            date
        """
    )
    return


if __name__ == "__main__":
    app.run()
