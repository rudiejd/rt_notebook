import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    return duckdb, mo


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        INSTALL AWS;
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        LOAD ICU;
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        LOAD aws;

        CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER credential_chain);

        ATTACH 's3://mbta-ctd-dataplatform-archive/lamp/catalog.db' AS lamp;
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE MACRO TO_HUMAN_TIME(unix_timestamp) AS
                	TO_TIMESTAMP(unix_timestamp)::TIMESTAMPTZ AT TIME ZONE 'US/Eastern'
        """
    )
    return


@app.cell
def _(mo):
    from datetime import datetime, timezone, timedelta
    yesterday = datetime.now() - timedelta(days=1)
    date_ui = mo.ui.date(label="Service Date", value=yesterday.date())
    range = mo.ui.date_range()
    route_id_ui = mo.ui.dropdown(['', 'Mattapan', 'Green-B', 'Green-C', 'Green-D', 'Green-E'], label="Route", value="Mattapan")
    vehicle_id_ui = mo.ui.text(label="Vehicle ID (blank for any)", value="")


    return date_ui, datetime, route_id_ui, timedelta, vehicle_id_ui


@app.cell
def _(date_ui, datetime, mo):
    start_time_ui = mo.ui.datetime(label="Start Time", value=datetime(day=date_ui.value.day, month=date_ui.value.month, year=date_ui.value.year))
    end_time_ui = mo.ui.datetime(label="End Time", value=datetime(day=date_ui.value.day, month=date_ui.value.month, year=date_ui.value.year))
    return end_time_ui, start_time_ui


@app.cell
def _(
    date_ui,
    datetime,
    end_time_ui,
    mo,
    route_id_ui,
    start_time_ui,
    timedelta,
    vehicle_id_ui,
):
    tu_df = mo.sql(
        f"""
        SELECT
            *,
            TO_HUMAN_TIME(feed_timestamp) as est_time
        FROM
              lamp.read_ymd("DEV_GREEN_RT_TRIP_UPDATES", DATE('{datetime.strftime(date_ui.value, "%Y-%m-%d")}'), DATE('{datetime.strftime(date_ui.value + timedelta(days=1), "%Y-%m-%d")}')) tu
        WHERE
               (LENGTH('{vehicle_id_ui.value}') == 0 OR tu."trip_update.vehicle.id" = '{vehicle_id_ui.value}')
                AND (LENGTH('{route_id_ui.value}') == 0 OR tu."trip_update.trip.route_id" = '{route_id_ui.value}')
                AND HOUR(est_time) >= {start_time_ui.value.hour}
                AND HOUR(est_time) <= {end_time_ui.value.hour}
                AND MINUTE(est_time) >= {start_time_ui.value.minute}
                AND MINUTE(est_time) <= {end_time_ui.value.minute}
        ORDER BY est_time
        """
    )
    return (tu_df,)


@app.cell
def _(tu_df):
    tu_df
    return


@app.cell
def _(
    date_ui,
    datetime,
    duckdb,
    end_time_ui,
    route_id_ui,
    start_time_ui,
    timedelta,
    vehicle_id_ui,
):
    vp_df = duckdb.sql(
        f"""
        SELECT
            *,
            TO_HUMAN_TIME(feed_timestamp) AS est_time
        FROM
        lamp.read_ymd("DEV_GREEN_RT_VEHICLE_POSITIONS", DATE('{datetime.strftime(date_ui.value, "%Y-%m-%d")}'), DATE('{datetime.strftime(date_ui.value + timedelta(days=1), "%Y-%m-%d")}')) vp
        WHERE
            (LENGTH('{vehicle_id_ui.value}') == 0 OR vp."vehicle.vehicle.id" = '{vehicle_id_ui.value}')
            AND (LENGTH('{route_id_ui.value}') == 0 OR vp."vehicle.trip.route_id" = '{route_id_ui.value}')
            AND AGE(est_time, '{start_time_ui.value}') >= '0 minutes'
            AND AGE(est_time, '{end_time_ui.value}') < '0 minutes'
         ORDER BY est_time
        """
    ).df()
    #vp_df = vp_df.merge(tu_df, how='left', left_on=['vehicle.vehicle.id', "vehicle.stop_id", 'feed_timestamp'], right_on=["trip_update.vehicle.id", "trip_update.stop_time_update.stop_id", "feed_timestamp"])
    return (vp_df,)


@app.cell
def _(
    date_ui,
    end_time_ui,
    mo,
    route_id_ui,
    start_time_ui,
    tu_df,
    vehicle_id_ui,
    vp_df,
):
    import leafmap
    import geopandas as gpd
    import pandas as pd

    plot_df = pd.DataFrame({'vehicle_id': vp_df['vehicle.vehicle.id'], 'direction_id': vp_df["vehicle.trip.direction_id"], 'trip': vp_df['vehicle.trip.trip_id'], 'next_stop_id': vp_df['vehicle.stop_id'], 'time': vp_df['est_time'], 'consist': vp_df["vehicle.vehicle.label"] })

    plot_df = plot_df.astype({'time': 'str'})

    gdf = gpd.GeoDataFrame(plot_df, geometry=gpd.points_from_xy(x=vp_df['vehicle.position.longitude'], y=vp_df['vehicle.position.latitude'], crs="EPSG:4326"))


    m = leafmap.Map(center=(42.361145, -71.057083), zoom=12, height="400px")
    m.add_tile_layer(url="https://cdn.mbta.com/osm_tiles/{z}/{x}/{y}.png", name="MassDOT", attribution="MassDOT")
    if gdf.size != 0:    
        m.add_gdf(gdf, layer_name="vps", )
    mo.vstack([vehicle_id_ui, route_id_ui, date_ui, start_time_ui, end_time_ui, m, tu_df])
    return


@app.cell
def _(vp_df):
    vp_df
    return


@app.cell
def _(mo, vp_df):
    _df = mo.sql(
        f"""
        SELECT MIN(est_time), vp_df."vehicle.trip.trip_id" trip_id, vp_df."vehicle.vehicle.id" vehicle_id FROM vp_df
        WHERE vp_df."vehicle.trip.schedule_relationship" != 'ADDED'
        GROUP BY trip_id, vehicle_id
        """
    )
    return


if __name__ == "__main__":
    app.run()
