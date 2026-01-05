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
    date_ui = mo.ui.datetime()

    from datetime import datetime, timezone, timedelta
    return date_ui, datetime, timedelta


@app.cell
def _(date_ui, datetime, mo, route_id_ui, timedelta):
    tu_df = mo.sql(
        f"""
        SELECT
            *,
            TO_HUMAN_TIME(feed_timestamp) as est_time
        FROM
              lamp.read_ymd("DEV_GREEN_RT_TRIP_UPDATES", DATE('{datetime.strftime(date_ui.value, "%Y-%m-%d")}'), DATE('{datetime.strftime(date_ui.value + timedelta(days=1), "%Y-%m-%d")}')) tu
        WHERE
            tu."trip_update.trip.route_id" = '{route_id_ui.value}'
            AND HOUR(est_time) = {date_ui.value.hour}
            AND MINUTE(est_time) = {date_ui.value.minute}
        ORDER BY est_time
        """
    )
    return (tu_df,)


@app.cell
def _(date_ui, datetime, mo, route_id_ui, timedelta):
    vp_df = mo.sql(
        f"""
        SELECT
            *,
            TO_HUMAN_TIME(feed_timestamp) AS est_time
        FROM
        lamp.read_ymd("DEV_GREEN_RT_VEHICLE_POSITIONS", DATE('{datetime.strftime(date_ui.value, "%Y-%m-%d")}'), DATE('{datetime.strftime(date_ui.value + timedelta(days=1), "%Y-%m-%d")}')) vp
        WHERE
            vp.day = {date_ui.value.day}
            AND vp.month = {date_ui.value.month}
            AND vp.year = {date_ui.value.year}
            AND vp."vehicle.trip.route_id" = '{route_id_ui.value}'
            AND HOUR(est_time) = {date_ui.value.hour}
            AND MINUTE(est_time) = {date_ui.value.minute}
        ORDER BY est_time
        """
    )
    return (vp_df,)


@app.cell
def _(mo):
    route_id_ui = mo.ui.dropdown(options=["Mattapan", "Green-B", "Green-C", "Green-D", "Green-E"], value="Mattapan", label="Route")
    route_id_ui
    return (route_id_ui,)


@app.cell
def _(date_ui):
    date_ui
    return


@app.cell
def _(date_ui, mo, route_id_ui, tu_df, vp_df):
    import leafmap
    import geopandas as gpd
    import pandas as pd

    label_col = "id="+ vp_df['vehicle.vehicle.id'] + "\n time=" + vp_df['est_time'] + "\n trip=" + vp_df['vehicle.trip.trip_id'] + "\n next_stop_id= " + vp_df['vehicle.stop_id'] + " direction=" + vp_df["vehicle.trip.direction_id"]

    plot_df = pd.DataFrame({'vehicle_id': vp_df['vehicle.vehicle.id'], 'direction_id': vp_df["vehicle.trip.direction_id"], 'trip': vp_df['vehicle.trip.trip_id'], 'next_stop_id': vp_df['vehicle.stop_id'], 'time': vp_df['est_time'] })

    plot_df = plot_df.astype({'time': 'str'})

    gdf = gpd.GeoDataFrame(plot_df, geometry=gpd.points_from_xy(x=vp_df['vehicle.position.longitude'], y=vp_df['vehicle.position.latitude'], crs="EPSG:4326"))


    m = leafmap.Map(center=(42.361145, -71.057083), zoom=12, height="400px")
    m.add_tile_layer(url="https://cdn.mbta.com/osm_tiles/{z}/{x}/{y}.png", name="MassDOT", attribution="MassDOT")
    m.add_gdf(gdf, layer_name="vps")
    mo.vstack([route_id_ui, date_ui, m, tu_df])
    return


if __name__ == "__main__":
    app.run()
