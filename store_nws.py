import sqlite3
from datetime import timedelta

import requests
import pandas as pd
from dateutil.tz import gettz
from dateutil.parser import parse
from prefect import Flow, Parameter, task, unmapped
from prefect.schedules import IntervalSchedule
from prefect.engine.results import LocalResult
from prefect.engine.signals import SKIP
from prefect.run_configs import UniversalRun

DATABASE_PATH = "forecast.db"
TABLE_QUERY = "SELECT name FROM sqlite_master WHERE type='table' AND name='updates';"
UPDATES_QUERY = "SELECT stn_id FROM updates WHERE source='nws' AND update_dt=? LIMIT 1"

METADATA_URL = (
    "https://mesonet.agron.iastate.edu/sites/networks.php?"
    "network=_ALL_&format=csv&nohtml=on"
)

DATA_FMT = (
    "https://forecast.weather.gov/MapClick.php?"
    "lat={lat}&lon={lon}&lg=english&&FcstType=digital"
)

TZINFOS = {
    "PST": gettz("US/Pacific"),
    "PDT": gettz("US/Pacific"),
    "PT": gettz("US/Pacific"),
    "MST": gettz("US/Mountain"),
    "MDT": gettz("US/Mountain"),
    "MT": gettz("US/Mountain"),
    "CST": gettz("US/Central"),
    "CDT": gettz("US/Central"),
    "CT": gettz("US/Central"),
    "EST": gettz("US/Eastern"),
    "EDT": gettz("US/Eastern"),
    "ET": gettz("US/Eastern"),
}

@task(max_retries=3,
      retry_delay=timedelta(minutes=1),
      target="stn_meta.csv",
      checkpoint=True,
      result=LocalResult(dir="~/.prefect")
      )
def retreive_meta():
    stn_df = pd.read_csv(METADATA_URL)
    return stn_df


@task
def lookup_lat_lon(stn_df, stn_id):
    """
    Lookup lat / lon for a given station id.
    """
    stn_row = stn_df.query(f"stid == '{stn_id}'").iloc[0]
    return {"lat": stn_row["lat"], "lon": stn_row["lon"]}


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def retrieve_data(coords):
    """
    Get tabular data from NWS.
    """
    df_list = pd.read_html(DATA_FMT.format(**coords))
    return df_list


def _to_utc(time):
    """
    Convert timezone aware datetimes into UTC.
    """
    return pd.to_datetime(parse(time, tzinfos=TZINFOS)).tz_convert("UTC")


@task
def check_exists(df_list):
    """
    Check if data already exists in database; if so, skip.
    """
    _, update_time = df_list[3].iloc[0]
    update_dt = _to_utc(update_time.split(":", 1)[1])

    with sqlite3.connect(DATABASE_PATH) as con:
        table_exists = len(pd.read_sql(TABLE_QUERY, con)) > 0
        if table_exists:
            update_df = pd.read_sql(
                UPDATES_QUERY,
                con,
                params=[update_dt.strftime("%Y-%m-%d %H:%M:%S")],
            )
            if len(update_df) > 0:
                raise SKIP()

    return update_dt


@task
def subset_forecast(df_list):
    """
    Subset the forecast dataframe.
    """
    df = df_list[7].T
    return df


@task
def drop_empty(df):
    """
    Remove empty columns.
    """
    df = df.drop(columns=[0, 17])
    return df


@task
def rename_columns(df):
    """
    Replace the first row as column names,
    lowercasing the column names, removing text in parentheses,
    and replacing spaces with underscores.
    """
    df.columns = (
        df.iloc[0]
        .str.lower()
        .str.split("(", expand=True)[0]
        .str.strip()
        .str.replace(" ", "_")
    )
    df = df.drop([0])
    return df


@task
def rejoin_data(df):
    """
    The two tables were mangled as separate columns in the same df
    so re-separate them and join them as rows.
    """
    df = pd.concat([df.iloc[:, :16], df.iloc[:, 16:]]).ffill()
    return df


def _get_tau(df, update_dt):
    """
    Gets forecast step as hours.
    """
    return (df.index - update_dt).total_seconds() / 60 / 60


@task
def parse_timę(df, update_dt):
    """
    Create a datetime object from dataframe columns.
    """
    df.index = [
        _to_utc(f"{update_dt.year}/{row['date']} {row['hour']}, {update_dt.tzname()}")
        for _, row in df.iterrows()
    ]
    df = df.drop(columns=["date", "hour"])
    df["tau"] = _get_tau(df, update_dt)
    return df


@task
def fix_time(df, update_dt):
    """
    Since the year is inferred, add some logic to correct it during a new year.
    """
    df["valid"] = df.index
    rewind_idx = df["tau"] > 100 * 24
    df.loc[rewind_idx, "valid"] = df.loc[rewind_idx].index - pd.DateOffset(years=1)

    forward_idx = df["tau"] < -100 * 24
    df.loc[forward_idx, "valid"] = df.loc[forward_idx].index + pd.DateOffset(years=1)

    df = df.set_index("valid")
    df["tau"] = _get_tau(df, update_dt)
    return df


@task
def export_data(df):
    """
    Save forecast data to database.
    
    """
    with sqlite3.connect(DATABASE_PATH) as con:
        df.to_sql("nws", con, if_exists="append")


@task
def export_metadata(stn_id, update_dt):
    """
    Save forecast data to database.
    """
    with sqlite3.connect(DATABASE_PATH) as con:
        pd.DataFrame(
            {
                "stn_id": [stn_id],
                "source": ["nws"],
                "update_dt": [update_dt.tz_localize(None)],
            }
        ).set_index("stn_id").to_sql("updates", con, if_exists="append")


schedule = IntervalSchedule(interval=timedelta(minutes=30))
with Flow("store_nws", schedule=schedule) as flow:
    flow.run_config = UniversalRun(labels=["nws"])
    stn_ids = Parameter("stn_id", default=["CMI", "ORD", "SEA"])

    stn_df = retreive_meta()
    coords = lookup_lat_lon.map(unmapped(stn_df), stn_ids)

    df_list = retrieve_data.map(coords)
    update_dt = check_exists.map(df_list)

    df = subset_forecast.map(df_list)
    df = drop_empty.map(df)
    df = rename_columns.map(df)
    df = rejoin_data.map(df)
    df = parse_timę.map(df, update_dt)
    df = fix_time.map(df, update_dt)

    export_metadata.map(stn_ids, update_dt)
    export_data.map(df)

flow.register("forecast_verification")
