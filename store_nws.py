from typing import Tuple
import sqlite3
import xml.etree.cElementTree as et
from collections import defaultdict
from io import StringIO
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.deployments import DeploymentSpec


META_URL = (
    "https://mesonet.agron.iastate.edu/sites/networks.php?"
    "network=_ALL_&format=csv&nohtml=on"
)
DATA_URL_FMT = "https://forecast.weather.gov/MapClick.php?lat={lat}&lon={lon}&FcstType=digitalDWML"
TEMPORAL_COLS = ("start_valid_time", "initialization_time", "forecast_hour")
DATABASE_NAME = "nws_forecast.db"
DATABASE_DIR = Path.home() / "Data"
DATABASE_PATH = DATABASE_DIR / DATABASE_NAME
STATION_IDS = ("KSEA", "KBDU", "KORD", "KCMI", "KMRY", "KSAN", "KNYC", "KIND", "KHOU")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=365))
def retrieve_meta():
    meta_df = pd.read_csv(META_URL)
    return meta_df


@task(cache_key_fn=task_input_hash)
def get_station_coords(stid, meta_df):
    lon, lat = meta_df.loc[meta_df["stid"] == stid, ["lon", "lat"]].iloc[0]
    return lon, lat


@task
def retrieve_data(lon, lat):
    url = DATA_URL_FMT.format(lon=lon, lat=lat)
    with requests.get(url) as resp:
        data = resp.content.decode()
    return data


@task
def get_root(data):
    with StringIO(data) as buf:
        tree = et.parse(buf)
    root = tree.getroot()
    return root


@task
def extract_initialization_time(root):
    for head in root.findall("head"):
        for product in head.findall("product"):
            for creation_date in product.findall("creation-date"):
                initialization_time = pd.to_datetime(creation_date.text)
    return initialization_time


@task
def extract_params(root):
    mapping = defaultdict(lambda: [])
    for data in root.findall('data'):
        for time_layout in data.findall("time-layout"):
            for start_valid_time in time_layout.findall("start-valid-time"):
                mapping["start_valid_time"].append(start_valid_time.text)

        for parameters in data.findall("parameters"):
            for parameter in parameters:
                tag = parameter.tag.replace("-", "_")
                type_ = parameter.attrib.get("type")

                for value in parameter.findall("value"):
                    if type_ is None:
                        break
                    mapping[f"{tag}_{type_}"].append(value.text)

                for weather_conditions in parameter.findall("weather-conditions"):
                    if weather_conditions.attrib:  # is nil
                        mapping["weather"].append("")
                    else:
                        for i, value in enumerate(weather_conditions.findall("value")):
                            text = " ".join(value.attrib.values())
                            if i == 0:
                                mapping["weather"].append(text)
                            else:
                                mapping["weather"][-1] += f" and {text}"
    return mapping


@task
def create_df(mapping, initialization_time):
    df = pd.DataFrame(mapping).pipe(
        lambda df: df.assign(**{
            "initialization_time": pd.to_datetime([initialization_time] * len(df)),
            "start_valid_time": pd.to_datetime(df["start_valid_time"])
        })
    ).pipe(
        lambda df: df.assign(**{
            "forecast_hour": (
                df["start_valid_time"] - df["initialization_time"]
            ).dt.total_seconds() / 3600
        })
    ).set_index(
        ["start_valid_time", "initialization_time"]
    ).apply(
        pd.to_numeric,
        errors="ignore"
    ).reset_index()
    return df


@task(retries=3, retry_delay_seconds=10)
def to_database(stid, initialization_time, df):
    logger = get_run_logger()

    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving data to {DATABASE_PATH}")
    with sqlite3.connect(DATABASE_PATH) as con:
        table_row = con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{stid}';")
        table = table_row.fetchone()
        if table is not None:
            row = con.execute(f"SELECT initialization_time FROM {stid} ORDER BY rowid DESC LIMIT 1;")
            existing_initialization_time = pd.to_datetime(row.fetchone()[0])
            if existing_initialization_time == initialization_time:
                logger.warning(f"{initialization_time} already exists in the database for {stid}, skipping")
                return

        df.to_sql(stid, con, index=False, if_exists="append")
        for col in TEMPORAL_COLS:
            con.execute(f"CREATE INDEX IF NOT EXISTS {col}_index ON {stid}({col});")


@flow
def process_forecasts(stids: Tuple = STATION_IDS):
    for stid in stids:
        stid = stid.upper()

        meta_df = retrieve_meta()
        lon, lat = get_station_coords(stid, meta_df).result()

        data = retrieve_data(lon, lat)
        root = get_root(data)

        initialization_time = extract_initialization_time(root)
        mapping = extract_params(root)

        df = create_df(mapping, initialization_time)
        to_database(stid, initialization_time, df)


DeploymentSpec(
    flow=process_forecasts,
    name="hourly-deployment",
    tags=["nws", "forecast"],
    schedule=IntervalSchedule(interval=timedelta(hours=1)),
)
