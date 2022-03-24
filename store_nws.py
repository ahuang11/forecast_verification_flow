import xml.etree.cElementTree as et
from collections import defaultdict
from io import StringIO

import pandas as pd
import requests
from prefect import flow, task

URL_FMT = (
    "https://forecast.weather.gov/MapClick.php?lat={lat}&lon={lon}&FcstType=digitalDWML"
)


@task
def retrieve_data(lat, lon):
    url = URL_FMT.format(lat=lat, lon=lon)
    with requests.get(url) as resp:
        content = resp.content.decode()
    return content


@task
def parse_xml(content):
    with StringIO(content) as buf:
        tree = et.parse(buf)

    root = tree.getroot()
    mapping = defaultdict(lambda: [])
    for data in root.findall("data"):
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
def create_df(mapping):
    df = (
        pd.DataFrame(mapping)
        .pipe(
            lambda df: df.assign(
                **{"start_valid_time": pd.to_datetime(df["start_valid_time"])}
            )
        )
        .set_index("start_valid_time")
        .apply(pd.to_numeric, errors="ignore")
    )
    return df


@flow
def process_forecast(lat, lon):
    content = retrieve_data(lat, lon)
    mapping = parse_xml(content)
    df = create_df(mapping)
    return df


lat = 40.0157
lon = -105.2792
process_forecast(lat, lon)
