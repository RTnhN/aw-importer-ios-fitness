#!/usr/bin/env python3

# ruff: noqa: T201 DTZ007 G004

import csv
import logging
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

import pandas as pd
from aw_client.client import ActivityWatchClient
from aw_core import dirs
from aw_core.models import Event
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

WATCHER_NAME = "aw-importer-ios-fitness"

logger = logging.getLogger(WATCHER_NAME)
DEFAULT_CONFIG = f"""
[{WATCHER_NAME}]
data_path = ""
"""
FILE_PREFIX = "HKWorkoutActivityType"


def _parse_and_add_data(aw: ActivityWatchClient, bucket_name: str, path: Path) -> None:
    already_logged_events = {
        event["data"]["uid"] for event in aw.get_events(bucket_name)
    }
    added_logs = 0
    batch_events = []  # For batch processing

    df_fitness = pd.read_csv(path, skiprows=1)
    df_fitness = df_fitness.fillna("")
    for index, row in df_fitness.iterrows():
        try:
            start_date = row.get("startDate")
            end_date = row.get("endDate")
            activity_type = row.get("activityType", "") or ""
            product_type = row.get("productType", "") or ""
            source_name = row.get("sourceName", "") or ""
            source_version = row.get("sourceVersion", "") or ""
            total_energy_burned = row.get("totalEnergyBurned", "") or ""
            total_distance = row.get("totalDistance", "") or ""
            flights_climbed = row.get("totalFlightsClimbed", "") or ""
            timezone = row.get("HKTimeZone", "") or ""
            average_mets = row.get("HKAverageMETs", "") or ""
            weather_temp = row.get("HKWeatherTemperature", "") or ""
            weather_humidity = row.get("HKWeatherHumidity", "") or ""

            uid = f"{start_date}_{end_date}_{activity_type}"
            if uid in already_logged_events:
                continue  # it's already logged
            fmt = "%Y-%m-%d %H:%M:%S %z"

            start_td = datetime.strptime(start_date, fmt)
            end_td = datetime.strptime(end_date, fmt)

            session_duration = end_td - start_td
            data = {
                "title": f"{activity_type}",
                "uid": uid,
                "product_type": product_type,
                "source_name": source_name,
                "source_version": source_version,
                "total_energy_burned": total_energy_burned,
                "total_distance": total_distance,
                "flights_climbed": flights_climbed,
                "timezone": timezone,
                "average_mets": average_mets,
                "weather_temp": weather_temp,
                "weather_humidity": weather_humidity,
            }
            event = Event(
                timestamp=start_td.isoformat(),
                duration=int(session_duration.total_seconds()),
                data=data,
            )
            batch_events.append(event)
            added_logs += 1
        except Exception as e:
            # print(f"There was a problem with the following row: {row}")
            print(e)
            continue

        # Batch insert if supported
    if batch_events:
        aw.insert_events(bucket_name, batch_events)

    print_statusline(f"Added {added_logs} item(s)")


def _load_config() -> dict:
    from aw_core.config import load_config_toml

    return load_config_toml(WATCHER_NAME, DEFAULT_CONFIG)


def print_statusline(msg) -> None:
    last_msg_length = (
        len(print_statusline.last_msg) if hasattr(print_statusline, "last_msg") else 0
    )
    print(" " * last_msg_length, end="\r")
    print(msg, end="\r")
    print_statusline.last_msg = msg


class CSVFileHandler(FileSystemEventHandler):
    """Custom event handler for watchdog to process new or modified CSV files."""

    def __init__(
        self,
        aw: ActivityWatchClient,
        bucket_name: str,
        data_path: Path,
    ) -> None:
        """Initialize the CSVFileHandler.

        This accepts the ActivityWatchClient, bucket name, and data path.
        """
        self.aw = aw
        self.bucket_name = bucket_name
        self.data_path = data_path

    def on_created(self, event: FileSystemEvent) -> None:
        """Call when a new file or folder is created."""
        self.process(event)

    def process(self, event: FileSystemEvent) -> None:
        """Process the file if it's a CSV that hasn't been imported yet."""
        if not event.is_directory and event.src_path.endswith(".csv"):
            file_path = Path(event.src_path)
            if not file_path.stem.endswith("_imported"):
                _parse_and_add_data(self.aw, self.bucket_name, file_path)
                file_path.rename(
                    self.data_path
                    / Path(file_path.stem + "_imported" + file_path.suffix)
                )


def main() -> None:
    """Run the main entry point for the aw-importer-foqos script."""
    logging.basicConfig(level=logging.INFO)

    config_dir = dirs.get_config_dir(WATCHER_NAME)
    config = _load_config()
    data_path = config[WATCHER_NAME].get("data_path", "")

    if not data_path:
        logger.warning(
            f"""You need to specify the folder that has the data files.
                       You can find the config file here: {config_dir}""",
        )
        sys.exit(1)

    aw = ActivityWatchClient(WATCHER_NAME, testing=False)
    bucket_name = f"{aw.client_name}_{aw.client_hostname}"
    if aw.get_buckets().get(bucket_name) is None:
        aw.create_bucket(bucket_name, event_type="lifecycle_data", queued=True)
    aw.connect()

    # Set up watchdog observer
    event_handler = CSVFileHandler(aw, bucket_name, Path(data_path))
    observer = Observer()
    observer.schedule(event_handler, data_path, recursive=True)
    observer.start()

    try:
        while True:
            sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
