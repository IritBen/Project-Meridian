import xml.etree.ElementTree as ET
import csv
import json
import time
from typing import BinaryIO
from urllib.error import URLError
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import quote
from shutil import copyfileobj, rmtree
from tempfile import TemporaryFile, mkdtemp
from zipfile import ZipFile, ZipInfo
from meridian.trip_windows import (
    bronze_window_directory,
    parse_month_window,
    parse_trip_job,
)

SOURCE_URL = "https://s3.amazonaws.com/tripdata/"

S3_NAMESPACE = {
    "s3": "http://s3.amazonaws.com/doc/2006-03-01/"
}

def replace_window_directory(
        staging_dir: Path,
        target_dir: Path,
) -> None:
    backup_dir = staging_dir.with_name(
        f"{staging_dir.name}-previous"
    )

    if target_dir.exists():
        target_dir.rename(backup_dir)

    try:
        staging_dir.rename(target_dir)
    except BaseException:
        if backup_dir.exists() and not target_dir.exists():
            backup_dir.rename(target_dir)
        raise
    else:
        if backup_dir.exists():
            rmtree(backup_dir)

def discover_source_objects(
    source_prefix: str,
) -> list[tuple[str, str]]:
    with urlopen(SOURCE_URL, timeout=30) as response:
        listing_xml = response.read()

    root = ET.fromstring(listing_xml)

    matching_keys_and_dates = []

    contents_elements = root.findall(
        "s3:Contents",
        S3_NAMESPACE,
    )

    for contents_element in contents_elements:
        file_name_element = contents_element.find(
            "s3:Key",
            S3_NAMESPACE,
        )
        last_modified_element = contents_element.find(
            "s3:LastModified",
            S3_NAMESPACE,
        )

        if (
            file_name_element is None
            or last_modified_element is None
        ):
            continue

        file_name = file_name_element.text
        last_modified = last_modified_element.text

        if file_name is None or last_modified is None:
            continue

        if file_name.startswith(source_prefix):
            matching_keys_and_dates.append(
                (file_name, last_modified)
            )

    return matching_keys_and_dates

def select_latest_source_keys(
    matching_keys_and_dates: list[tuple[str, str]],
    ) -> tuple[list[str], str]:
    latest_last_modified = max(
        last_modified
        for _, last_modified in matching_keys_and_dates
    )

    selected_keys = [
        file_name
        for file_name, last_modified in matching_keys_and_dates
        if last_modified == latest_last_modified
    ]

    return selected_keys, latest_last_modified

def select_archive_members(
    zip_file: ZipFile,
    member_prefix: str,
) -> list[ZipInfo]:
    csv_members = []

    for member in zip_file.infolist():
        if member.is_dir():
            continue

        if member.filename.startswith("__MACOSX/"):
            continue

        if not member.filename.lower().endswith(".csv"):
            continue

        member_name = Path(member.filename).name

        if not member_name.startswith(member_prefix):
            continue

        csv_members.append(member)

    if not csv_members:
        raise FileNotFoundError(
            "No matching CSV files found in source archive"
        )

    members_by_directory: dict[str, list[ZipInfo]] = {}

    for member in csv_members:
        directory = str(Path(member.filename).parent)

        if directory not in members_by_directory:
            members_by_directory[directory] = []

        members_by_directory[directory].append(member)

    latest_directory = None
    latest_directory_time = None

    for directory, members in members_by_directory.items():
        directory_time = max(
            member.date_time
            for member in members
        )

        if (
            latest_directory_time is None
            or directory_time > latest_directory_time
        ):
            latest_directory = directory
            latest_directory_time = directory_time

    return members_by_directory[latest_directory]

def count_csv_rows(csv_path: Path) -> int:
    with csv_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as csv_file:
        reader = csv.reader(csv_file)

        next(reader, None)

        return sum(1 for _ in reader)

def download_with_retry(
    archive_url: str,
    archive_file: BinaryIO,
    attempts: int = 3,
) -> None:
    for attempt in range(1, attempts + 1):
        try:
            archive_file.seek(0)
            archive_file.truncate(0)

            with urlopen(
                archive_url,
                timeout=60,
            ) as archive_response:
                copyfileobj(
                    archive_response,
                    archive_file,
                )

            archive_file.seek(0)
            return

        except (URLError, OSError):
            if attempt == attempts:
                raise

            wait_seconds = 2 ** (attempt - 1)

            print(
                f"Download failed; retrying in "
                f"{wait_seconds} seconds "
                f"(attempt {attempt + 1}/{attempts})"
            )

            time.sleep(wait_seconds)

def extract_archive_to_staging(
    file_name: str,
    member_prefix: str,
    staging_dir: Path,
) -> tuple[int, int]:
    encoded_key = quote(file_name)
    archive_url = SOURCE_URL + encoded_key

    total_rows = 0
    objects = 0

    with TemporaryFile() as archive_file:
        download_with_retry(
            archive_url,
            archive_file,
        )
    
        with ZipFile(archive_file) as zip_file:
            selected_members = select_archive_members(
                zip_file,
                member_prefix,
            )

            for member in selected_members:
                destination_path = (
                    staging_dir
                    / Path(member.filename).name
                )

                with zip_file.open(member) as source_file:
                    with destination_path.open(
                        "wb"
                    ) as destination_file:
                        copyfileobj(
                            source_file,
                            destination_file,
                        )

                total_rows += count_csv_rows(
                    destination_path
                )
                objects += 1

    return total_rows, objects

def source_prefixes_for_window(
    market: str,
    year: str,
    month: str,
) -> tuple[str, str]:
    window_number = f"{year}{month}"
    window_date = (int(year), int(month))

    if market == "jc":
        source_prefix = f"JC-{window_number}"
        member_prefix = source_prefix
    else:
        if window_date < (2024, 1):
            source_prefix = year
        else:
            source_prefix = window_number

        member_prefix = window_number

    return source_prefix, member_prefix

def write_bronze_manifest(
    staging_dir: Path,
    job: str,
    window: str,
    objects: int,
    rows: int,
    source_keys: list[str],
    last_modified: str,
) -> None:
    manifest = {
        "layer": "bronze",
        "job": job,
        "window": window,
        "objects": objects,
        "rows": rows,
        "source_keys": source_keys,
        "last_modified": last_modified,
    }

    manifest_path = staging_dir / "manifest.json"

    with manifest_path.open(
        "w",
        encoding="utf-8",
    ) as manifest_file:
        json.dump(
            manifest,
            manifest_file,
            indent=2,
        )

def ingest_to_bronze(job: str, window: str) -> None:

    _, market = parse_trip_job(job)
    year, month = parse_month_window(window)

    source_prefix, member_prefix = (
        source_prefixes_for_window(
            market,
            year,
            month,
        )
    )

    target_dir = bronze_window_directory(job, window)

    matching_keys_and_dates = discover_source_objects(
    source_prefix
    )

    if not matching_keys_and_dates:
        raise FileNotFoundError(f"No source file found for job={job}, window={window}")

    selected_keys, latest_last_modified = (
        select_latest_source_keys(
            matching_keys_and_dates
        )
    )

    target_dir.parent.mkdir(parents=True, exist_ok=True)

    staging_dir = Path(
        mkdtemp(
            prefix=f".{month}-staging-",
            dir=target_dir.parent
        )
    )

    try:

        total_rows = 0
        objects = 0

        total_rows = 0
        objects = 0

        for file_name in selected_keys:
            archive_rows, archive_objects = (
                extract_archive_to_staging(
                    file_name,
                    member_prefix,
                    staging_dir,
                )
            )

            total_rows += archive_rows
            objects += archive_objects

        write_bronze_manifest(
            staging_dir=staging_dir,
            job=job,
            window=window,
            objects=objects,
            rows=total_rows,
            source_keys=selected_keys,
            last_modified=latest_last_modified,
        )

        replace_window_directory(
            staging_dir,
            target_dir,
        )

    finally:
        if staging_dir.exists():
            rmtree(staging_dir)             

def inspect_bronze(job:str, window: str) -> dict:
    manifest_path = (
        bronze_window_directory(job, window)
        / "manifest.json"
    )

    with manifest_path.open("r", encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    return {
        "layer": manifest["layer"],
        "job": manifest["job"],
        "window": manifest["window"],
        "objects": manifest["objects"],
        "rows": manifest["rows"],
    }


                     