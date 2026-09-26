import xml.etree.ElementTree as ET
import csv
import json
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import quote
from shutil import copyfileobj
from tempfile import TemporaryFile
from zipfile import ZipFile

SOURCE_URL = "https://s3.amazonaws.com/tripdata/"

S3_NAMESPACE = {
    "s3": "http://s3.amazonaws.com/doc/2006-03-01/"
}


def ingest_to_bronze(job: str, window: str) -> None:
    bronze_root = Path("/bronze")
    dataset, market = job.split(":")
    year, month = window.split("-")

    target_dir = bronze_root / dataset / market / year / month

    with urlopen(SOURCE_URL, timeout=30) as response:
        listing_xml = response.read()

    root = ET.fromstring(listing_xml)

    matching_keys_and_dates = []

    source_prefix = f"{market.upper()}-{year}{month}"

    contents_elements = root.findall("s3:Contents", S3_NAMESPACE)

    for contents_element in contents_elements:
        file_name_element = contents_element.find("s3:Key", S3_NAMESPACE)
        last_modified_element = contents_element.find("s3:LastModified", S3_NAMESPACE)

        if file_name_element is None or last_modified_element is None:
            continue

        file_name = file_name_element.text
        last_modified = last_modified_element.text

        if file_name is None or last_modified is None:
            continue

        if file_name.startswith(source_prefix):
            matching_keys_and_dates.append((file_name, last_modified))


    if not matching_keys_and_dates:
        raise FileNotFoundError(f"No source file found for job={job}, window={window}")

    latest_last_modified = matching_keys_and_dates[0][1]

    for file_name, last_modified in matching_keys_and_dates:
        if last_modified > latest_last_modified:
            latest_last_modified = last_modified

    target_dir.mkdir(parents=True, exist_ok=True)

    selected_keys = []

    for file_name, last_modified in matching_keys_and_dates:
        if last_modified == latest_last_modified:
            selected_keys.append(file_name)

    total_rows = 0
    objects = 0

    for file_name in selected_keys:
        encoded_key = quote(file_name)
        archive_url = SOURCE_URL + encoded_key
        with TemporaryFile() as archive_file:
            with urlopen(archive_url, timeout=60) as archive_response:
                copyfileobj(archive_response, archive_file)

            archive_file.seek(0)

            with ZipFile(archive_file) as zip_file:
                csv_members = []

                for member in zip_file.infolist():
                    if member.is_dir():
                        continue

                    if member.filename.startswith("__MACOSX/"):
                        continue

                    if not member.filename.lower().endswith(".csv"):
                        continue

                    csv_members.append(member)

                for member in csv_members:
                    destination_path = target_dir / Path(member.filename).name

                    with zip_file.open(member) as source_file:
                        with destination_path.open("wb") as destination_file:
                            copyfileobj(source_file, destination_file)

                    with destination_path.open(
                        "r",
                        encoding="utf-8-sig",
                        newline="",
                    ) as csv_file:
                        reader = csv.reader(csv_file)
                    
                        header = next(reader)
                    
                        row_count = 0

                        for row in reader:
                            row_count += 1

                    total_rows += row_count
                    objects += 1
                    
    manifest = {
    "layer": "bronze",
    "job": job,
    "window": window,
    "objects": objects,
    "rows": total_rows,
    "source_keys": selected_keys,
    "last_modified": latest_last_modified,
    }

    manifest_path = target_dir / "manifest.json"

    with manifest_path.open("w", encoding="utf-8") as manifest_file:
        json.dump(manifest, manifest_file, indent=2)               

def inspect_bronze(job:str, window: str) -> dict:
    bronze_root = Path("/bronze")
    dataset, market = job.split(":")
    year, month = window.split("-")

    manifest_path = bronze_root / dataset / market / year / month / "manifest.json"

    with manifest_path.open("r", encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    return {
        "layer": manifest["layer"],
        "job": manifest["job"],
        "window": manifest["window"],
        "objects": manifest["objects"],
        "rows": manifest["rows"],
    }


                     