from pathlib import Path
from urllib.request import urlopen
import xml.etree.ElementTree as ET

SOURCE_URL = "https://s3.amazonaws.com/tripdata/"


def ingest_to_bronze(job: str, window: str) -> None:
    bronze_root = Path("/bronze")
    dataset, market = job.split(":")
    year, month = window.split("-")
    print(f"{dataset}\n{market}\n{year}\n{month}")
    target_dir = bronze_root / dataset / market / year / month
    print(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    with urlopen(SOURCE_URL, timeout=30) as response:
        listing_xml = response.read()

    root = ET.fromstring(listing_xml)

    namespace = {
        "s3": "http://s3.amazonaws.com/doc/2006-03-01/"
    }

    key_elements = root.findall("s3:Contents/s3:Key", namespace)

    matching_keys = []

    source_prefix = f"{market.upper()}-{year}{month}"

    for key_element in key_elements:
        file_name = key_element.text
        if file_name is not None:
            if file_name.startswith(source_prefix):
                matching_keys.append(file_name)

    print(f"source_prefix: {source_prefix}")
    print(f"matching_keys: {matching_keys}")

