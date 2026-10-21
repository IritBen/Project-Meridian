import xml.etree.ElementTree as ET
from urllib.parse import urlencode
from urllib.request import urlopen

from meridian.trip_windows import (
    parse_month_window,
    parse_trip_job,
)

SOURCE_URL = "https://s3.amazonaws.com/tripdata/"

S3_NAMESPACE = {
    "s3": "http://s3.amazonaws.com/doc/2006-03-01/"
}

def is_month_published(
    job: str,
    window: str,
) -> bool:
    
    _, market = parse_trip_job(job)
    year, month = parse_month_window(window)

    window_number = f"{year}{month}"

    if market == "jc":
        source_prefix = f"JC-{window_number}"
    elif int(year) < 2024:
        source_prefix = year
    else:
        source_prefix = window_number

    query = urlencode({"prefix": source_prefix})
    listing_url = f"{SOURCE_URL}?{query}"

    with urlopen(listing_url, timeout=30) as response:
        listing_xml = response.read()

    root = ET.fromstring(listing_xml)

    for contents_element in root.findall(
        "s3:Contents",
        S3_NAMESPACE,
    ):
        file_name_element = contents_element.find(
            "s3:Key",
            S3_NAMESPACE,
        )

        if file_name_element is None:
            continue
        
        file_name = file_name_element.text

        if (
            file_name is not None
            and file_name.startswith(source_prefix)
        ):
            return True

    return False
