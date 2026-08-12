from __future__ import annotations

import re
import sys
import zipfile
import xml.etree.ElementTree as ET

from datetime import datetime
from pathlib import Path, PurePosixPath
from urllib.parse import quote

import requests


S3_URL = "https://s3.amazonaws.com/tripdata/"

# /app/files בתוך הקונטיינר יהיה מחובר ל-volume
FILES_DIR = Path("files")
ZIP_DIR = FILES_DIR / "zips"


def get_s3_zip_keys() -> list[str]:
    """
    מחזירה את כל קבצי ה-ZIP שקיימים ב-S3.
    """

    result = []
    continuation_token = None

    while True:
        params = {
            "list-type": "2",
            "max-keys": "1000",
        }

        if continuation_token:
            params["continuation-token"] = continuation_token

        response = requests.get(
            S3_URL,
            params=params,
            timeout=60,
        )

        response.raise_for_status()

        root = ET.fromstring(response.content)

        namespace = root.tag.split("}")[0].strip("{")

        def tag(name: str) -> str:
            return f"{{{namespace}}}{name}"

        for item in root.findall(tag("Contents")):
            key = item.findtext(tag("Key"))

            if not key:
                continue

            if not key.lower().endswith(".zip"):
                continue

            result.append(key)

        is_truncated = (
            root.findtext(tag("IsTruncated"))
            == "true"
        )

        if not is_truncated:
            break

        continuation_token = root.findtext(
            tag("NextContinuationToken")
        )

    return result


def find_source_zip(
    market: str,
    year_month: str,
    zip_keys: list[str],
) -> str:
    """
    מחפשת קודם ZIP חודשי.
    אם אין - מחפשת ZIP שנתי.
    """

    market = market.upper()

    if market not in {"NY", "JC"}:
        raise ValueError(
            "market must be NY or JC"
        )

    if len(year_month) != 6 or not year_month.isdigit():
        raise ValueError(
            "year_month must be YYYYMM, for example 202404"
        )

    year = year_month[:4]

    # קודם נחפש ZIP חודשי
    for key in zip_keys:
        name = PurePosixPath(key).name

        if market == "JC":
            if not name.upper().startswith("JC"):
                continue
        else:
            if name.upper().startswith("JC"):
                continue

        if year_month in name:
            return key

    # אם אין חודשי - נחפש שנתי
    for key in zip_keys:
        name = PurePosixPath(key).name

        if market == "JC":
            if not name.upper().startswith("JC"):
                continue
        else:
            if name.upper().startswith("JC"):
                continue

        if year in name:
            return key

    raise FileNotFoundError(
        f"No ZIP found for market={market}, month={year_month}"
    )


def download_zip(
    key: str,
) -> Path:
    """
    מורידה את ה-ZIP ל-volume.

    אם הקובץ כבר קיים - הוא נדרס.
    """

    ZIP_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_name = PurePosixPath(key).name

    zip_path = ZIP_DIR / file_name

    print(f"Downloading: {key}")

    encoded_key = quote(
        key,
        safe="/",
    )

    response = requests.get(
        f"{S3_URL}{encoded_key}",
        stream=True,
        timeout=300,
    )

    response.raise_for_status()

    with zip_path.open("wb") as output:
        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):
            if chunk:
                output.write(chunk)

    return zip_path


def parse_csv_month_and_market(
    internal_path: str,
) -> tuple[str, str] | None:
    """
    מחזירה:
        month
        market

    Examples:

    4_April/201804-citibike-tripdata_1.csv
        -> 201804, NY

    folder/JC-201612-citibike-tripdata.csv
        -> 201612, JC
    """

    file_name = PurePosixPath(
        internal_path
    ).name

    if not file_name.lower().endswith(".csv"):
        return None

    jc_match = re.match(
        r"^JC[-_]?(\d{6})",
        file_name,
        re.IGNORECASE,
    )

    if jc_match:
        return jc_match.group(1), "JC"

    ny_match = re.match(
        r"^(\d{6})",
        file_name,
    )

    if ny_match:
        return ny_match.group(1), "NY"

    return None


def get_modified_time(
    info: zipfile.ZipInfo,
) -> datetime:
    """
    מחזירה את Date Modified של הקובץ בתוך ה-ZIP.
    """

    return datetime(
        *info.date_time
    )


def find_latest_month_files(
    zip_path: Path,
    market: str,
    year_month: str,
) -> list[zipfile.ZipInfo]:
    """
    מחפשת בתוך ZIP את כל ה-CSV-ים של market + month.

    אחר כך מוצאת את ה-Date Modified המאוחר ביותר,
    ומחזירה את כל הקבצים שיש להם בדיוק timestamp זהה אליו.
    """

    candidates = []

    with zipfile.ZipFile(zip_path) as archive:
        for info in archive.infolist():

            if info.is_dir():
                continue

            parsed = parse_csv_month_and_market(
                info.filename
            )

            if parsed is None:
                continue

            file_month, file_market = parsed

            if file_market != market:
                continue

            if file_month != year_month:
                continue

            candidates.append(info)

        if not candidates:
            raise FileNotFoundError(
                f"No CSV files found for "
                f"market={market}, month={year_month}"
            )

        latest_timestamp = max(
            get_modified_time(info)
            for info in candidates
        )

        latest_files = [
            info
            for info in candidates
            if get_modified_time(info)
            == latest_timestamp
        ]

        print(
            f"Latest timestamp found: "
            f"{latest_timestamp}"
        )

        print(
            f"Files with latest timestamp: "
            f"{len(latest_files)}"
        )

        for info in latest_files:
            print(
                f"  {info.filename}"
            )

        return latest_files


def extract_files(
    zip_path: Path,
    files_to_extract: list[zipfile.ZipInfo],
    market: str,
    year_month: str,
) -> None:
    """
    מחלצת רק את הקבצים שנבחרו.

    מבנה היעד:

    files/
      NY/
        2024/
          202404-citibike-tripdata_1.csv
          202404-citibike-tripdata_2.csv
    """

    year = year_month[:4]

    target_dir = (
        FILES_DIR
        / market
        / year
    )

    target_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    with zipfile.ZipFile(zip_path) as archive:

        for info in files_to_extract:

            file_name = PurePosixPath(
                info.filename
            ).name

            target_path = (
                target_dir
                / file_name
            )

            print(
                f"Extracting: "
                f"{info.filename}"
            )

            print(
                f"        -> "
                f"{target_path}"
            )

            with archive.open(info) as source:
                with target_path.open("wb") as output:
                    output.write(
                        source.read()
                    )


def main(
    market: str,
    year_month: str,
) -> None:

    market = market.upper()

    print(
        f"Market: {market}"
    )

    print(
        f"Month: {year_month}"
    )

    # 1. קבלת רשימת ה-ZIP-ים
    zip_keys = get_s3_zip_keys()

    # 2. מציאת ה-ZIP המתאים
    selected_zip_key = find_source_zip(
        market=market,
        year_month=year_month,
        zip_keys=zip_keys,
    )

    print(
        f"Selected ZIP: "
        f"{selected_zip_key}"
    )

    # 3. הורדת ה-ZIP ל-volume
    zip_path = download_zip(
        selected_zip_key
    )

    # 4. מציאת הקבצים המעודכנים ביותר
    files_to_extract = find_latest_month_files(
        zip_path=zip_path,
        market=market,
        year_month=year_month,
    )

    # 5. חילוץ רק הקבצים שנבחרו
    extract_files(
        zip_path=zip_path,
        files_to_extract=files_to_extract,
        market=market,
        year_month=year_month,
    )

    print()
    print("Done.")


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print(
            "Usage: "
            "python read_files.py <NY|JC> <YYYYMM>"
        )

        sys.exit(1)

    main(
        market=sys.argv[1],
        year_month=sys.argv[2],
    )
