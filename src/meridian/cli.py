import argparse
import json
import os
import psycopg

from meridian.warehouse import initialize_warehouse
from meridian.bronze import ingest_to_bronze, inspect_bronze
from meridian.gold import (
    report_daily_station_trips,
    transform_to_gold,
)
from meridian.silver import inspect_silver, transform_to_silver

def open_database_connection() -> psycopg.Connection:
    dsn = os.environ["DATABASE_URL"]

    return psycopg.connect(dsn)

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(
    dest="command",
    required=True,
)

init_parser = subparsers.add_parser("init")
init_parser.add_argument(
    "target",
    choices=("warehouse",),
)


run_parser = subparsers.add_parser("run")
run_parser.add_argument("layer")
run_parser.add_argument("job")
run_parser.add_argument("window")


inspect_parser = subparsers.add_parser("inspect")
inspect_parser.add_argument("layer")
inspect_parser.add_argument("job")
inspect_parser.add_argument("window")


report_parser = subparsers.add_parser("report")
report_parser.add_argument("report_name")
report_parser.add_argument("market")
report_parser.add_argument("station")
report_parser.add_argument("day")


args = parser.parse_args()


if args.command == "init":
    with open_database_connection() as connection:
        initialize_warehouse(connection)

    print("Warehouse initialized")


elif args.command == "run":
    if args.layer == "ingest-to-bronze":
        ingest_to_bronze(
            args.job,
            args.window,
        )

    elif args.layer == "transform-to-silver":
        with open_database_connection() as connection:
            transform_to_silver(
                args.job,
                args.window,
                connection,
            )

    elif args.layer == "transform-to-gold":
        with open_database_connection() as connection:
            transform_to_gold(
                args.job,
                args.window,
                connection,
            )

    else:
        parser.error(
            f"Unsupported run layer: {args.layer}"
        )


elif args.command == "inspect":
    if args.layer == "bronze":
        output = inspect_bronze(args.job, args.window)

    elif args.layer == "silver":
        
        with open_database_connection() as connection:
            output = inspect_silver(
                args.job,
                args.window,
                connection,
            )

    else:
        parser.error(f"Unsupported inspect layer: {args.layer}")

    print(json.dumps(output))


elif args.command == "report":
    if args.report_name == "daily-station-trips":

        with open_database_connection() as connection:
            output = report_daily_station_trips(
                args.market,
                args.station,
                args.day,
                connection,
            )

    else:
        parser.error(
            f"Unsupported report: {args.report_name}"
        )

    print(json.dumps(output))