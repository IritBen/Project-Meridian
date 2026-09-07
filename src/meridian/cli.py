import argparse
from meridian.bronze import ingest_to_bronze

parser = argparse.ArgumentParser()

parser.add_argument("command")
parser.add_argument("layer")
parser.add_argument("job")
parser.add_argument("window")

args = parser.parse_args()

if args.command == "run" and args.layer == "ingest-to-bronze":
    ingest_to_bronze(args.job, args.window) 