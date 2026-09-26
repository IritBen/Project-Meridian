import argparse
import json
from meridian.bronze import ingest_to_bronze, inspect_bronze
from meridian.silver import transform_to_silver

parser = argparse.ArgumentParser()

parser.add_argument("command")
parser.add_argument("layer")
parser.add_argument("job")
parser.add_argument("window")

args = parser.parse_args()

if args.command == "run" and args.layer == "ingest-to-bronze":
    ingest_to_bronze(args.job, args.window) 

elif args.command == "run" and args.layer == "transform-to-silver":
    transform_to_silver(args.job, args.window)
    
elif args.command == "inspect" and args.layer == "bronze":
    output = inspect_bronze(args.job, args.window) 
    print(json.dumps(output))