#!/usr/bin/env python3
import os
import json
import argparse
import logging

from pathlib import Path
from google.cloud import storage

logger = logging.getLogger("tap-cloud-storage")
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    return args


def download(args):
    logger.debug(f"Downloading data...")
    config = args.config
    bucket_name = config['bucket']
    remote_path = config['path_prefix']
    target_dir = config['target_dir']

    # Upload all data in input_path to Google Cloud Storage
    storage_client = storage.Client.from_service_account_json(args.config_path)
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=remote_path)

    for blob in blobs:
        key = blob.name
        if key.endswith("/"):
            # Ignore directories
            continue

        target_path = Path(target_dir).joinpath(Path(key).name)

        logger.debug(f"Downloading: {bucket_name}:{key} -> {target_path}")
        blob.download_to_filename(target_path)

    logger.debug(f"Data downloaded.")


def main():
    # Parse command line arguments
    args = parse_args()

    # Download the data
    download(args)


if __name__ == "__main__":
    main()
