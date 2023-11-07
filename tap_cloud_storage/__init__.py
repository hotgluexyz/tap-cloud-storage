#!/usr/bin/env python3
import os
import json
import argparse
import logging

from datetime import datetime, timedelta
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
        required=True
    )

    parser.add_argument(
        '-s', '--state',
        help='State file',
        required=False
    )

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    if args.state:
        setattr(args, 'state_path', args.state)
        if os.path.exists(args.state):
            args.state = load_json(args.state)

    # Fall back to setting state to a empty dict
    if type(args.state) != dict:
        args.state = {}

    return args

def download_with_replication_key(blob, state, target_path):
    logger.info(f"Downloading incremental: {blob.name} -> {target_path}")

    if not state.get(blob.name):
        blob.download_to_filename(target_path)
        state[blob.name] = {
            "replication_key_value": blob.updated.isoformat(),
            "replication_key": "updated"
        }
        return state

    replication_key_value = datetime.fromisoformat(state[blob.name].get('replication_key_value'))

    if blob.updated > replication_key_value:
        blob.download_to_filename(target_path)
        state[blob.name] = {
            "replication_key_value": blob.updated.isoformat(),
            "replication_key": "updated"
        }
        return state

    # As the file didn't change, we don't need to download it again
    logger.info(f"{blob.name} being ignored... No updates")
    return state

def download(args):
    logger.info(f"Downloading data...")
    config = args.config
    state = args.state
    bucket_name = config['bucket']
    remote_path = config['path_prefix']
    target_dir = config['target_dir']
    incremental_mode = config.get('incremental_mode',False)

    # Upload all data in input_path to Google Cloud Storage
    storage_client = storage.Client.from_service_account_json(args.config_path)
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=remote_path)

    if state == {}:
        state = {"bookmarks": {}}

    for blob in blobs:
        key = blob.name
        if key.endswith("/"):
            # Ignore directories
            continue

        target_path = Path(target_dir).joinpath(Path(key).name)

        if incremental_mode:
            state["bookmarks"] = download_with_replication_key(
                blob,
                state["bookmarks"],
                target_path
            )
        else:
            logger.debug(f"Downloading: {bucket_name}:{key} -> {target_path}")
            blob.download_to_filename(target_path)

    logger.info(f"Data downloaded.")

    if incremental_mode:
        json.dump(state, open(args.state_path, "w"), indent=4)


def main():
    # Parse command line arguments
    args = parse_args()

    # Download the data
    download(args)


if __name__ == "__main__":
    main()
