import argparse
import json
import os
from google.cloud import storage

from counter import generate_stats

if __name__ == "__main__":
    # arguments, we only need the bucket name
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket_name", help="The bucket name")
    args = parser.parse_args()

    # get the bucket name
    bucket_name = args.bucket_name

    # get the bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # list the blobs in the bucket
    blobs = bucket.list_blobs()

    # filter the jsonl files
    jsonl_files = [blob.name for blob in blobs if blob.name.endswith(".jsonl")]

    all_csv = []
    for jsonl_file in jsonl_files:
        # generate the stats
        csv = generate_stats(bucket_name, jsonl_file)
        # add the csv to the list
        for row in csv:
            all_csv.append(row)

    # save to downloads directory
    # as a csv file
    downloads_dir = "downloads"
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)

    full_path = os.path.join(downloads_dir, "all.csv")
    # rows as a set to remove duplicates
    rows = set()

    with open(full_path, "w") as f:
        for row in all_csv:
            if row in rows:
                continue
            f.write(row)
            # add a new line
            f.write("\n")

    # upload the file to the bucket
    blob = bucket.blob("all.csv")
    blob.upload_from_filename(full_path)
