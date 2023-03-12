import os
from counter import generate_stats
import functions_framework
from google.cloud import storage


# Triggered by a change to a Cloud Storage bucket.
@functions_framework.cloud_event
def stats(cloud_event):
    data = cloud_event.data

    event_type = cloud_event["type"]

    # if the event type is not OBJECT_FINALIZE
    # do nothing
    if "finalize" not in event_type:
        return

    bucket = data["bucket"]
    name = data["name"]

    # if the file is not a jsonl file
    # do nothing
    if not name.endswith(".jsonl"):
        return

    csv = generate_stats(bucket, name)

    # ensure downloads directory exists
    downloads_dir = "downloads"
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)

    full_path = os.path.join(downloads_dir, "all.csv")

    # download the all.csv file from the bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob("all.csv")
    blob.download_to_filename(full_path)

    # rows as a set to remove duplicates
    rows = set()

    # open the file
    with open(full_path, "r") as f:
        for line in f:
            rows.add(line)

    # now append the new rows to the file
    with open(full_path, "a") as f:
        for row in csv:
            if row in rows:
                continue
            f.write(row)
            # add a new line
            f.write("\n")

    # upload the file to the bucket
    blob = bucket.blob("all.csv")
    blob.upload_from_filename(full_path)
