import datetime
import json
import os
from google.cloud import storage
from collections import Counter

sijamuodot = []


def key_for_stats(sijamuoto, number, av, tn):
    """This function takes in the sijamuoto, number, av and tn
    and returns a key for the stats dictionary"""
    return f"{sijamuoto}:{number}:{av}:{tn}"


def generate_stats(bucket, name):
    """This function takes in a bucket and a file name
     and generates the stats for the file. The output is in CSV format:
    isoformat_timestamp,filename,sijamuoto,number,av,tn,count
    2020-01-01T12:00:00,filename,sisatulento,plural,D,10,1123
    arguments:
    bucket: the bucket name
    name: the file name (must be a jsonl file, the name is epoch_timestamp.jsonl)"""
    # get the file
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(name)

    # ensure downloads directory exists
    downloads_dir = "downloads"
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)

    full_path = os.path.join(downloads_dir, name)

    # if the file does not exist, download it
    if not os.path.exists(full_path):
        blob.download_to_filename(full_path)

    items = []
    # open the file
    with open(full_path, "r") as f:
        for line in f:
            item = json.loads(line)
            items.append(item)

    try:
        timestamp = datetime.datetime.fromtimestamp(int(name.split(".")[0]))
    except ValueError:
        # get the timestamp for Feb 1 2023
        timestamp = datetime.datetime(2023, 2, 1)

    # list for all keys
    keys = []

    # go through the file and generate the stats
    for item in items:
        # check item has all the keys
        if "SIJAMUOTO" not in item:
            continue
        if "NUMBER" not in item:
            continue
        if "av" not in item:
            continue
        if "tn" not in item:
            continue
        sijamuoto = item["SIJAMUOTO"]
        number = item["NUMBER"]
        av = item["av"]
        if type(av) is dict:
            if "#text" in av:
                av = av["#text"]
            else:
                av = "_"

        tn = item["tn"]
        key = key_for_stats(sijamuoto, number, av, tn)

        keys.append(key)

    # count the keys
    stats = Counter(keys)

    # dump the stats to a csv string
    csv = []
    for key, count in stats.items():
        # check number of ":" in the key
        if key.count(":") != 3:
            print(f"Invalid key: {key}")
        sijamuoto, number, av, tn = key.split(":")
        line = f"{timestamp.isoformat()},{name},{sijamuoto},{number},{av},{tn},{count}"
        csv.append(line)

    return csv
