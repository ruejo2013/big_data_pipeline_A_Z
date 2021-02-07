#!/usr/bin/env python3

import json

import tempfile

from faker import Faker

from s3fs.core import S3FileSystem
from writeToDynamodb import writeToDynamodb
from writeTotable import insertData

fake = Faker()

s3 = S3FileSystem(anon=False)
bucket = "ftp-mena"


def s3_write(bucket, filename, file):
    """ Func to write an s3 bucket"""
    with s3.open(f"s3://{bucket}/{filename}", "w") as s3File:
        s3File.write(file)


def create_file(files=5):
    """Func to create json files"""
    alldata = {}
    alldata["records"] = []
    name = fake.name()
    fname = name.replace(" ", "-") + ".json"
    for _ in range(files):
        data = {
            "userid": fake.uuid4(),
            "name": fake.name(),
            "age": fake.random_int(min=18, max=101, step=1),
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state(),
            "zip": fake.zipcode(),
            "phone": fake.phone_number(),
            "email": fake.email()
        }
        alldata["records"].append(data)
    return (alldata["records"], fname)

def writeFiles(numb):
    """func to write to s3  and dynamodb"""
    user = 1
    while user < numb:
        data, fname = create_file(10)
        # insert data to the database
        insertData(data)
        # dump the data as json object and write to s3 bucket
        datajson = json.dumps(data) # make a json object from a python object
        # make a tempfile, and write the json object to it, no need since the data is converted to a json object with
        # json.dumps
        # fd, filename = tempfile.mkstemp(suffix='.json')
        # with open(filename, 'w') as f1:
        #     json.dump(datajson, f1)
        # load the json file to a dynamodb
        writeToDynamodb(data)
        s3_write(bucket, fname, datajson)
        user += 1
