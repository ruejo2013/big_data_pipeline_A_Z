#!/usr/bin/env python3

from configparser import ConfigParser

import boto3
file = "config.ini"
config = ConfigParser()
config.read(file)
tablename = config["dynamodb"]["tablename"]

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(tablename)

# create a dynamo db table

def createTable(tablename=tablename):
    """Func to create dynamodb table"""
    table = dynamodb.create_table(
        TableName=tablename,
        KeySchema=[
            {
                'AttributeName': 'userid',
                'KeyType': 'HASH' # partition key
            },
            {
                'AttributeName': 'age',
                'KeyType': 'RANGE' # sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'userid',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'age',
                'AttributeType': 'N'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    print('Table status:', table.table_status)

    print('Waiting for', table.name, 'to complete creating...')
    table.meta.client.get_waiter('table_exists').wait(TableName=tablename)
    print('Table status:', dynamodb.Table(tablename).table_status)



def writeToDynamodb(file_name):
    """Func to write json file to dynamodb table"""
    # with open(file_name) as json_file:
    #     customers = json.load(json_file, parse_float=decimal.Decimal)

    for customer in file_name:
        userid = customer['userid']
        name = customer['name']
        age = customer['age']
        street = customer['street']
        city = customer['city']
        state = customer['state']
        zip = customer['zip']
        phone = customer['phone']
        email = customer['email']

        print('Adding movie: {} {}'.format(userid, name))


        table.put_item(
            Item={
                "userid": userid,
                "name": name,
                "age": age,
                "street": street,
                "city": city,
                "state": state,
                "zip": zip,
                "phone": phone,
                "email": email
            }
        )
