#!/usr/bin/env python

import os
#from configparser import ConfigParser
import writeToDynamodb
import data
import writeTotable
# file = 'aws_config.ini'
# config = ConfigParser()
# config.read(file)
os.environ['AWS_CONFIG_FILE'] = 'aws_config.ini'


if __name__ == '__main__':
    writeTotable.createTable()
    writeToDynamodb.createTable()
    data.writeFiles(20)
