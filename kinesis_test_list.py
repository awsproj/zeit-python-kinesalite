#!/usr/bin/env python
#  kinesis_test_list.py

from kinesis_data_client import KinesisDataClient as kinesisClient

if __name__ == '__main__':
    endpoint='http://127.0.0.1:4567'
    kinesis = kinesisClient(endpoint_url=endpoint)
    kinesis.op_list()

