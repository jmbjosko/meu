#--------------
# Author: J. M. B. Josko
# --------------
# Preamble - Loading required packages and set parameters
# --------------

import sys
import datetime
import json
import csv
from amazon_kclpy import kcl
from amazon_kclpy.v2 import processor
from boto import kinesis

# Define record limits
if len(sys.argv) > 1:
    LIMIT = sys.argv[1]
else:
    LIMIT = 1000

#--------
# Get connection to Luiza Kinesis
#--------

# Authentication string
auth = {"aws_access_key_id":"AKIAIMYC23Z4HZ4BVJVA", "aws_secret_access_key":"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"}
connection = kinesis.connect_to_region('us-east-1',**auth)

# Connect to the host
response = connection.describe_stream('big-data-analytics-desafio')

#--------
# Gather all Shards of the Luiza Stream
#--------

shard_ids = []          
stream_name = None 
if response and 'StreamDescription' in response:
    stream_name = response['StreamDescription']['StreamName']

    # Put in the list each shard related to the stream
    for shard_id in response['StreamDescription']['Shards']: 
        shard_id = shard_id['ShardId']
        shard_iterator = connection.get_shard_iterator(stream_name, shard_id, 'TRIM_HORIZON')
        shard_ids.append(shard_iterator['ShardIterator'])

#--------
# Gather all Shards records
#--------

result = []
nrow = 0

# For each shard extract correponding records
for shard_iterator in shard_ids:
    while shard_iterator and nrow <= LIMIT:
        response = connection.get_records(shard_iterator, 10000)
        if len(response['Records'])> 0:
            for res in response['Records']: 
                s = res['Data']
                s = s.replace("'", "\"")
                result.append(json.loads(s))
                nrow += 1
        shard_iterator = response['NextShardIterator']

# Check streams exsitance
if len(result) > 0:

    # define desired keys
    keys = keys = ['event_type', 'datetime']

    # open a csv file
    with open('D:\Luizalabs\stream.csv', 'w') as f:  
        w = csv.DictWriter(f, keys)
        w.writeheader()

        # write all streams
        for rec in result:
            x = str(rec['datetime'])
            rec['datetime'] = datetime.datetime.fromtimestamp(float(x[0:10])).strftime('%Y-%m-%d')
            w.writerow({a:rec[a] for a in keys})

print ("Total records....")
print (len(result))

#--------
# Close connection
#--------

connection.close()    
