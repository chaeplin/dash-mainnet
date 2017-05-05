#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io, os, sys
import simplejson as json
import datetime
import time
import redis
from ISStreamer.Streamer import Streamer

# bucket1 = dash121
iss_bucket_name1 = 'dash121'
iss_bucket_key1  = 'xxx'
iss_access_key1  = 'xxxx'
QUE_NAME1        = 'INITIALSTATE_PUSH' + iss_bucket_name1

# bucket2 = testnet
iss_bucket_name2 = 'testnet'
iss_bucket_key2  = 'xxxx'
iss_access_key2  = 'xxxxxx'
QUE_NAME2        = 'INITIALSTATE_PUSH' + iss_bucket_name2

# bucket3 = ticker
iss_bucket_name3   = 'ticker'
iss_bucket_key3    = 'xxxx'
iss_access_key3    = 'xxxx'
QUE_NAME3          = 'INITIALSTATE_PUSH' + iss_bucket_name3

# streamer
streamer1 = Streamer(bucket_key=iss_bucket_key1, access_key=iss_access_key1)#, debug_level=2)
streamer2 = Streamer(bucket_key=iss_bucket_key2, access_key=iss_access_key2)#, debug_level=2)
streamer3 = Streamer(bucket_key=iss_bucket_key3, access_key=iss_access_key3)#, debug_level=2)

# redis
POOL = redis.ConnectionPool(host='192.168.10.2', port=16379, db=0)
r = redis.StrictRedis(connection_pool=POOL)

# main

try:
    r.ping()

except Exception as e:
    print(e)
    sys.exit()


try:

    while 1:
        quelist = (QUE_NAME1, QUE_NAME2, QUE_NAME3)
        jobque = r.brpop(quelist, 5)
        
        if jobque:
            redis_val   = json.loads(jobque[1].decode("utf-8"))
            bucket_name = redis_val.get('bucket_name', 'dash121')
            kprefix     = redis_val.get('key_prefix')
            epoch00     = redis_val.get('epoch')
            bucket      = redis_val.get('bucket')

            print(epoch00, bucket_name, kprefix, bucket)
        
            if bucket_name == iss_bucket_name1:
                streamer1.log_object(bucket, key_prefix=kprefix, epoch=epoch00)

            elif bucket_name == iss_bucket_name2:
                streamer2.log_object(bucket, key_prefix=kprefix, epoch=epoch00)

            elif bucket_name == iss_bucket_name3:
                streamer3.log_object(bucket, key_prefix=kprefix, epoch=epoch00)

            time.sleep(0.25)

        else:
            b = { "tstamp": time.time() }
            streamer1.log_object(b)
            streamer2.log_object(b)
            streamer3.log_object(b)

            streamer1.flush()
            time.sleep(0.25)
            streamer2.flush()
            time.sleep(0.25)
            streamer3.flush()
            time.sleep(0.25)


except Exception as e:
    print(e)
    sys.exit()

except KeyboardInterrupt:
    print('[dequeue] intterupted by keyboard')
    sys.exit()

