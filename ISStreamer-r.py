#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io, os, sys
import simplejson as json
import datetime
import time
import redis
from ISStreamer.Streamer import Streamer

iss_bucket_name   = 'xxx'
iss_bucket_key    = 'xxx'
iss_access_key    = 'xxx'

QUE_NAME = 'INITIALSTATE_PUSH' + iss_bucket_name

# redis
POOL = redis.ConnectionPool(host='192.168.10.2', port=16379, db=0)
r = redis.StrictRedis(connection_pool=POOL)

streamer = Streamer(bucket_key=iss_bucket_key, access_key=iss_access_key, buffer_size=30) #, debug_level=2)
#streamer = Streamer(bucket_name=iss_bucket_name, bucket_key=iss_bucket_key, access_key=iss_access_key) #, debug_level=2)

try:
    r.ping()

except Exception as e:
    print(e)
    sys.exit()


try:

    while 1:
        quelist = (QUE_NAME)
        jobque = r.brpop(quelist, 10)
        
        if jobque:
            redis_val  = json.loads(jobque[1].decode("utf-8"))
            kprefix    = redis_val.get('key_prefix')
            epoch00    = int(redis_val.get('epoch'))
            bucket     = redis_val.get('bucket')

#            print(epoch00, kprefix, bucket)
            
#            streamer.log_object(bucket, key_prefix=kprefix)
            streamer.log_object(bucket, key_prefix=kprefix, epoch=epoch00)
            time.sleep(0.1)

        else:
            b = { "tstamp": time.time() }
            streamer.log_object(b)
            streamer.flush()


except Exception as e:
    print(e)
    sys.exit()

except KeyboardInterrupt:
    print('[dequeue] intterupted by keyboard')
    sys.exit()

#
