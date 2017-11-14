#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io, os, sys
import simplejson as json
import datetime
import time
import redis

from twython import Twython, TwythonError

# cron
# * * * * * ~/mainet-forkcheck.py > /dev/null
#
# eash system use blocknotify to send block hash
# bhashdata = {
#         "sv": server_name,
#         "bn": block_number,
#         "bh": blockhash
# }
# r.lpush(QUE_FORK_CHECK_NMAE, json.dumps(bhashdata))

def now():
    return int(time.time())

#
QUE_FORK_CHECK_NMAE  = 'QUE_FORK_CHECK'
r_lasttweettime      = 'FORK_r_lasttweettime'

count_of_server = 4

#
APP_KEY            = 'x'
APP_SECRET         = 'x'
OAUTH_TOKEN        = 'x-x'
OAUTH_TOKEN_SECRET = 'x'

# redis
POOL = redis.ConnectionPool(host='192.168.1.1', port=16379, db=0)
r = redis.StrictRedis(connection_pool=POOL)

#
twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

# main
try:
    r.ping()

except Exception as e:
    print(e)
    sys.exit()

blks = {}
try:
    while 1:
        jobque = r.brpop(QUE_FORK_CHECK_NMAE, 1)
        if jobque:
            redis_val = json.loads(jobque[1].decode("utf-8"))
            print(redis_val)
            bn = str(redis_val.get('bn'))
            bh = redis_val.get('bh')

            if blks.get(bn, None) == None:
                blks[bn] = []           

            blks[bn].append(bh) 

        else:
            break

    for x in blks:
        svc = len(blks[x])
        cnt = len(set(blks[x]))
        print(x, cnt, set(blks[x]))
        if svc >= count_of_server:
            if cnt > 1:
                lasttweettime = r.get(r_lasttweettime).decode("utf-8")
                tweet_msg = "*** possible fork in block - " + x
                if lasttweettime:
                    if x > lasttweettime:
                        r.set(r_lasttweettime, int(x))
                        twitter.update_status(status=tweet_msg)
    
                else:
                    r.set(r_lasttweettime, int(x))
                    twitter.update_status(status=tweet_msg)

        else:
            for y in blks[x]:
                bhashdata = {
                        "sv": 'readd',
                        "bn": x,
                        "bh": y 
                }
                r.lpush(QUE_FORK_CHECK_NMAE, json.dumps(bhashdata))
    

except Exception as e:
    sys.exit(e)

except KeyboardInterrupt:
    sys.exit()

