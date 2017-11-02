#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io, os, sys
import simplejson as json
import datetime
import time
import redis

from twython import Twython, TwythonError

def now():
    return int(time.time())

iss_bucket_name   = 'xxx'

QUE_NAME = 'INITIALSTATE_PUSH' + iss_bucket_name

APP_KEY            = 'xxxx'
APP_SECRET         = 'xxxx'
OAUTH_TOKEN        = 'xx-xx'
OAUTH_TOKEN_SECRET = 'xxxx'

#
r_lasttweettime      = 'LSTTWTTM'

# redis
POOL = redis.ConnectionPool(host='192.168.10.2', port=16379, db=0)
r = redis.StrictRedis(connection_pool=POOL)

twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

try:
    r.ping()

except Exception as e:
    print(e)
    sys.exit()

maxqn = 0
qlist = [QUE_NAME]
for x in qlist:
    nofq = r.llen(x)
    if nofq >= maxqn:
        maxqn = nofq

if maxqn > 200:
    lasttweettime = r.get(r_lasttweettime)
    if(lasttweettime):
        if now() - int(lasttweettime) > 60:
            r.set(r_lasttweettime, now())
            twitter.update_status(status='having problem to update initialstate - ' + str(now()))

    else:
        r.set(r_lasttweettime, now())
        twitter.update_status(status='having problem to update initialstate - ' + str(now()))