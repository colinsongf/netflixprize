#import logging
#logging.basicConfig(level=logging.INFO)
#broker logging everything
#logging.getLogger("Broker").setLevel(logging.DEBUG)

from time import time

import Broker

destination = '/netflix/work'
kind = 'QUEUE'

broker = Broker.Client('localhost', 3322)
broker.subscribe(destination, kind)

def consume(n):
    for id in xrange(1000):
        msg = broker.consume()

while True:
    n = 1000
    t = time()
    consume(n)
    d = time()-t
    print "consumed %f msg/s" % (n/d)
