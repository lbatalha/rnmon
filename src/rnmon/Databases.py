import time
import requests
from random import randrange
from collections import deque

import RNS
from . import MP

class InfluxWriter:
    def __init__(self, address: str, batch_size: int = 1000, flush_interval: int = 5, **kwargs):
        self.maxlen = batch_size
        self.flush_interval = flush_interval
        self.flush_jitter = kwargs.setdefault('flush_jitter', 0)
        self.address = address
        self.http_headers = kwargs.setdefault('http_headers', None)
        self.run()

    def run(self):
        last_push = time.time()
        jitter = 0
        RNS.log("[RNMon] Started InfluxWriter", RNS.LOG_INFO)
        while not MP.terminate.is_set():
            if len(MP.metric_queue) >= self.maxlen or (time.time() - last_push) >  (self.flush_interval + jitter):
                data = []
                try:
                    for _ in range(self.maxlen):
                        data.append(MP.metric_queue.pop())
                except IndexError:
                    pass
                if data:
                    RNS.log(f"[RNMon] Pushing metrics - Count: {len(data)} Time: {int(time.time() - last_push)}s", RNS.LOG_DEBUG)
                    r = requests.post(self.address, headers=self.http_headers, data="\n".join(data))
                last_push = time.time()
                jitter = randrange(-self.flush_jitter, self.flush_jitter+1)
            time.sleep(0.2)

        RNS.log(f"[RNMon] Stopped InfluxWriter", RNS.LOG_INFO)
