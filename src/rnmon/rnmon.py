import os
import sys
import time
import random
import re
import argparse
import importlib
import concurrent.futures
import multiprocessing as mp
from collections import deque
from pprint import pp
from queue import Empty, Full

import requests
from yaml import safe_load, dump

import RNS
RNS.Link.KEEPALIVE=1
RNS.Link.STALE_TIME=2*RNS.Link.KEEPALIVE
# TODO: PR to have these be configurable as arguments to RNS.Link.__init__

# Line protocol translation table to escape invalid tag/label characters
lproto_label_ttable = str.maketrans({
    " ": "\\ ",
    ",": "\\,"
})



metric_queue = mp.Queue(10000)

class RNSRemote:

    ASPECTS = ("rnstransport", "remote", "management")

    NODE_METRICS = {
        "rxb": "rns_transport_node_rx_bytes_total",
        "txb": "rns_transport_node_tx_bytes_total",
        "transport_uptime": "rns_transport_node_uptime_s",
        "link_count": "rns_transport_node_link_count", # returned as second array element, unlabelled...
    }
    NODE_LABELS = {
        "transport_id": "transport_id",
    }
    IFACE_METRICS = {
        "clients": "rns_iface_client_count",
        "bitrate": "rns_iface_bitrate",
        "status": "rns_iface_up",
        "mode": "rns_iface_mode",
        "rxb": "rns_iface_rx_bytes_total",
        "txb": "rns_iface_tx_bytes_total",
        "held_announces": "rns_iface_announces_held_count",
        "announce_queue": "rns_iface_announces_queue_count",
    }
    IFACE_LABELS = {
        "type": "type",
        "short_name": "name",
    }

    def __init__(self, interval: int, dest_ident_hexhash: str, identity: os.PathLike, configpath: str, verbosity: int, **kwargs):
        self.alive: bool = True
        self.link: RNS.Link
        self.remote_dest: RNS.Destination

        self.interval = interval
        self.dest_ident_hexhash = dest_ident_hexhash # Destination identity hex hash
        self.identity = identity # Identity file path to use to identify for remote management
        self.configpath = configpath

        self.request_timeout = interval

        validate_hexhash(self.dest_ident_hexhash)

        # Derive destination application address hash from identity hex hash
        self.dest_hash = RNS.Destination.hash_from_name_and_identity( \
            '.'.join(RNSRemote.ASPECTS), bytes.fromhex(self.dest_ident_hexhash))

        # Initialize Reticulum Instance
        self.rns = RNS.Reticulum(configdir=configpath, verbosity=verbosity)

        RNS.log(f"Loading identity from '{self.identity}'", RNS.LOG_INFO)
        self.mgmt_identity = RNS.Identity.from_file(os.path.expanduser(self.identity))
        if not self.mgmt_identity:
            #RNS.Identity.load() should propagate exceptions... q_q
            raise FileNotFoundError("Failed to load identity, check path and permissions.")
        RNS.log(f"Loaded identity from '{self.identity}'", RNS.LOG_INFO)

        RNS.log(f"Setting up Destination Config: {RNS.prettyhexrep(self.dest_hash)}")
        self.remote_dest = RNS.Destination(
            RNS.Identity.recall(self.dest_hash),
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            *RNSRemote.ASPECTS
        )

        self._establish_link()
        self.run()

    def run(self):
        print(f"Starting Main")
        while self.alive:
            # try:
            #    print(metric_queue.get_nowait())
            # except Empty:
            #     RNS.log("Metric queue is empty, nothing to get", RNS.LOG_WARNING)
            if self.link.status != RNS.Link.ACTIVE:
                RNS.log(f"Link Status: {self.link.status}", RNS.LOG_DEBUG) #DEBUG
                RNS.log(f"Link establishment timeout: {self.link.establishment_timeout}", RNS.LOG_DEBUG) #DEBUG
                time.sleep(0.2) #DEBUG
                continue
            try:
                # No point in spamming requests if the last one hasnt timed out yet, save local and network resources
                if not self.link.pending_requests:
                    req = self.link.request(
                        "/status",
                        data = [True],
                        response_callback = self._on_response,
                        failed_callback = self._on_request_fail,
                        timeout = self.request_timeout
                    )
                    RNS.log(f"Sending request {RNS.prettyhexrep(req.request_id)}", RNS.LOG_EXTREME)
            except Exception as e:
                RNS.log(f"Error while sending request over the link: {str(e)}")

            time.sleep(self.interval)


    def _ensure_path(self):
        if not RNS.Transport.has_path(self.dest_hash):
            RNS.log("No path to destination known. Requesting path and waiting for announce to arrive...")
            RNS.Transport.request_path(self.dest_hash)
            while not RNS.Transport.has_path(self.dest_hash):
                time.sleep(0.2)

    def _establish_link(self):
        # Always make sure we have a valid path before linking, there is an edge case where when >1 clients
        # are connected to the same shared instance, if the destination being linked to is directly connected
        # to their shared instance and it dies or gets restarted, the link establishment timeout is set to maximum value
        # because the path state is somehow lost or expires. hops_to() returns Transport.PATHFINDER_M causing the Link.__init__
        # establishment_timeout to be huge (https://github.com/markqvist/Reticulum/blob/9a1884cfecf0daa20afc194432569208c7e457c3/RNS/Link.py#L277)
        # I dont know what race condition or weird edge case causes this Link reset, and it only happens with a shared instance with >1
        # clients and only if their link dest is directly connected to their own instance. Just restarting the destination
        # continuosly will eventually trigger the issue. This happens even if there is a circular topology with 3 rns instances
        # where the shared instance is connected to two others, and those are each connected between themselves
        self._ensure_path()
        RNS.log("Establishing a new link with server")
        self.link = RNS.Link(self.remote_dest)
        self.link.set_link_established_callback(self._on_link_established)
        self.link.set_link_closed_callback(self._on_link_closed)

    def _on_link_established(self, link: RNS.Link) -> None:
        RNS.log("Link established with server", RNS.LOG_DEBUG)
        RNS.log(f"KEEPALIVE interval: {link.KEEPALIVE}s, Stale time: {link.stale_time}s", RNS.LOG_DEBUG)
        link.identify(self.mgmt_identity)
        self.request_timeout = link.rtt * link.traffic_timeout_factor + RNS.Resource.RESPONSE_MAX_GRACE_TIME*1.125
        if self.request_timeout >= self.interval:
           self.request_timeout = self.interval
        RNS.log(f"Set Request timeout: {self.request_timeout}s", RNS.LOG_DEBUG)

    def _on_link_closed(self, link: RNS.Link) -> None:
        reason = link.teardown_reason
        if reason == RNS.Link.TIMEOUT:
            RNS.log("The link timed out, reconnecting", RNS.LOG_WARNING)
        elif reason == RNS.Link.DESTINATION_CLOSED:
            RNS.log("The link was closed by the server, reconnecting", RNS.LOG_WARNING)
        else:
            RNS.log("Link closed unexpectedly, terminating", RNS.LOG_ERROR)
        self._establish_link()

    def _on_response(self, response: RNS.RequestReceipt) -> None:
        #pp(response.response) #DEBUG
        self._parse_metrics(response.response)

    def _on_request_fail(self, response: RNS.RequestReceipt) -> None:
        RNS.log(f"The request {RNS.prettyhexrep(response.request_id)} failed.", RNS.LOG_DEBUG)

    def _parse_metrics(self, data: list):
        iface_labels = {}
        iface_metrics = {}
        node_labels = {}
        node_metrics = {}
        t = time.time()
        #pp(data[0])

        # link_count isnt labeled >.>
        node_metrics[RNSRemote.NODE_METRICS['link_count']] = data[1]

        for mk, mv in data[0].items():
            if mk == 'interfaces':
                for iface in mv:
                    for k, v in iface.items():
                        if k in RNSRemote.IFACE_METRICS:
                            iface_metrics[RNSRemote.IFACE_METRICS[k]] = v
                        if k in RNSRemote.IFACE_LABELS:
                            iface_labels[RNSRemote.IFACE_LABELS[k]] = v.translate(lproto_label_ttable)
                    iface_labels['identity'] = self.dest_ident_hexhash

                    # convert to influx line format
                    labels = ",".join(f"{k}={v}" for k, v in iface_labels.items())
                    for k, v in iface_metrics.items():
                        metric = f"{k},{labels} value={v} {t}"
                        try:
                            metric_queue.put_nowait(metric)
                        except Full:
                            RNS.log("Metric queue is full, dropping older metrics", RNS.LOG_EXTREME)
                            metric_queue.get_nowait()
                            metric_queue.put_nowait(metric)
            else:
                if mk in RNSRemote.NODE_METRICS:
                    node_metrics[RNSRemote.NODE_METRICS[mk]] = mv

                node_labels['identity'] = self.dest_ident_hexhash
                #convert to influx line format
                labels = ",".join(f"{k}={v}" for k, v in node_labels.items())
                for k, v in node_metrics.items():
                    metric = f"{k},{labels} value={v} {t}"
                    try:
                        metric_queue.put_nowait(metric)
                    except Full:
                        RNS.log("Metric queue is full, dropping older metrics", RNS.LOG_EXTREME)
                        metric_queue.get_nowait()
                        metric_queue.put_nowait(metric)


def validate_hexhash(hexhash: str) -> None:
        dest_len = (RNS.Reticulum.TRUNCATED_HASHLENGTH//8)*2
        if len(hexhash) != dest_len:
            raise TypeError(f"Destination length is invalid, must be {dest_len} hexadecimal characters ({dest_len//2} bytes)")

class InfluxWriter:
    def __init__(self, batch_size: int = 1000, flush_interval: int = 5, **kwargs):
        self.maxlen = batch_size
        self.flush_interval = flush_interval
        self.run()

    def run(self):
        push_queue = deque([],maxlen=self.maxlen)
        metric_count: int = 0
        last_push = time.time()
        print("Started InfluxWriter")
        with open("/Users/lbatalha/src/rnmon/metrics.log", "w", buffering=1) as f:
            while True:
                try:
                    push_queue.append(metric_queue.get_nowait())
                    metric_count += 1
                except Empty:
                    pass
                if metric_count == self.maxlen-1 or time.time() - last_push >  self.flush_interval:
                    print(f"[InfluxWriter] Pushing metrics - Count: {metric_count} Time: {int(time.time() - last_push)}s")
                    metric_count = 0
                    last_push = time.time()
                    f.write("\n".join(push_queue))

                time.sleep(0.005)


def main():
    JOB_TYPES = {
        "transport_node": RNSRemote,
        "influx": InfluxWriter
    }
    try:
        parser = argparse.ArgumentParser(description="Simple request/response example")
        parser.add_argument('-v', '--verbose', action='count', default=0)
        parser.add_argument("--config", type=str, default=None, \
                            help="path to Reticulum config directory")
        parser.add_argument("targets", nargs='?', type=argparse.FileType('r'), default="scraping.yaml", \
                            help="path to target list file")
        args = parser.parse_args()

        target_config = safe_load(args.targets)

        with concurrent.futures.ProcessPoolExecutor(mp_context=mp.get_context("fork")) as executor:
            jobs = []
            # Setup Scraping Jobs
            for target in target_config['targets']:
                jobs.append({
                    "type": target['type'],
                    "interval": target['interval'],
                    "dest_ident_hexhash": target['dest_identity'],
                    "identity": target['rpc_identity'],
                    "configpath": args.config,
                    "verbosity": args.verbose
                })
            # Setup InfluxWriter push job
            jobs.append({
                "type": "influx",
                "batch_size": target_config['batch_size'],
                "flush_interval": target_config['flush_interval'],
            })
            # I WILL commit crimes >:3
            futures = {executor.submit(JOB_TYPES[job['type']], **job): job for job in jobs}
            while len(futures) > 0:
                new_jobs = {}
                done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    if future.exception():
                        print(f"Job exited with exception: \"{future.exception()}\"")
                        job = futures[future]
                        new_jobs[executor.submit(JOB_TYPES[job['type']], **job)] = job
                for future in not_done:
                    job = futures[future]
                    new_jobs[executor.submit(JOB_TYPES[job['type']], **job)] = job
                futures = new_jobs

    except KeyboardInterrupt:
        sys.exit(0)

if __name__ == '__main__':
    main()
