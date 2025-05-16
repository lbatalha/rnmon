import sys
import time
import signal
import argparse
import concurrent.futures

from yaml import safe_load

import RNS
# RNS.Link.KEEPALIVE=10
# RNS.Link.STALE_TIME=2*RNS.Link.KEEPALIVE
# TODO: PR to have these be configurable as arguments to RNS.Link.__init__

from .Databases import InfluxWriter
from .Remotes import LXMFPropagationNode, RNSTransportNode
from . import MP, RNSUtils



def main():
    parser = argparse.ArgumentParser(description="Simple request/response example")
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument("--rns-config", type=str, default=None, \
                        help="path to Reticulum config directory")
    parser.add_argument("config", nargs='?', type=argparse.FileType('r'), default="scraping.yaml", \
                        help="path to target list file")
    args = parser.parse_args()

    config = safe_load(args.config)

    JOB_TYPES = {
        "transport_node": RNSTransportNode,
        "lxmf_propagation_node": LXMFPropagationNode,
        "influx": InfluxWriter
    }

    # Init Reticulum instance
    RNS.Reticulum(configdir=args.rns_config, verbosity=args.verbose)

    def sig_handler(signum, frame):
        MP.terminate.set()
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    jobs = []
    # Setup InfluxWriter push job
    jobs.append({"type": "influx"} | config['influxdb'])
    # Setup Scraping Jobs
    for target in config['targets']:
        RNSUtils.validate_hexhash(target['dest_identity'])
        jobs.append({"verbosity": args.verbose} | target)

    futures = {}
    with concurrent.futures.ThreadPoolExecutor(len(jobs)) as executor:

        for job in jobs:
            futures[executor.submit(JOB_TYPES[job['type']], **job )] = job

        while len(futures) > 0:
            new_jobs = {}
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            if MP.terminate.is_set():
                break
            for future in done:
                job = futures[future]
                if future.exception():
                    RNS.log(f"[RNMon] Job exited with exception: \"{future.exception()}\"", RNS.LOG_WARNING)
                    RNS.log(f"[RNMon] Job Exception Restart: {JOB_TYPES[job['type']]}", RNS.LOG_WARNING)
                new_jobs[executor.submit(JOB_TYPES[job['type']], **job)] = job
            for future in not_done:
                job = futures[future]
                new_jobs[future] = job
            futures = new_jobs
            time.sleep(1)


    # RNS.exit() calls os._exit(), It does not clean things up properly
    # and triggers semaphore_tracker:UserWarning since multiprocessing.resource_tracker
    # is running, as intended, until it is terminated on main thread exit
    # RNS.exit()
    RNS.Reticulum.exit_handler()
    sys.exit(0)

if __name__ == '__main__':
    main()
