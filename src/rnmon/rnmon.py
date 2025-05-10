import os
import sys
import time
import random
import argparse
import pprint
import concurrent.futures
import multiprocessing

import RNS
RNS.Link.KEEPALIVE=1
RNS.Link.STALE_TIME=2*RNS.Link.KEEPALIVE
# TODO: PR to have these be configurable as arguments to RNS.Link.__init__

class RNSRemote:

    ASPECTS = ("rnstransport", "remote", "management")

    def __init__(self, interval: int, dest_ident_hexhash: str, identity: os.PathLike, configpath):
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
        self.rns = RNS.Reticulum(configdir=configpath, verbosity=2)

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
            if self.link.status != RNS.Link.ACTIVE:
                RNS.log(f"Link Status: {self.link.status}", RNS.LOG_DEBUG) #DEBUG
                RNS.log(f"Link establishment timeout: {self.link.establishment_timeout}", RNS.LOG_DEBUG) #DEBUG
                time.sleep(1) #DEBUG
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
        data = response.response
        RNS.log(f"{self.dest_ident_hexhash} responded with size: {len(data)}") #DEBUG
        #pprint.pp(len(data)) #DEBUG

    def _on_request_fail(self, response: RNS.RequestReceipt) -> None:
        RNS.log(f"The request {RNS.prettyhexrep(response.request_id)} failed.", RNS.LOG_DEBUG)


def validate_hexhash(hexhash: str) -> None:
        dest_len = (RNS.Reticulum.TRUNCATED_HASHLENGTH//8)*2
        if len(hexhash) != dest_len:
            raise TypeError(f"Destination length is invalid, must be {dest_len} hexadecimal characters ({dest_len//2} bytes)")

def main():
    try:
        parser = argparse.ArgumentParser(description="Simple request/response example")
        parser.add_argument(
            "-n",
            "--interval",
            action="store",
            default=60,
            help="path to alternative Reticulum config directory",
            type=int
        )
        parser.add_argument(
            "--config",
            action="store",
            default=None,
            help="path to alternative Reticulum config directory",
            type=str
        )
        parser.add_argument(
            "-i",
            "--identity",
            action="store",
            default=None,
            help="path to identity to usee for remote management",
            type=str
        )
        parser.add_argument(
            "destination",
            nargs="?",
            default=None,
            help="hexadecimal hash of the destination identity",
            type=str
        )

        args = parser.parse_args()

        if args.config:
            configarg = args.config
        else:
            configarg = None

        if (args.destination == None):
            print("")
            parser.print_help()
            print("")
        else:
            with concurrent.futures.ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn")) as executor:
                jobs = [
                    {"interval": args.interval, "dest_ident_hexhash": args.destination, "identity": args.identity, "configpath": configarg},
                    {"interval": args.interval, "dest_ident_hexhash": "someotherhash", "identity": args.identity, "configpath": configarg}
                ]
                futures = {executor.submit(RNSRemote, **job): job for job in jobs}
                while len(futures) > 0:
                    new_jobs = {}
                    print(new_jobs)
                    done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                    for future in done:
                        if future.exception():
                            print(f"Job exited with exception: \"{future.exception()}\"")
                            job = futures[future]
                            new_jobs[executor.submit(RNSRemote, **job)] = job
                    for future in not_done:
                        job = futures[future]
                        new_jobs[executor.submit(RNSRemote, **job)] = job
                    futures = new_jobs

    except KeyboardInterrupt:
        print("")
        sys.exit(0)

if __name__ == '__main__':
    main()
