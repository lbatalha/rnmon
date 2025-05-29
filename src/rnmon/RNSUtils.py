import os
import time

import RNS

path_request_timeout = 30
link_est_timeout = 30

def establish_link(dest_identity: str, rpc_identity: os.PathLike, dest_type) -> RNS.Link:
    mgmt_identity = read_rpc_identity(rpc_identity)
    dest_hash = RNS.Destination.hash_from_name_and_identity( \
                '.'.join(dest_type.ASPECTS), bytes.fromhex(dest_identity))
    ensure_path(dest_hash)
    RNS.log(f"[RNMon] Setting up Destination: {RNS.prettyhexrep(dest_hash)}", RNS.LOG_INFO)
    remote_dest = RNS.Destination(
        RNS.Identity.recall(dest_hash),
        RNS.Destination.OUT,
        RNS.Destination.SINGLE,
        *dest_type.ASPECTS
    )
    RNS.log(f"[RNMon] Establishing a new link with {RNS.prettyhexrep(dest_hash)}", RNS.LOG_DEBUG)
    link = RNS.Link(remote_dest)
    link.set_link_established_callback(on_link_established(link, mgmt_identity))
    link.set_link_closed_callback(on_link_closed)

    start = time.time()
    while link.status != RNS.Link.ACTIVE:
        # Sometimes with multiple clients in a shared instance, it is possible that
        # the establishment timeout is reset to PATHFINDER_M due to some edge cases
        if time.time() - start > link_est_timeout:
            RNS.log("[RNMon] Timed out waiting for link establishment", RNS.LOG_ERROR)
            raise RuntimeError
        RNS.log(f"[RNMon] Link to {dest_identity} Status: {link.status}", RNS.LOG_DEBUG)
        time.sleep(1)
    link.identify(mgmt_identity)
    return link

def on_link_established(link: RNS.Link, rpc_identity: RNS.Identity) -> None:
    RNS.log("[RNMon] Link established with server", RNS.LOG_DEBUG)
    RNS.log(f"[RNMon] KEEPALIVE interval: {link.KEEPALIVE}s, Stale time: {link.stale_time}s", RNS.LOG_DEBUG)

def on_link_closed(link: RNS.Link) -> None:
    reason = link.teardown_reason
    if reason == RNS.Link.TIMEOUT:
        RNS.log("[RNMon] The link timed out", RNS.LOG_WARNING)
    elif reason == RNS.Link.DESTINATION_CLOSED:
        RNS.log("[RNMon] The link was closed by the server", RNS.LOG_WARNING)
    elif reason == RNS.Link.INITIATOR_CLOSED:
        RNS.log("[RNMon] Closing link", RNS.LOG_ERROR)

def ensure_path(dest_hash: bytes) -> None:
    if not RNS.Transport.has_path(dest_hash):
        RNS.log("[RNMon] No path to destination known. Requesting path and waiting for announce to arrive...")
        RNS.Transport.request_path(dest_hash)
        start = time.time()
        while not RNS.Transport.has_path(dest_hash):
            time.sleep(0.2)
            # Abort if the path request is taking too long, for example if the next transport_node
            # is not in a mode that does path discovery for us, it might take a while to discover the path
            if time.time() - start > path_request_timeout:
                RNS.log("[RNMon] Timed out waiting for path announcement", RNS.LOG_ERROR)
                raise RuntimeError

def set_request_timeout(link: RNS.Link, interval: int) -> int:
        request_timeout = int(link.rtt * link.traffic_timeout_factor + RNS.Resource.RESPONSE_MAX_GRACE_TIME*1.125)
        if request_timeout >= interval:
            request_timeout = interval
        return request_timeout

def read_rpc_identity(rpc_identity: os.PathLike) -> RNS.Identity:
    mgmt_identity = RNS.Identity.from_file(os.path.expanduser(rpc_identity))
    if not mgmt_identity:
        raise FileNotFoundError(f"Failed to load identity from {rpc_identity}, check path and permissions.")
    RNS.log(f"[RNMon] Loaded identity from '{rpc_identity}'", RNS.LOG_INFO)
    return mgmt_identity


def validate_hexhash(hexhash: str) -> None:
        dest_len = (RNS.Reticulum.TRUNCATED_HASHLENGTH//8)*2
        if len(hexhash) != dest_len:
            raise TypeError(f"Destination length is invalid, must be {dest_len} hexadecimal characters ({dest_len//2} bytes)")
