import os
import time
from random import randrange

import RNS
from . import MP, RNSUtils

class RNSTransportNode:
    ASPECTS = ("rnstransport", "remote", "management")

    NODE_METRICS = {
        "rxb": "rns_transport_node_rx_bytes_total",
        "txb": "rns_transport_node_tx_bytes_total",
        "transport_uptime": "rns_transport_node_uptime",
        "link_count": "rns_transport_node_link_count", # returned as second array element, unlabelled...
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
        "incoming_announce_frequency": "rns_iface_announces_rx_rate",
        "outgoing_announce_frequency": "rns_iface_announces_tx_rate",
    }
    IFACE_LABELS = {
        "type": "type",
    }

    LPROTO_LABEL_TTABLE = str.maketrans({
        " ": "\\ ",
        ",": "\\,"
    })

    def __init__(self, interval: int, dest_identity: str, rpc_identity: os.PathLike, name: str, **kwargs) -> None:
        self.link = RNSUtils.establish_link(dest_identity, rpc_identity, self)
        self.interval = interval
        self.collection_jitter = kwargs.setdefault('collection_jitter', 0)

        # Used for metric labeling
        self.node_name = name
        self.dest_identity = dest_identity

        self.request_timeout = RNSUtils.set_request_timeout(self.link, interval)
        RNS.log(f"[RNMon] Set Request timeout for '{self.node_name}': {self.request_timeout}s", RNS.LOG_EXTREME)



        self.run()

    def run(self) -> bool:
        RNS.log(f"[RNMon] Starting RNSTransportNode scraper for '{self.node_name}'", RNS.LOG_INFO)
        last_request_time = time.time()
        jitter = 0
        while not MP.terminate.is_set():
            if self.link.status != RNS.Link.ACTIVE:
                RNS.log(f"[RNMon] Link no longer active, stopping scraper for '{self.node_name}", RNS.LOG_DEBUG)
                break
            try:
                # No point in spamming requests if the last one hasnt timed out yet, save local and network resources
                if not self.link.pending_requests and ((time.time() - last_request_time) > (self.interval + jitter)):
                    req = self.link.request(
                        "/status",
                        data = [True],
                        response_callback = self._on_response,
                        failed_callback = self._on_request_fail,
                        timeout = self.request_timeout
                    )
                    last_request_time = time.time()
                    jitter = randrange(-self.collection_jitter, self.collection_jitter+1)
                    RNS.log(f"[RNMon] Sending request {RNS.prettyhexrep(req.request_id)} to '{self.node_name}'", RNS.LOG_EXTREME)
            except Exception as e:
                RNS.log(f"[RNMon] Error while sending request to '{self.node_name}': {str(e)}")

            time.sleep(0.2)

        RNS.log(f"[RNMon] Stopping RNSTransportNode scraper for '{self.node_name}'", RNS.LOG_INFO)
        self.link.teardown()
        return False

    def _on_response(self, response) -> None:
        self._parse_metrics(response.response)

    def _on_request_fail(self, response) -> None:
        RNS.log(f"[RNMon] The request {RNS.prettyhexrep(response.request_id)} to '{self.node_name}' failed.", RNS.LOG_DEBUG)

    def _parse_metrics(self, data: list) -> None:
        iface_metrics, iface_labels = {}, {}
        node_metrics, node_labels = {}, {}
        t = time.time_ns()

        # link_count isnt labeled >.>
        node_metrics[RNSTransportNode.NODE_METRICS['link_count']] = data[1]

        for mk, mv in data[0].items():
            if mk == 'interfaces':
                for iface in mv:
                    if iface['short_name'].startswith('Client on'):
                        continue

                    for k, v in iface.items():
                        if k in RNSTransportNode.IFACE_METRICS:
                            iface_metrics[RNSTransportNode.IFACE_METRICS[k]] = v
                        if k in RNSTransportNode.IFACE_LABELS:
                            iface_labels[RNSTransportNode.IFACE_LABELS[k]] = v

                    iface_labels['name'] = iface['short_name']
                    iface_labels['identity'] = self.dest_identity
                    iface_labels['node_name'] = self.node_name

                    # convert to influx line format
                    labels = ",".join(f"{k}={v.translate(RNSTransportNode.LPROTO_LABEL_TTABLE)}" for k, v in iface_labels.items())
                    for k, v in iface_metrics.items():
                        metric = f"{k},{labels} value={v} {t}"
                        MP.metric_queue.append(metric)

            else:
                if mk in RNSTransportNode.NODE_METRICS:
                    node_metrics[RNSTransportNode.NODE_METRICS[mk]] = mv

                node_labels['identity'] = self.dest_identity
                node_labels['node_name'] = self.node_name

                #convert to influx line format
                labels = ",".join(f"{k}={v.translate(RNSTransportNode.LPROTO_LABEL_TTABLE)}" for k, v in node_labels.items())
                for k, v in node_metrics.items():
                    metric = f"{k},{labels} value={v} {t}"
                    MP.metric_queue.append(metric)

class LXMFPropagationNode:
    ASPECTS = ("lxmf", "propagation", "control")

    NODE_METRICS = {
        "autopeer_maxdepth": "lxmf_pn_autopeer_maxdepth",
        "delivery_limit": "lxmf_pn_delivery_limit",
        "discovered_peers": "lxmf_pn_discovered_peers_total",
        "max_peers": "lxmf_pn_max_peers",
        "propagation_limit": "lxmf_pn_propagation_limit",
        "unpeered_propagation_incoming": "lxmf_pn_unpeered_propagation_rx_total",
        "unpeered_propagation_rx_bytes": "lxmf_pn_unpeered_propagation_rx_bytes_total",
        "uptime": "lxmf_pn_uptime",
    }
    NODE_CLIENT_METRICS = {
        "client_propagation_messages_received": "lxmf_pn_client_propagation_messages_rx_total",
        "client_propagation_messages_served": "lxmf_pn_client_propagation_messages_tx_total",
    }
    NODE_MESSAGESTORE_METRICS = {
        "bytes": "lxmf_pn_msgstore_bytes_total",
        "count": "lxmf_pn_msgstore_count",
        "limit": "lxmf_pn_msgstore_limit",
    }
    PEER_METRICS = {
        "ler": "lxmf_pn_peer_link_establishment_rate",
        "str": "lxmf_pn_peer_sync_transfer_rate",
        "last_heard": "lxmf_pn_peer_last_heard",
        "last_sync_attempt": "lxmf_pn_peer_last_sync_attempt",
        "next_sync_attempt": "lxmf_pn_peer_next_sync_attempt",
        "state": "lxmf_pn_peer_state",
        "sync_backoff": "lxmf_pn_peer_sync_backoff",
        "peering_timebase": "lxmf_pn_peer_peering_timebase",
        "tx_bytes": "lxmf_pn_peer_tx_bytes_total",
        "rx_bytes": "lxmf_pn_peer_rx_bytes_total",
        "transfer_limit": "lxmf_pn_peer_transfer_limit_bytes",
        "network_distance": "lxmf_pn_peer_hop_count",
    }
    PEER_MESSAGE_METRICS = {
        "incoming": "lxmf_pn_peer_msg_rx_total",
        "offered": "lxmf_pn_peer_msg_offered_total",
        "outgoing": "lxmf_pn_peer_msg_tx_total",
        "unhandled": "lxmf_pn_peer_msg_unhandled_total",
    }
    PEER_LABELS = {
        "type": "type",
    }

    LPROTO_LABEL_TTABLE = str.maketrans({
        " ": "\\ ",
        ",": "\\,"
    })

    def __init__(self, interval: int, dest_identity: str, rpc_identity: os.PathLike, name: str, **kwargs) -> None:
        self.link = RNSUtils.establish_link(dest_identity, rpc_identity, self)
        self.interval = interval
        self.collection_jitter = kwargs.setdefault('collection_jitter', 0)

        # Used for metric labeling
        self.node_name = name
        self.dest_identity = dest_identity

        self.request_timeout = RNSUtils.set_request_timeout(self.link, interval)
        RNS.log(f"[RNMon] Set Request timeout: {self.request_timeout}s", RNS.LOG_EXTREME)
        self.run()

    def run(self) -> bool:
        RNS.log(f"[RNMon] Starting LXMFPropagationNode scraper for '{self.node_name}'", RNS.LOG_INFO)
        last_request_time = time.time()
        jitter = 0
        while not MP.terminate.is_set():
            if self.link.status != RNS.Link.ACTIVE:
                RNS.log(f"[RNMon] Link no longer active, stopping scraper for '{self.node_name}", RNS.LOG_DEBUG)
                break
            try:
                # No point in spamming requests if the last one hasnt timed out yet, save local and network resources
                if not self.link.pending_requests and ((time.time() - last_request_time) > (self.interval + jitter)):
                    req = self.link.request(
                        "/pn/get/stats",
                        data = [True],
                        response_callback = self._on_response,
                        failed_callback = self._on_request_fail,
                        timeout = self.request_timeout
                    )
                    last_request_time = time.time()
                    jitter = randrange(-self.collection_jitter, self.collection_jitter+1)
                    RNS.log(f"[RNMon] Sending request {RNS.prettyhexrep(req.request_id)} to '{self.node_name}'", RNS.LOG_EXTREME)
            except Exception as e:
                RNS.log(f"[RNMon] Error while sending request to '{self.node_name}': {str(e)}")

            time.sleep(0.2)

        RNS.log(f"[RNMon] Stopping LXMFPropagationNode scraper for '{self.node_name}'", RNS.LOG_INFO)
        self.link.teardown()
        return False

    def _on_response(self, response) -> None:
        #print(response.response)
        self._parse_metrics(response.response)

    def _on_request_fail(self, response) -> None:
        RNS.log(f"[RNMon] The request {RNS.prettyhexrep(response.request_id)} to '{self.dest_identity}' failed.", RNS.LOG_DEBUG)

    def _parse_metrics(self, data: dict) -> None:
        peer_metrics, peer_labels = {}, {}
        node_metrics, node_labels = {}, {}
        t = time.time_ns()

        for mk, mv in data.items():
            if mk == 'peers':
                for p, c in mv.items():
                    for k, v in c.items():
                        if k == 'messages':
                            for tp, n in v.items():
                                peer_metrics[LXMFPropagationNode.PEER_MESSAGE_METRICS[tp]] = n
                        if k in LXMFPropagationNode.PEER_METRICS:
                            peer_metrics[LXMFPropagationNode.PEER_METRICS[k]] = v
                        if k in LXMFPropagationNode.PEER_LABELS:
                            peer_labels[LXMFPropagationNode.PEER_LABELS[k]] = v

                    peer_labels['peer'] = p.hex()
                    peer_labels['identity'] = self.dest_identity
                    peer_labels['node_name'] = self.node_name

                    # convert to influx line format
                    labels = ",".join(f"{k}={v.translate(LXMFPropagationNode.LPROTO_LABEL_TTABLE)}" for k, v in peer_labels.items())
                    for k, v in peer_metrics.items():
                        metric = f"{k},{labels} value={v} {t}"
                        MP.metric_queue.append(metric)

            else:
                if mk == "clients":
                    for k, v in mv.items():
                        node_metrics[LXMFPropagationNode.NODE_CLIENT_METRICS[k]] = v
                if mk == "messagestore":
                    for k, v in mv.items():
                        node_metrics[LXMFPropagationNode.NODE_MESSAGESTORE_METRICS[k]] = v
                if mk in LXMFPropagationNode.NODE_METRICS:
                    node_metrics[LXMFPropagationNode.NODE_METRICS[mk]] = mv

                node_labels['identity'] = self.dest_identity
                node_labels['node_name'] = self.node_name

                #convert to influx line format
                labels = ",".join(f"{k}={v.translate(LXMFPropagationNode.LPROTO_LABEL_TTABLE)}" for k, v in node_labels.items())
                for k, v in node_metrics.items():
                    metric = f"{k},{labels} value={v} {t}"
                    MP.metric_queue.append(metric)
