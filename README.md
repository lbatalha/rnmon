# RNMon - Reticulum Application Monitoring Daemon

RNMon is a simple monitoring daemon designed to monitor the status of multiple RNS applications and push the metrics over http using the influx line protocol.

## Supported Applications

* Reticulum Transport Nodes (Also some metrics for non transport nodes)
* LXMF Propagation Nodes

## Installing

The package is available in [PyPI](https://pypi.org/project/rnmon/), install it with your python package manager of choice

I recommend using [`uv`](https://docs.astral.sh/uv/) since it cleanly manages an environment if you run or install it as a tool:

Execute it simply: `uvx rnmon`

Install it globally (but in its own environment): `uv tool install rnmon` and run `rnmon`

## Configuration

Configure the daemon via `scraping.yaml`, the example config has comments explaining the options.

The configuration for reticulum is auto-discovered, but you can specify the location of the configuration directory using the `--rns-config` argument.

## Operational principles

The metric pusher and all targets are executed in their own thread. The main thread starts a new RNS instance, and closes it on exit.

A link is established for each scrape target to reduce network overhead. If a link is broken for any reason, the thread is terminated and restarted - this avoids having to deal with the built-in RNS link retry mechanisms, their associated timeouts and any edge cases caused by using shared RNS intances. This might be changed in the future if RNS fixes the issues particular to this use case.
