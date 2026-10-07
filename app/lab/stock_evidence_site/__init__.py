"""Standalone read-only AI-Stock-Trader evidence review site.

See docs/stock_evidence_site.md for purpose, runtime boundary, and
deployment. This package is a separate application with its own
loopback listener and its own Tailscale Serve hostname; it does not
depend on, or modify, the existing Homelab ``lab-dashboard.service``
or its private packet API.
"""
