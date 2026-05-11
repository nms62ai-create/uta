"""First-party producer integrations.

Each subpackage here is a reference integration between UTA's embedded
:class:`trade_adapter.embedded.TradeAdapter` and a specific producer
(heatmap-sdk, channel bots, manual-trading UIs, etc.). Integrations are
optional: they live in their own namespace so they can be imported a-la-
carte and have zero impact on the core trading stack when unused.
"""
