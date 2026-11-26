"""strata.relay.handoffs 兼容入口（中心侧服务）。"""

from strata.transfer.relay import Relays

Handoffs = Relays
RelayService = Relays

__all__ = ["Handoffs", "Relays", "RelayService"]
