"""中心侧交割记录服务的 handoffs 命名入口。"""

from strata.transfer.relay import Relays, as_digests, pick

Handoffs = Relays
HandoffService = Relays
Relay = Relays
RelayService = Relays

__all__ = ["Relays", "Handoffs", "HandoffService", "Relay", "RelayService", "as_digests", "pick"]
