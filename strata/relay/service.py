"""strata.relay.service 兼容入口。"""

from strata.transfer.relay import Relays

RelayService = Relays
CenterRelay = Relays

__all__ = ["RelayService", "CenterRelay", "Relays"]
