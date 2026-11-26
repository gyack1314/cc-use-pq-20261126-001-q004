"""中心侧代收点服务的 agents 命名入口。"""

from strata.transfer.relay import Relays

Agents = Relays
AgentRegistry = Relays
Stations = Relays
Points = Relays

__all__ = ["Relays", "Agents", "AgentRegistry", "Stations", "Points"]
