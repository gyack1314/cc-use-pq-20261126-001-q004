"""strata.relay：代收转交能力的统一命名空间。"""

from strata.field.agent import Agent, DirectGateway, HttpGateway
from strata.transfer.relay import Relays

RelayAgent = Agent
RelayStation = Agent
Station = Agent
RelayPoint = Agent
Handoffs = Relays

__all__ = [
    "Agent", "RelayAgent", "RelayStation", "Station", "RelayPoint",
    "Relays", "Handoffs", "DirectGateway", "HttpGateway",
]
