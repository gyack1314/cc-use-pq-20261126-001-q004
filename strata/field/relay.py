"""代收点现场组件的同义入口：relay / station 命名。"""

from strata.field.agent import (
    Agent,
    DirectGateway,
    HttpGateway,
)

RelayAgent = Agent
RelayStation = Agent
RelayPoint = Agent
RelayDesk = Agent
Station = Agent
LocalStation = Agent
CustodyPoint = Agent
CollectionPoint = Agent
Point = Agent
Desk = Agent
Relay = Agent

__all__ = [
    "Agent", "RelayAgent", "RelayStation", "RelayPoint", "RelayDesk", "Station",
    "LocalStation", "CustodyPoint", "CollectionPoint", "Point", "Desk", "Relay",
    "DirectGateway", "HttpGateway",
]
