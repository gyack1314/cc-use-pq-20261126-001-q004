"""depot / courier 命名入口。"""

from strata.field.agent import Agent

Depot = Agent
CourierDesk = Agent
Courier = Agent

__all__ = ["Depot", "CourierDesk", "Courier", "Agent"]
