"""代收点 custodian / desk 命名入口。"""

from strata.field.agent import Agent

Custodian = Agent
CustodyPoint = Agent
Desk = Agent
RelayDesk = Agent

__all__ = ["Custodian", "CustodyPoint", "Desk", "RelayDesk", "Agent"]
