"""交割/转交命名入口。"""

from strata.field.agent import Agent, DirectGateway, HttpGateway

HandoffAgent = Agent
HandoffStation = Agent
Handoffs = Agent
RelayHandoff = Agent

__all__ = ["Agent", "HandoffAgent", "HandoffStation", "Handoffs", "RelayHandoff",
           "DirectGateway", "HttpGateway"]
