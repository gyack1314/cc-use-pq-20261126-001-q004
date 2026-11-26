"""代收点 station 命名入口。"""

from strata.field.agent import Agent, DirectGateway, HttpGateway

Station = Agent
RelayStation = Agent
AgentStation = Agent
LocalStation = Agent

__all__ = ["Station", "RelayStation", "AgentStation", "LocalStation", "Agent",
           "DirectGateway", "HttpGateway"]
