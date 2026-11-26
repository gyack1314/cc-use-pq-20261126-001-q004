"""strata.relay.agent 兼容入口。"""

from strata.field.agent import Agent, DirectGateway, HttpGateway

RelayAgent = Agent
RelayStation = Agent
Station = Agent

__all__ = ["Agent", "RelayAgent", "RelayStation", "Station", "DirectGateway", "HttpGateway"]
