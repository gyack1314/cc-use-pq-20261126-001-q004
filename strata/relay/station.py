"""strata.relay.station 兼容入口。"""

from strata.field.agent import Agent

RelayStation = Agent
Station = Agent

__all__ = ["RelayStation", "Station", "Agent"]
