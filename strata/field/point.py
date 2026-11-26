"""代收点 point 命名入口。"""

from strata.field.agent import Agent

Point = Agent
RelayPoint = Agent
CollectionPoint = Agent

__all__ = ["Point", "RelayPoint", "CollectionPoint", "Agent"]
