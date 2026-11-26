"""代收点 collection_point 命名入口（注意区别于馆藏 collection）。"""

from strata.field.agent import Agent

CollectionPoint = Agent
CollectionAgent = Agent

__all__ = ["CollectionPoint", "CollectionAgent", "Agent"]
