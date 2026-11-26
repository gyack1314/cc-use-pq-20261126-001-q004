"""代收/collect 命名入口。"""

from strata.field.agent import Agent

CollectPoint = Agent
CollectionDesk = Agent
Receiver = Agent
IntakePoint = Agent

__all__ = ["Agent", "CollectPoint", "CollectionDesk", "Receiver", "IntakePoint"]
