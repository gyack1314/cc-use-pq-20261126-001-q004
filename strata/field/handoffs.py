"""交割/转交命名入口（复数）。"""

from strata.field.agent import Agent, DirectGateway, HttpGateway

HandoffService = Agent
HandoffClient = Agent

__all__ = ["Agent", "HandoffService", "HandoffClient", "DirectGateway", "HttpGateway"]
