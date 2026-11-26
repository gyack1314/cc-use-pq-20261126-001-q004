"""代收点组件的复数模块名入口。"""

from strata.field.agent import Agent, DirectGateway, HttpGateway

Agents = Agent
RelayAgents = Agent

__all__ = ["Agent", "Agents", "RelayAgents", "DirectGateway", "HttpGateway"]
