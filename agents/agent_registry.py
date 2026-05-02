"""
Agent Registry

Maintains thread-safe registry of ServiceAgent instances, one per (service_id, cloud) pair.
Ensures lazy initialization and strict isolation across different service/cloud combinations.
"""

import logging
import threading
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class AgentRegistry:
    """
    Thread-safe registry for ServiceAgent instances.

    Maintains a Dict[(service_id, cloud), ServiceAgent] mapping.
    Each unique (service_id, cloud) pair gets exactly one agent instance,
    lazily initialized on first use.

    Thread-safe for concurrent access from multiple partition workers.
    """

    def __init__(self):
        """Initialize the AgentRegistry."""
        self._agents: Dict[Tuple[str, str], object] = {}  # Will be ServiceAgent
        self._lock = threading.Lock()
        logger.info("AgentRegistry initialized")

    def get_or_create_agent(
        self,
        service_id: str,
        cloud: str,
        agent_factory,
        agent_config: Optional[dict] = None,
    ) -> object:
        """
        Get or create a ServiceAgent for a specific (service_id, cloud) pair.

        Thread-safe lazy initialization. If agent doesn't exist, factory is called
        to create it with provided config.

        Args:
            service_id: Service ID (e.g., "service-api").
            cloud: Cloud provider (e.g., "aws", "gcp", "azure").
            agent_factory: Callable that creates a ServiceAgent.
                          Signature: agent_factory(service_id: str, cloud: str, config: dict) -> ServiceAgent
            agent_config: Configuration dict passed to agent_factory.
                         If None, factory should use defaults.

        Returns:
            ServiceAgent instance for this (service_id, cloud) pair.

        Raises:
            ValueError: If service_id or cloud is empty or None.
            Exception: Propagated from agent_factory if creation fails.
        """
        if not service_id or not cloud:
            raise ValueError(f"service_id and cloud must be non-empty (got service_id={service_id!r}, cloud={cloud!r})")

        key = (service_id, cloud)

        # Fast path: check without lock
        if key in self._agents:
            return self._agents[key]

        # Slow path: create with lock
        with self._lock:
            # Double-check inside lock
            if key in self._agents:
                return self._agents[key]

            # Create new agent
            config = agent_config or {}
            agent = agent_factory(service_id, cloud, config)
            self._agents[key] = agent

            logger.info(f"Created agent for (service_id={service_id}, cloud={cloud})")
            return agent

    def get_agent(self, service_id: str, cloud: str) -> Optional[object]:
        """
        Get an existing agent, or None if not yet created.

        Args:
            service_id: Service ID.
            cloud: Cloud provider.

        Returns:
            ServiceAgent if exists, None otherwise.
        """
        return self._agents.get((service_id, cloud))

    def get_all_agents(self) -> Dict[Tuple[str, str], object]:
        """
        Get all registered agents.

        Returns:
            Dict mapping (service_id, cloud) to ServiceAgent instances.
        """
        with self._lock:
            return dict(self._agents)

    def close_all(self):
        """
        Close all registered agents.

        Call during graceful shutdown. Calls agent.close() on each instance.
        """
        with self._lock:
            agents_to_close = list(self._agents.items())

        for (service_id, cloud), agent in agents_to_close:
            try:
                if hasattr(agent, 'close'):
                    agent.close()
                logger.info(f"Closed agent for (service_id={service_id}, cloud={cloud})")
            except Exception as e:
                logger.error(
                    f"Error closing agent (service_id={service_id}, cloud={cloud}): {e}",
                    exc_info=True,
                )

        with self._lock:
            self._agents.clear()

        logger.info("AgentRegistry closed, all agents shut down")

    def count(self) -> int:
        """
        Get count of registered agents.

        Returns:
            Number of agent instances currently in registry.
        """
        return len(self._agents)
