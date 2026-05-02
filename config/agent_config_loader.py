"""
Agent Configuration Loader

Loads agent specifications from YAML config file and provides
validated configuration for runner initialization.
"""

import logging
import os
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class AgentConfigLoader:
    """
    Loads and validates agent configuration from YAML files.

    Provides methods to:
    - Load agents.yml configuration
    - Validate schema
    - Apply environment variable overrides
    - Get normalized config for runner
    """

    # Default agent configuration values
    DEFAULT_AGENT_CONFIG = {
        "alpha": 0.3,
        "latency_threshold": 200.0,
        "error_threshold": 0.05,
        "cpu_threshold": 0.8,
        "window_size": 5,
    }

    DEFAULT_QUEUE_CONFIG = {
        "max_size": 1000,
        "timeout_seconds": 30,
    }

    DEFAULT_CONSUMER_CONFIG = {
        "session_timeout_ms": 30000,
        "heartbeat_interval_ms": 10000,
    }

    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """
        Load agent configuration from YAML file.

        Args:
            config_path: Path to agents.yml file. Can be absolute or relative.

        Returns:
            Validated config dict with keys:
            - group_id: Consumer group ID
            - consumer_config: Consumer settings
            - queue_config: Queue settings
            - agents: List of agent specs

        Raises:
            FileNotFoundError: If config file not found
            yaml.YAMLError: If YAML parsing fails
            ValueError: If validation fails
        """
        # Expand environment variables and resolve relative paths
        expanded_path = os.path.expandvars(config_path)
        abs_path = os.path.abspath(expanded_path)

        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Config file not found: {abs_path}")

        logger.info(f"Loading agent config from: {abs_path}")

        try:
            with open(abs_path, "r") as f:
                raw_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML: {e}")

        if not raw_config:
            raise ValueError("Config file is empty or invalid")

        # Validate and normalize
        config = AgentConfigLoader._validate_config(raw_config)
        logger.info(f"Loaded config with {len(config['agents'])} agents")

        return config

    @staticmethod
    def _validate_config(raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and normalize configuration.

        Args:
            raw_config: Raw parsed config dict

        Returns:
            Normalized config dict with all required fields

        Raises:
            ValueError: If validation fails
        """
        # Extract top-level keys with defaults
        group_id = raw_config.get("group_id", "service_agent_parallel")
        if not group_id:
            raise ValueError("group_id cannot be empty")

        # Get consumer config with defaults merged
        consumer_config = raw_config.get("consumer_config", {})
        consumer_config = {
            **AgentConfigLoader.DEFAULT_CONSUMER_CONFIG,
            **consumer_config,
        }

        # Get queue config with defaults merged
        queue_config = raw_config.get("queue_config", {})
        queue_config = {
            **AgentConfigLoader.DEFAULT_QUEUE_CONFIG,
            **queue_config,
        }

        # Validate queue config
        if queue_config["max_size"] < 10:
            raise ValueError("queue_config.max_size must be >= 10")
        if queue_config["timeout_seconds"] < 1:
            raise ValueError("queue_config.timeout_seconds must be >= 1")

        # Parse agents
        agents_list = raw_config.get("agents", [])
        if not isinstance(agents_list, list):
            raise ValueError("agents must be a list")
        if not agents_list:
            raise ValueError("agents list cannot be empty")

        agents = []
        seen = set()

        for i, agent_spec in enumerate(agents_list):
            if not isinstance(agent_spec, dict):
                raise ValueError(f"agents[{i}] must be a dict")

            service_id = agent_spec.get("service_id")
            cloud = agent_spec.get("cloud")

            if not service_id or not cloud:
                raise ValueError(f"agents[{i}] must have service_id and cloud")

            # Check for duplicates
            key = (service_id, cloud)
            if key in seen:
                raise ValueError(f"Duplicate agent: ({service_id}, {cloud})")
            seen.add(key)

            # Build normalized agent config
            normalized_agent = {
                "service_id": service_id,
                "cloud": cloud,
            }

            # Add optional agent parameters with defaults
            for param, default_value in AgentConfigLoader.DEFAULT_AGENT_CONFIG.items():
                normalized_agent[param] = agent_spec.get(param, default_value)

            agents.append(normalized_agent)

        return {
            "group_id": group_id,
            "consumer_config": consumer_config,
            "queue_config": queue_config,
            "agents": agents,
        }

    @staticmethod
    def get_from_env_or_default(config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load config from environment variable or default path.

        Checks environment variable AGENTS_CONFIG_PATH first,
        falls back to config/agents.yml if not set.

        Args:
            config_path: Optional override path. If provided, uses this.

        Returns:
            Loaded config dict

        Raises:
            FileNotFoundError: If no config found
        """
        if config_path:
            return AgentConfigLoader.load_config(config_path)

        # Check environment variable
        env_path = os.getenv("AGENTS_CONFIG_PATH")
        if env_path:
            return AgentConfigLoader.load_config(env_path)

        # Try default path
        default_path = "config/agents.yml"
        if os.path.exists(default_path):
            return AgentConfigLoader.load_config(default_path)

        raise FileNotFoundError(
            "No agent config found. Set AGENTS_CONFIG_PATH or place config/agents.yml in project root"
        )
