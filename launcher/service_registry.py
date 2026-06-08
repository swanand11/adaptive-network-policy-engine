"""Service Registry - Track service dependencies and startup order."""

import logging
from typing import Dict, List, Set, Optional
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class ServiceRegistry:
    """Manage service dependencies and determine startup order."""

    def __init__(self):
        self.services: Dict[str, Dict] = {}
        self.dependencies: Dict[str, List[str]] = defaultdict(list)
        logger.info("ServiceRegistry initialized")

    def register(
        self,
        name: str,
        command: str,
        depends_on: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        """Register a service with its dependencies.
        
        Args:
            name: Service name
            command: Command to execute
            depends_on: List of service names this service depends on
            **kwargs: Additional service configuration
        """
        self.services[name] = {
            "name": name,
            "command": command,
            "depends_on": depends_on or [],
            **kwargs
        }
        
        if depends_on:
            self.dependencies[name] = depends_on
        
        logger.info(f"Registered service: {name} (depends on: {depends_on or 'none'})")

    def get_startup_order(self) -> List[str]:
        """Calculate service startup order using topological sort.
        
        Returns:
            List of service names in startup order
            
        Raises:
            ValueError: If circular dependency detected
        """
        # Build adjacency list and in-degree count
        in_degree = defaultdict(int)
        adj_list = defaultdict(list)
        
        # Initialize all services with in-degree 0
        for service in self.services:
            in_degree[service] = 0
        
        # Build graph
        for service, deps in self.dependencies.items():
            for dep in deps:
                if dep not in self.services:
                    logger.warning(
                        f"Service {service} depends on {dep} which is not registered"
                    )
                    continue
                adj_list[dep].append(service)
                in_degree[service] += 1
        
        # Kahn's algorithm for topological sort
        queue = deque([s for s in self.services if in_degree[s] == 0])
        result = []
        
        while queue:
            service = queue.popleft()
            result.append(service)
            
            for dependent in adj_list[service]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # Check for circular dependencies
        if len(result) != len(self.services):
            remaining = set(self.services.keys()) - set(result)
            raise ValueError(
                f"Circular dependency detected involving services: {remaining}"
            )
        
        logger.info(f"Calculated startup order: {result}")
        return result

    def get_shutdown_order(self) -> List[str]:
        """Get service shutdown order (reverse of startup).
        
        Returns:
            List of service names in shutdown order
        """
        startup_order = self.get_startup_order()
        shutdown_order = list(reversed(startup_order))
        logger.info(f"Calculated shutdown order: {shutdown_order}")
        return shutdown_order

    def get_service(self, name: str) -> Optional[Dict]:
        """Get service configuration.
        
        Args:
            name: Service name
            
        Returns:
            Service configuration dict or None
        """
        return self.services.get(name)

    def get_all_services(self) -> Dict[str, Dict]:
        """Get all registered services.
        
        Returns:
            Dict mapping service names to configurations
        """
        return self.services.copy()

    def get_dependencies(self, name: str) -> List[str]:
        """Get direct dependencies of a service.
        
        Args:
            name: Service name
            
        Returns:
            List of dependency service names
        """
        return self.dependencies.get(name, [])

    def get_dependents(self, name: str) -> List[str]:
        """Get services that depend on this service.
        
        Args:
            name: Service name
            
        Returns:
            List of dependent service names
        """
        dependents = []
        for service, deps in self.dependencies.items():
            if name in deps:
                dependents.append(service)
        return dependents

    def validate(self) -> bool:
        """Validate service registry for consistency.
        
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check for circular dependencies
            self.get_startup_order()
            
            # Check all dependencies are registered
            for service, deps in self.dependencies.items():
                for dep in deps:
                    if dep not in self.services:
                        logger.error(
                            f"Service {service} depends on unregistered service {dep}"
                        )
                        return False
            
            logger.info("Service registry validation passed")
            return True
            
        except ValueError as e:
            logger.error(f"Service registry validation failed: {e}")
            return False
