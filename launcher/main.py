"""Main Launcher - Multi-Agent Multi-Cloud Network Policy Orchestrator

Single command to start the entire system:
    python launcher/main.py

Features:
- Dependency-aware startup
- Process supervision with auto-restart
- Real-time monitoring via WebSocket
- Health checks
- Graceful shutdown
"""

import asyncio
import logging
import signal
import sys
import time
import yaml
import subprocess
import os
from pathlib import Path
from typing import Dict, Any

from .process_manager import ProcessManager
from .service_registry import ServiceRegistry
from .websocket_server import WebSocketServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('launcher.log')
    ]
)
logger = logging.getLogger(__name__)


def get_venv_python() -> str:
    """Get the python executable from the virtual environment.
    
    Returns:
        Path to python executable in venv, or 'python3' if not in venv
    """
    # Check if we're in a virtual environment
    venv_path = os.getenv('VIRTUAL_ENV')
    if venv_path:
        python_exe = os.path.join(venv_path, 'bin', 'python3')
        if os.path.exists(python_exe):
            logger.info(f"Using venv python: {python_exe}")
            return python_exe
    
    # Fallback to system python3
    logger.info("Using system python3")
    return 'python3'


def cleanup_ports(ports: list) -> None:
    """Kill processes using specified ports.
    
    Args:
        ports: List of port numbers to clean up
    """
    for port in ports:
        try:
            logger.info(f"Cleaning up port {port}...")
            # Use lsof to find process using port and kill it
            cmd = f"sudo lsof -ti:{port} | xargs -r sudo kill -9"
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=5)
            if result.returncode == 0:
                logger.info(f"✓ Cleaned up port {port}")
            else:
                logger.debug(f"Port {port} was not in use")
        except Exception as e:
            logger.warning(f"Could not clean up port {port}: {e}")


class Launcher:
    """Main orchestration controller."""

    def __init__(self, config_path: str = "launcher/config.yaml"):
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self.process_manager = ProcessManager()
        self.service_registry = ServiceRegistry()
        self.websocket_server = WebSocketServer()
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        logger.info("=" * 80)
        logger.info("Multi-Agent Multi-Cloud Network Policy Orchestrator")
        logger.info("=" * 80)

    def load_config(self) -> None:
        """Load configuration from YAML file."""
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        
        logger.info(f"Loaded configuration from {self.config_path}")
        logger.info(f"System: {self.config['system']['name']} v{self.config['system']['version']}")

    def register_services(self) -> None:
        """Register all services with the service registry."""
        logger.info("Registering services...")
        
        # Infrastructure services
        if self.config.get('infrastructure'):
            for name, cfg in self.config['infrastructure'].items():
                if cfg.get('enabled', True):
                    self.service_registry.register(
                        name=name,
                        command=cfg.get('command', ''),
                        health_check_url=cfg.get('health_check'),
                        port=cfg.get('port'),
                        depends_on=[]
                    )
        
        # Simulators
        if self.config.get('simulators'):
            for name, cfg in self.config['simulators'].items():
                if cfg.get('enabled', True):
                    self.service_registry.register(
                        name=f"simulator-{name}",
                        command=cfg['command'],
                        health_check_url=cfg.get('health_check'),
                        port=cfg.get('port'),
                        depends_on=['kafka', 'prometheus']
                    )
        
        # Agents
        if self.config.get('agents'):
            for agent_type, cfg in self.config['agents'].items():
                if not cfg.get('enabled', True):
                    continue
                
                if 'instances' in cfg:
                    # Multiple instances
                    for instance in cfg['instances']:
                        self.service_registry.register(
                            name=instance['name'],
                            command=instance['command'],
                            env=instance.get('env', {}),
                            depends_on=cfg.get('depends_on', [])
                        )
                else:
                    # Single instance
                    self.service_registry.register(
                        name=agent_type,
                        command=cfg['command'],
                        depends_on=cfg.get('depends_on', [])
                    )
        
        # Consumers
        if self.config.get('consumers'):
            for name, cfg in self.config['consumers'].items():
                if cfg.get('enabled', True):
                    self.service_registry.register(
                        name=name,
                        command=cfg['command'],
                        depends_on=cfg.get('depends_on', [])
                    )
        
        # HITL
        if self.config.get('hitl', {}).get('enabled'):
            cfg = self.config['hitl']
            self.service_registry.register(
                name='hitl',
                command=cfg['command'],
                health_check_url=cfg.get('health_check'),
                port=cfg.get('port'),
                depends_on=cfg.get('depends_on', [])
            )
        
        # Traffic generator
        if self.config.get('traffic', {}).get('generator', {}).get('enabled'):
            cfg = self.config['traffic']['generator']
            self.service_registry.register(
                name='traffic-generator',
                command=cfg['command'],
                depends_on=cfg.get('depends_on', [])
            )
        
        # Backend
        if self.config.get('backend', {}).get('api', {}).get('enabled'):
            cfg = self.config['backend']['api']
            self.service_registry.register(
                name='backend-api',
                command=cfg['command'],
                health_check_url=cfg.get('health_check'),
                port=cfg.get('port'),
                depends_on=cfg.get('depends_on', [])
            )
        
        # Frontend
        if self.config.get('frontend', {}).get('enabled'):
            cfg = self.config['frontend']
            self.service_registry.register(
                name='frontend',
                command=cfg['command'],
                health_check_url=cfg.get('health_check'),
                port=cfg.get('port'),
                depends_on=cfg.get('depends_on', [])
            )
        
        # Validate registry
        if not self.service_registry.validate():
            raise ValueError("Service registry validation failed")
        
        logger.info(f"Registered {len(self.service_registry.services)} services")

    def register_processes(self) -> None:
        """Register services with process manager."""
        logger.info("Registering processes with process manager...")
        
        for service_name, service_cfg in self.service_registry.get_all_services().items():
            self.process_manager.register(
                name=service_name,
                command=service_cfg['command'],
                health_check_url=service_cfg.get('health_check_url'),
                port=service_cfg.get('port'),
                env=service_cfg.get('env', {}),
                max_restarts=service_cfg.get('max_restarts', 3)
            )
        
        logger.info(f"Registered {len(self.process_manager.processes)} processes")

    async def start_services(self) -> None:
        """Start all services in dependency order."""
        logger.info("Starting services...")
        
        # Get the python executable from venv
        python_exe = get_venv_python()
        
        startup_order = self.service_registry.get_startup_order()
        logger.info(f"Startup order: {startup_order}")
        
        failed_services = []
        
        for service_name in startup_order:
            service_cfg = self.service_registry.get_service(service_name)
            
            # Check if dependencies failed
            deps = service_cfg.get('depends_on', [])
            failed_deps = [dep for dep in deps if dep in failed_services]
            if failed_deps:
                logger.error(
                    f"Skipping {service_name}: dependencies failed: {failed_deps}"
                )
                failed_services.append(service_name)
                continue
            
            logger.info(f"Starting {service_name}...")
            success = self.process_manager.start(service_name, python_exe=python_exe)
            
            if success:
                logger.info(f"✓ {service_name} started")
                
                # Wait for startup delay if configured
                if 'startup_delay' in service_cfg:
                    delay = service_cfg['startup_delay']
                    logger.info(f"  Waiting {delay}s for {service_name} to initialize...")
                    await asyncio.sleep(delay)
            else:
                logger.error(f"✗ Failed to start {service_name}")
                failed_services.append(service_name)
                
                # Decide whether to continue or abort
                if service_cfg.get('critical', False):
                    logger.error("Critical service failed, aborting startup")
                    raise RuntimeError(f"Failed to start critical service: {service_name}")
        
        if failed_services:
            logger.warning(f"Some services failed to start: {failed_services}")
        
        logger.info("=" * 80)
        logger.info("Service startup phase complete!")
        logger.info("=" * 80)

    async def monitor_services(self) -> None:
        """Monitor services and broadcast status updates."""
        logger.info("Starting service monitoring...")
        
        while self._running:
            try:
                # Get status of all processes
                statuses = self.process_manager.get_all_status()
                
                # Broadcast to WebSocket clients
                await self.websocket_server.broadcast("health", statuses)
                
                # Check for failed processes
                for status in statuses:
                    if status['status'] == 'failed':
                        logger.warning(f"Service {status['name']} is in failed state")
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def start(self) -> None:
        """Start the entire system."""
        try:
            # Load configuration
            self.load_config()
            
            # Clean up ports that might be in use
            logger.info("Cleaning up potentially used ports...")
            cleanup_ports([8001, 8002, 8003, 8080, 8765])
            await asyncio.sleep(2)
            
            # Register services and processes
            self.register_services()
            self.register_processes()
            
            # Start WebSocket server
            await self.websocket_server.start()
            
            # Start all services
            await self.start_services()
            
            # Mark as running
            self._running = True
            
            # Start monitoring
            monitor_task = asyncio.create_task(self.monitor_services())
            
            # Display status
            self.display_status()
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
            # Cancel monitoring
            monitor_task.cancel()
            
        except Exception as e:
            logger.error(f"Error during startup: {e}", exc_info=True)
            raise

    async def stop(self) -> None:
        """Stop the entire system gracefully."""
        logger.info("=" * 80)
        logger.info("Shutting down system...")
        logger.info("=" * 80)
        
        self._running = False
        
        # Stop services in reverse order
        shutdown_order = self.service_registry.get_shutdown_order()
        logger.info(f"Shutdown order: {shutdown_order}")
        
        for service_name in shutdown_order:
            logger.info(f"Stopping {service_name}...")
            self.process_manager.stop(service_name)
        
        # Stop WebSocket server
        await self.websocket_server.stop()
        
        logger.info("=" * 80)
        logger.info("System shutdown complete")
        logger.info("=" * 80)

    def display_status(self) -> None:
        """Display current system status."""
        logger.info("=" * 80)
        logger.info("SYSTEM STATUS")
        logger.info("=" * 80)
        
        statuses = self.process_manager.get_all_status()
        
        for status in statuses:
            status_icon = "✓" if status['status'] == 'healthy' else "✗"
            pid_str = str(status['pid']) if status['pid'] else "N/A"
            logger.info(
                f"{status_icon} {status['name']:30s} | "
                f"Status: {status['status']:10s} | "
                f"PID: {pid_str:>8s} | "
                f"CPU: {status['cpu_percent']:5.1f}% | "
                f"MEM: {status['memory_mb']:6.1f}MB"
            )
        
        logger.info("=" * 80)
        logger.info("WebSocket Server: ws://localhost:8765")
        logger.info("Dashboard: http://localhost:5173")
        logger.info("Backend API: http://localhost:8000")
        logger.info("HITL Portal: http://localhost:8080")
        logger.info("=" * 80)

    def signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self._shutdown_event.set()


async def main():
    """Main entry point."""
    launcher = Launcher()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, launcher.signal_handler)
    signal.signal(signal.SIGTERM, launcher.signal_handler)
    
    try:
        await launcher.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        await launcher.stop()


if __name__ == "__main__":
    asyncio.run(main())
