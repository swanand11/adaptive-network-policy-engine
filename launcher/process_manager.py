"""Process Manager - Supervise and monitor system processes."""

import subprocess
import psutil
import logging
import time
import signal
import os
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class ProcessStatus(Enum):
    """Process status states."""
    STARTING = "starting"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    STOPPED = "stopped"
    UNKNOWN = "unknown"


@dataclass
class ProcessInfo:
    """Process information and metrics."""
    name: str
    command: str
    pid: Optional[int] = None
    status: ProcessStatus = ProcessStatus.STOPPED
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    restart_count: int = 0
    started_at: Optional[datetime] = None
    last_health_check: Optional[datetime] = None
    health_check_url: Optional[str] = None
    port: Optional[int] = None
    env: Dict[str, str] = field(default_factory=dict)
    max_restarts: int = 3
    process: Optional[subprocess.Popen] = None
    psutil_process: Optional[psutil.Process] = None


class ProcessManager:
    """Manage system processes with supervision and health monitoring."""

    def __init__(self):
        self.processes: Dict[str, ProcessInfo] = {}
        self._running = False
        logger.info("ProcessManager initialized")

    def register(
        self,
        name: str,
        command: str,
        health_check_url: Optional[str] = None,
        port: Optional[int] = None,
        env: Optional[Dict[str, str]] = None,
        max_restarts: int = 3,
    ) -> None:
        """Register a process for management.
        
        Args:
            name: Unique process name
            command: Shell command to execute
            health_check_url: Optional HTTP health check endpoint
            port: Optional port number
            env: Optional environment variables
            max_restarts: Maximum restart attempts
        """
        if name in self.processes:
            logger.warning(f"Process {name} already registered, updating configuration")
        
        self.processes[name] = ProcessInfo(
            name=name,
            command=command,
            health_check_url=health_check_url,
            port=port,
            env=env or {},
            max_restarts=max_restarts,
        )
        logger.info(f"Registered process: {name}")

    def start(self, name: str, python_exe: str = 'python3') -> bool:
        """Start a registered process.
        
        Args:
            name: Process name
            python_exe: Python executable to use (for Python processes)
            
        Returns:
            True if started successfully, False otherwise
        """
        if name not in self.processes:
            logger.error(f"Process {name} not registered")
            return False

        proc_info = self.processes[name]
        
        if proc_info.process and proc_info.process.poll() is None:
            logger.warning(f"Process {name} already running (PID: {proc_info.pid})")
            return True

        try:
            logger.info(f"Starting process: {name} - {proc_info.command}")
            proc_info.status = ProcessStatus.STARTING
            
            # Prepare environment
            import os
            env = os.environ.copy()
            env.update(proc_info.env)
            
            # Add project root to PYTHONPATH for module imports
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if "PYTHONPATH" in env:
                env["PYTHONPATH"] = f"{project_root}:{env['PYTHONPATH']}"
            else:
                env["PYTHONPATH"] = project_root
            
            logger.debug(f"PYTHONPATH: {env['PYTHONPATH']}")
            
            # Replace 'python3' in command with the actual python executable
            command = proc_info.command.replace('python3', python_exe)
            
            # Start process
            process = subprocess.Popen(
                command,
                shell=True,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid if hasattr(os, 'setsid') else None
            )
            
            proc_info.process = process
            proc_info.pid = process.pid
            proc_info.started_at = datetime.now()
            
            # Create psutil process for monitoring
            try:
                proc_info.psutil_process = psutil.Process(process.pid)
            except psutil.NoSuchProcess:
                logger.warning(f"Could not create psutil process for {name}")
            
            # Give process time to start
            time.sleep(1)
            
            # Check if process is still running
            # IMPORTANT: Use return code, not stderr output
            # Docker and other tools write status messages to stderr even on success
            if process.poll() is None:
                # Process is still running - this is success
                proc_info.status = ProcessStatus.HEALTHY
                logger.info(f"Process {name} started successfully (PID: {process.pid})")
                return True
            else:
                # Process exited - check return code
                return_code = process.returncode
                if return_code == 0:
                    # Return code 0 = success (even if process exited)
                    proc_info.status = ProcessStatus.HEALTHY
                    logger.info(f"Process {name} started successfully (PID: {process.pid})")
                    return True
                else:
                    # Non-zero return code = failure
                    proc_info.status = ProcessStatus.FAILED
                    stdout, stderr = process.communicate()
                    logger.error(
                        f"Process {name} failed to start (return code: {return_code}):\n"
                        f"STDOUT: {stdout.decode() if stdout else 'N/A'}\n"
                        f"STDERR: {stderr.decode() if stderr else 'N/A'}"
                    )
                    return False
                
        except Exception as e:
            logger.error(f"Error starting process {name}: {e}", exc_info=True)
            proc_info.status = ProcessStatus.FAILED
            return False

    def stop(self, name: str, timeout: int = 10) -> bool:
        """Stop a running process gracefully.
        
        Args:
            name: Process name
            timeout: Timeout in seconds for graceful shutdown
            
        Returns:
            True if stopped successfully, False otherwise
        """
        if name not in self.processes:
            logger.error(f"Process {name} not registered")
            return False

        proc_info = self.processes[name]
        
        if not proc_info.process or proc_info.process.poll() is not None:
            logger.info(f"Process {name} not running")
            proc_info.status = ProcessStatus.STOPPED
            return True

        try:
            logger.info(f"Stopping process: {name} (PID: {proc_info.pid})")
            
            # Try graceful shutdown first
            import os
            if hasattr(os, 'killpg'):
                os.killpg(os.getpgid(proc_info.process.pid), signal.SIGTERM)
            else:
                proc_info.process.terminate()
            
            # Wait for process to exit
            try:
                proc_info.process.wait(timeout=timeout)
                logger.info(f"Process {name} stopped gracefully")
            except subprocess.TimeoutExpired:
                logger.warning(f"Process {name} did not stop gracefully, forcing kill")
                if hasattr(os, 'killpg'):
                    os.killpg(os.getpgid(proc_info.process.pid), signal.SIGKILL)
                else:
                    proc_info.process.kill()
                proc_info.process.wait()
            
            proc_info.status = ProcessStatus.STOPPED
            proc_info.pid = None
            proc_info.process = None
            proc_info.psutil_process = None
            
            return True
            
        except Exception as e:
            logger.error(f"Error stopping process {name}: {e}", exc_info=True)
            return False

    def restart(self, name: str) -> bool:
        """Restart a process.
        
        Args:
            name: Process name
            
        Returns:
            True if restarted successfully, False otherwise
        """
        if name not in self.processes:
            logger.error(f"Process {name} not registered")
            return False

        proc_info = self.processes[name]
        
        if proc_info.restart_count >= proc_info.max_restarts:
            logger.error(
                f"Process {name} exceeded max restarts ({proc_info.max_restarts}), "
                f"not restarting"
            )
            proc_info.status = ProcessStatus.FAILED
            return False

        logger.info(f"Restarting process: {name} (attempt {proc_info.restart_count + 1})")
        
        self.stop(name)
        time.sleep(2)  # Brief delay before restart
        
        if self.start(name):
            proc_info.restart_count += 1
            return True
        
        return False

    def get_status(self, name: str) -> Optional[ProcessInfo]:
        """Get current status of a process.
        
        Args:
            name: Process name
            
        Returns:
            ProcessInfo or None if not found
        """
        return self.processes.get(name)

    def get_all_status(self) -> List[Dict]:
        """Get status of all processes.
        
        Returns:
            List of process status dictionaries
        """
        statuses = []
        
        for name, proc_info in self.processes.items():
            # For Docker processes, perform health check
            if 'docker compose' in proc_info.command:
                self.health_check(name)
            # For regular processes, update metrics if process is running
            elif proc_info.psutil_process:
                try:
                    proc_info.cpu_percent = proc_info.psutil_process.cpu_percent(interval=0.1)
                    mem_info = proc_info.psutil_process.memory_info()
                    proc_info.memory_mb = mem_info.rss / (1024 * 1024)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    proc_info.status = ProcessStatus.UNKNOWN
            
            statuses.append({
                "name": name,
                "status": proc_info.status.value,
                "pid": proc_info.pid,
                "cpu_percent": round(proc_info.cpu_percent, 2),
                "memory_mb": round(proc_info.memory_mb, 2),
                "restart_count": proc_info.restart_count,
                "started_at": proc_info.started_at.isoformat() if proc_info.started_at else None,
                "port": proc_info.port,
            })
        
        return statuses
        return statuses

    def start_all(self, names: Optional[List[str]] = None) -> Dict[str, bool]:
        """Start multiple processes.
        
        Args:
            names: List of process names, or None for all
            
        Returns:
            Dict mapping process names to success status
        """
        if names is None:
            names = list(self.processes.keys())
        
        results = {}
        for name in names:
            results[name] = self.start(name)
            time.sleep(0.5)  # Brief delay between starts
        
        return results

    def stop_all(self, names: Optional[List[str]] = None) -> Dict[str, bool]:
        """Stop multiple processes.
        
        Args:
            names: List of process names, or None for all
            
        Returns:
            Dict mapping process names to success status
        """
        if names is None:
            names = list(self.processes.keys())
        
        results = {}
        for name in reversed(names):  # Stop in reverse order
            results[name] = self.stop(name)
        
        return results

    def health_check(self, name: str) -> bool:
        """Perform health check on a process.
        
        Args:
            name: Process name
            
        Returns:
            True if healthy, False otherwise
        """
        if name not in self.processes:
            return False

        proc_info = self.processes[name]
        
        # For Docker processes, check if the container is running
        if 'docker compose' in proc_info.command:
            try:
                import subprocess
                # For docker compose services, check if any container is running
                # Use docker compose ps to check all containers
                result = subprocess.run(
                    "sudo docker compose ps --format '{{.State}}'",
                    shell=True,
                    capture_output=True,
                    timeout=5,
                    text=True,
                    cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                )
                
                # Check return code, not stderr (Docker writes warnings to stderr)
                if result.returncode == 0:
                    # Check if we have any running containers
                    output = result.stdout.strip()
                    if output and 'running' in output.lower():
                        proc_info.status = ProcessStatus.HEALTHY
                        proc_info.last_health_check = datetime.now()
                        logger.debug(f"Docker service {name} is healthy")
                        return True
                    else:
                        proc_info.status = ProcessStatus.DEGRADED
                        logger.warning(f"Docker service {name} containers not running: {output}")
                        return False
                else:
                    # Non-zero return code = actual failure
                    proc_info.status = ProcessStatus.DEGRADED
                    logger.warning(f"Docker compose ps failed with return code {result.returncode}")
                    return False
                    
            except Exception as e:
                logger.warning(f"Docker health check failed for {name}: {e}")
                proc_info.status = ProcessStatus.DEGRADED
                return False
        
        # For regular processes, check if process is running
        if not proc_info.process or proc_info.process.poll() is not None:
            proc_info.status = ProcessStatus.FAILED
            return False
        
        # If health check URL provided, check it
        if proc_info.health_check_url:
            try:
                import requests
                response = requests.get(
                    f"http://{proc_info.health_check_url}",
                    timeout=5
                )
                if response.status_code == 200:
                    proc_info.status = ProcessStatus.HEALTHY
                    proc_info.last_health_check = datetime.now()
                    return True
                else:
                    proc_info.status = ProcessStatus.DEGRADED
                    return False
            except Exception as e:
                logger.warning(f"Health check failed for {name}: {e}")
                proc_info.status = ProcessStatus.DEGRADED
                return False
        
        # Default: process is running
        proc_info.status = ProcessStatus.HEALTHY
        proc_info.last_health_check = datetime.now()
        return True

    def monitor_loop(self, interval: int = 10) -> None:
        """Continuous monitoring loop with auto-restart.
        
        Args:
            interval: Check interval in seconds
        """
        self._running = True
        logger.info(f"Starting monitoring loop (interval: {interval}s)")
        
        while self._running:
            for name, proc_info in self.processes.items():
                if proc_info.status == ProcessStatus.STOPPED:
                    continue
                
                # Check if process died
                if proc_info.process and proc_info.process.poll() is not None:
                    logger.warning(f"Process {name} died unexpectedly")
                    proc_info.status = ProcessStatus.FAILED
                    
                    # Auto-restart if enabled
                    if proc_info.restart_count < proc_info.max_restarts:
                        logger.info(f"Auto-restarting {name}")
                        self.restart(name)
                
                # Perform health check
                self.health_check(name)
            
            time.sleep(interval)

    def shutdown(self) -> None:
        """Shutdown all processes and cleanup."""
        logger.info("Shutting down ProcessManager")
        self._running = False
        self.stop_all()
