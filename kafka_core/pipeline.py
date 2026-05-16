"""Shared pipeline topology and partition helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from .enums import CloudProvider


PIPELINE_TOPICS = ("metrics.events", "service.state", "topo.decisions")


@dataclass(frozen=True)
class PipelineService:
    name: str
    service_id: str
    cloud: CloudProvider
    partition: int
    metrics_url: str

    @property
    def key(self) -> str:
        return f"{self.service_id}@{self.cloud.value}"


SERVICES = (
    PipelineService(
        name="aws",
        service_id="service-cache-aws",
        cloud=CloudProvider.AWS,
        partition=0,
        metrics_url="http://127.0.0.1:8001/metrics",
    ),
    PipelineService(
        name="aks",
        service_id="service-cache-aks",
        cloud=CloudProvider.AZURE,
        partition=1,
        metrics_url="http://127.0.0.1:8002/metrics",
    ),
    PipelineService(
        name="do",
        service_id="service-cache-droplet",
        cloud=CloudProvider.DIGITALOCEAN,
        partition=2,
        metrics_url="http://127.0.0.1:8003/metrics",
    ),
)

SERVICE_BY_NAME: Dict[str, PipelineService] = {svc.name: svc for svc in SERVICES}
SERVICE_BY_ID: Dict[str, PipelineService] = {svc.service_id: svc for svc in SERVICES}
CLOUD_TO_PARTITION: Dict[str, int] = {svc.cloud.value: svc.partition for svc in SERVICES}
NAME_TO_PARTITION: Dict[str, int] = {svc.name: svc.partition for svc in SERVICES}


def service_partition(service_id: str, cloud: Optional[str] = None) -> int:
    """Return the canonical partition for a pipeline service."""
    if service_id in SERVICE_BY_ID:
        return SERVICE_BY_ID[service_id].partition
    if service_id in NAME_TO_PARTITION:
        return NAME_TO_PARTITION[service_id]
    if cloud and cloud in CLOUD_TO_PARTITION:
        return CLOUD_TO_PARTITION[cloud]
    if cloud and cloud in NAME_TO_PARTITION:
        return NAME_TO_PARTITION[cloud]
    raise KeyError(f"No pipeline partition registered for service_id={service_id!r}")


def service_key(service_id: str, cloud: str) -> str:
    """Return the stable key used at every pipeline hop."""
    return f"{service_id}@{cloud}"


def services_as_endpoint_dicts() -> Dict[str, Dict]:
    """Return adapter-compatible endpoint configuration."""
    return {
        svc.name: {
            "url": svc.metrics_url,
            "cloud": svc.cloud,
            "service_id": svc.service_id,
            "partition": svc.partition,
        }
        for svc in SERVICES
    }


def iter_services() -> Iterable[PipelineService]:
    return iter(SERVICES)
