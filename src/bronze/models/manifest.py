from dataclasses import dataclass
from typing import Dict, List
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.models.manifest import ManifestBase


@dataclass
class BronzeManifestSourceConfigPayload:
    domain_source: DomainSource
    domain_source_type: DomainSourceType
    dataset_name: str
    request_payload: Dict


@dataclass
class BronzeManifestSource:
    source_config_payload: BronzeManifestSourceConfigPayload


@dataclass
class BronzeManifest(ManifestBase):
    sources: List[BronzeManifestSource]