"""
Base FHIR resource class
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..utils.field_tracker import FieldTracker
from ..utils.uuid_generator import DeterministicUUIDGenerator


@dataclass
class FHIRResource(ABC):
    """Base class for all FHIR resources with original ID preservation"""
    resource_type: str
    uuid_id: str  # Generated UUID5
    original_id: str  # From source JSON
    full_url: str
    raw_data: Dict[str, Any]
    vendor_name: str
    field_tracker: FieldTracker
    
    @abstractmethod
    def to_postgres_records(self) -> Dict[str, List[Dict[str, Any]]]:
        """Convert to PostgreSQL table records"""
        pass
    
    def get_data_lineage_info(self) -> Dict[str, Any]:
        """Return data lineage information"""
        return {
            'entity_uuid': self.uuid_id,
            'entity_type': self.resource_type,
            'original_id': self.original_id,
            'full_url': self.full_url,
            'vendor_name': self.vendor_name,
            'generation_method': 'uuid5_deterministic',
            'created_at': datetime.now().isoformat()
        }
    
    def get_field_coverage_report(self) -> Dict[str, Any]:
        """Get field coverage report for this resource"""
        return self.field_tracker.get_coverage_report()
    
    @staticmethod
    def _clean_string_value(value: Any, max_length: Optional[int] = None) -> str:
        """Clean and truncate string values for database storage"""
        if value is None:
            return ''
        
        cleaned = str(value).strip()
        
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
        
        return cleaned
    
    @staticmethod
    def _safe_bool_conversion(value: Any) -> bool:
        """Safely convert value to boolean"""
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'active')
        elif isinstance(value, (int, float)):
            return bool(value)
        else:
            return False
