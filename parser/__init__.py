"""
FHIR Cache Parser - Object-oriented FHIR data processing for PostgreSQL
"""

from .models import FHIRResource, FHIROrganization, FHIREndpoint
from .models.factories import create_fhir_resource_from_file, create_fhir_resource_from_json
from .output import PostgreSQLTableManager, CSVExporter
from .utils import DeterministicUUIDGenerator, FieldTracker, DataValidator

# Import main processor
from .processor import FHIRCacheProcessor

__version__ = "1.0.0"

__all__ = [
    'FHIRCacheProcessor',
    'FHIRResource',
    'FHIROrganization', 
    'FHIREndpoint',
    'PostgreSQLTableManager',
    'CSVExporter',
    'DeterministicUUIDGenerator',
    'FieldTracker',
    'DataValidator',
    'create_fhir_resource_from_file',
    'create_fhir_resource_from_json'
]
