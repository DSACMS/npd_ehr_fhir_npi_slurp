"""
Deterministic UUID5 generation for FHIR resources
"""
import uuid
from typing import Optional


class DeterministicUUIDGenerator:
    """Generate consistent UUID5s based on source data"""
    
    # Define namespace UUIDs for different entity types
    ORGANIZATION_NAMESPACE = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')  # Standard DNS namespace
    ENDPOINT_NAMESPACE = uuid.UUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8')      # Custom endpoint namespace
    VENDOR_NAMESPACE = uuid.UUID('6ba7b812-9dad-11d1-80b4-00c04fd430c8')        # Custom vendor namespace
    
    @staticmethod
    def generate_organization_uuid(*, full_url: str, original_id: str, vendor_name: str) -> str:
        """Generate deterministic UUID5 for organization"""
        # Create consistent seed from multiple identifiers
        seed = f"{vendor_name}::{full_url}::{original_id}"
        return str(uuid.uuid5(DeterministicUUIDGenerator.ORGANIZATION_NAMESPACE, seed))
    
    @staticmethod
    def generate_endpoint_uuid(*, full_url: str, original_id: str, vendor_name: str) -> str:
        """Generate deterministic UUID5 for endpoint"""
        seed = f"{vendor_name}::{full_url}::{original_id}"
        return str(uuid.uuid5(DeterministicUUIDGenerator.ENDPOINT_NAMESPACE, seed))
    
    @staticmethod
    def generate_vendor_uuid(*, vendor_name: str, directory_path: str) -> str:
        """Generate deterministic UUID5 for vendor"""
        seed = f"{vendor_name}::{directory_path}"
        return str(uuid.uuid5(DeterministicUUIDGenerator.VENDOR_NAMESPACE, seed))
    
    @staticmethod
    def generate_npi_issuer_uuid(*, npi_system: str) -> str:
        """Generate deterministic UUID5 for NPI issuer"""
        seed = f"npi_issuer::{npi_system}"
        return str(uuid.uuid5(DeterministicUUIDGenerator.VENDOR_NAMESPACE, seed))
