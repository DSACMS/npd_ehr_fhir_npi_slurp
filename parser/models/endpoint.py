"""
FHIR Endpoint resource model
"""
from typing import Dict, List, Any, Optional
from datetime import datetime

from .base import FHIRResource
from ..utils.field_tracker import FieldTracker, JSONFieldWalker
from ..utils.uuid_generator import DeterministicUUIDGenerator
from ..utils.validators import DataValidator


class FHIREndpoint(FHIRResource):
    """Enhanced Endpoint class with original ID preservation"""
    
    def __init__(self, *, full_url: str, resource_data: Dict[str, Any], vendor_name: str):
        # Extract original ID
        original_id = resource_data.get('id', '')
        
        # Generate deterministic UUID5
        uuid_id = DeterministicUUIDGenerator.generate_endpoint_uuid(
            full_url=full_url,
            original_id=original_id,
            vendor_name=vendor_name
        )
        
        # Initialize field tracker
        field_tracker = FieldTracker()
        json_walker = JSONFieldWalker(field_tracker)
        
        # Walk through JSON and track all fields
        json_walker.walk_and_track(resource_data)
        
        super().__init__(
            resource_type='Endpoint',
            uuid_id=uuid_id,
            original_id=original_id,
            full_url=full_url,
            raw_data=resource_data,
            vendor_name=vendor_name,
            field_tracker=field_tracker
        )
        
        # Extract specific fields and mark them as processed
        self.address = json_walker.extract_and_track_field(resource_data, 'address', '')
        self.status = json_walker.extract_and_track_field(resource_data, 'status', '')
        self.name = json_walker.extract_and_track_field(resource_data, 'name', '')
        self.description = json_walker.extract_and_track_field(resource_data, 'description', '')
        self.connection_type = json_walker.extract_and_track_field(resource_data, 'connectionType', {})
        self.payload_types = json_walker.extract_and_track_field(resource_data, 'payloadType', [])
        self.payload_mime_types = json_walker.extract_and_track_field(resource_data, 'payloadMimeType', [])
        
        # Track nested connection type fields
        if isinstance(self.connection_type, dict):
            json_walker.walk_and_track(self.connection_type, "connectionType")
            json_walker.mark_field_processed("connectionType.system")
            json_walker.mark_field_processed("connectionType.code")
            json_walker.mark_field_processed("connectionType.display")
        
        # Track payload type fields
        for i, payload_type in enumerate(self.payload_types):
            json_walker.walk_and_track(payload_type, f"payloadType[{i}]")
            json_walker.mark_field_processed(f"payloadType[{i}].coding")
            if isinstance(payload_type, dict) and 'coding' in payload_type:
                for j, coding in enumerate(payload_type['coding']):
                    json_walker.walk_and_track(coding, f"payloadType[{i}].coding[{j}]")
                    json_walker.mark_field_processed(f"payloadType[{i}].coding[{j}].system")
                    json_walker.mark_field_processed(f"payloadType[{i}].coding[{j}].code")
        
        # Initialize validator for data processing with shared NPI validator
        self.validator = DataValidator.create_with_shared_npi_validator()
    
    def to_postgres_records(self) -> Dict[str, List[Dict[str, Any]]]:
        """Returns records for multiple PostgreSQL tables"""
        
        # Validate the endpoint URL
        url_validation = self.validator.validate_url(self.address)
        
        # Base endpoint_instance record (FHIR-focused)
        base_endpoint_record = {
            'id': self.uuid_id,  # UUID5 for referential integrity
            'original_id': self._clean_string_value(self.original_id, 200),
            'full_url': self._clean_string_value(self.full_url, 500),
            'ehr_vendor_id': None,  # Will be populated by the processor
            'address': self._clean_string_value(self.address, 200),
            'endpoint_connection_type_id': self._get_connection_type_id(),
            'name': self._clean_string_value(self.name, 200),
            'description': self._clean_string_value(self.description, 1000),
            'environment_type_id': self._determine_environment_type(),
            'status': self._clean_string_value(self.status, 20),
            'vendor_name': self._clean_string_value(self.vendor_name, 200),
            'is_valid_url': url_validation.get('is_valid', False),
            'url_validation_error': self._clean_string_value(
                url_validation.get('validation_error', ''), 200
            ),
            'created_at': datetime.now().isoformat()
        }
        
        # NPD endpoint_instance record (matches full_npd.sql schema exactly)
        npd_endpoint_record = {
            'id': self.uuid_id,
            'ehr_vendor_id': None,  # Will be populated by the processor
            'address': self._clean_string_value(self.address, 200),
            'endpoint_connection_type_id': self._get_connection_type_id(),
            'name': self._clean_string_value(self.name, 200),
            'description': self._clean_string_value(self.description, 1000),
            'environment_type_id': self._determine_environment_type()
        }
        
        return {
            'endpoint_instance': [base_endpoint_record],
            'npd_endpoint_instance': [npd_endpoint_record],
            'endpoint_instance_to_payload': self._extract_payload_records(),
            'npd_endpoint_instance_to_payload': self._extract_npd_payload_records(),
            'data_lineage': [self.get_data_lineage_info()],
        }
    
    def _get_connection_type_id(self) -> str:
        """Extract and normalize connection type ID"""
        if not isinstance(self.connection_type, dict):
            return ''
        
        # Standard FHIR connection types
        code = self.connection_type.get('code', '')
        system = self.connection_type.get('system', '')
        
        # Map to standard connection type IDs
        if code:
            return self._clean_string_value(code, 20)
        elif 'hl7-fhir-rest' in str(self.connection_type).lower():
            return 'hl7-fhir-rest'
        elif 'direct-project' in str(self.connection_type).lower():
            return 'direct-project'
        else:
            return 'unknown'
    
    def _determine_environment_type(self) -> str:
        """Determine environment type based on URL patterns"""
        address_lower = self.address.lower()
        
        # Common patterns for different environments
        if any(pattern in address_lower for pattern in ['test', 'staging', 'dev', 'sandbox', 'demo']):
            return 'test'
        elif any(pattern in address_lower for pattern in ['prod', 'api', 'live']):
            return 'production'
        else:
            # Default to production if unclear
            return 'production'
    
    def _extract_payload_records(self) -> List[Dict[str, Any]]:
        """Extract payload type records"""
        payload_records = []
        
        # Process payload types from payloadType field
        for payload_type in self.payload_types:
            if not isinstance(payload_type, dict):
                continue
            
            codings = payload_type.get('coding', [])
            for coding in codings:
                if not isinstance(coding, dict):
                    continue
                
                system = coding.get('system', '')
                code = coding.get('code', '')
                
                if code:  # Only create record if we have a code
                    payload_record = {
                        'endpoint_instance_id': self.uuid_id,
                        'endpoint_instance_original_id': self._clean_string_value(self.original_id, 200),
                        'payload_type_id': self._clean_string_value(code, 200),
                        'mime_type_id': None,  # Will be populated separately
                        'system': self._clean_string_value(system, 200),
                        'sequence': len(payload_records)  # Track order
                    }
                    payload_records.append(payload_record)
        
        return payload_records
    
    def _extract_npd_payload_records(self) -> List[Dict[str, Any]]:
        """Extract NPD payload type records (matches full_npd.sql schema)"""
        payload_records = []
        
        # Process payload types from payloadType field
        for payload_type in self.payload_types:
            if not isinstance(payload_type, dict):
                continue
            
            codings = payload_type.get('coding', [])
            for coding in codings:
                if not isinstance(coding, dict):
                    continue
                
                code = coding.get('code', '')
                
                if code:  # Only create record if we have a code
                    # NPD record with only schema-compliant columns
                    payload_record = {
                        'endpoint_instance_id': self.uuid_id,
                        'mime_type_id': None,  # Will be populated separately via MIME type processing
                        'payload_type_id': self._clean_string_value(code, 200)
                    }
                    payload_records.append(payload_record)
        
        return payload_records
    
    def get_mime_type_records(self) -> List[Dict[str, Any]]:
        """Extract MIME type records for separate processing"""
        mime_type_records = []
        
        for i, mime_type in enumerate(self.payload_mime_types):
            if not mime_type:  # Skip empty values
                continue
                
            mime_record = {
                'endpoint_instance_id': self.uuid_id,
                'mime_type': self._clean_string_value(str(mime_type), 100),
                'sequence': i
            }
            mime_type_records.append(mime_record)
        
        return mime_type_records
    
    def get_connection_type_record(self) -> Optional[Dict[str, Any]]:
        """Extract connection type record for lookup table"""
        if not isinstance(self.connection_type, dict):
            return None
        
        code = self.connection_type.get('code', '')
        display = self.connection_type.get('display', '')
        definition = self.connection_type.get('definition', '')
        
        if not code:
            return None
        
        return {
            'id': self._clean_string_value(code, 20),
            'display': self._clean_string_value(display, 20),
            'definition': self._clean_string_value(definition, 200)
        }
    
    def is_fhir_endpoint(self) -> bool:
        """Check if this is a FHIR REST endpoint"""
        connection_type = self._get_connection_type_id()
        return connection_type == 'hl7-fhir-rest'
    
    def is_secure_endpoint(self) -> bool:
        """Check if endpoint uses HTTPS"""
        return self.address.lower().startswith('https://')
    
    def get_endpoint_summary(self) -> Dict[str, Any]:
        """Get summary information about this endpoint"""
        return {
            'uuid_id': self.uuid_id,
            'original_id': self.original_id,
            'vendor_name': self.vendor_name,
            'address': self.address,
            'connection_type': self._get_connection_type_id(),
            'environment': self._determine_environment_type(),
            'is_fhir': self.is_fhir_endpoint(),
            'is_secure': self.is_secure_endpoint(),
            'status': self.status,
            'payload_types_count': len(self.payload_types),
            'mime_types_count': len(self.payload_mime_types)
        }
