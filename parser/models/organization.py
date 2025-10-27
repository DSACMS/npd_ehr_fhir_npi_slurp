"""
FHIR Organization resource model
"""
from typing import Dict, List, Any, Optional
from datetime import datetime

from .base import FHIRResource
from ..utils.field_tracker import FieldTracker, JSONFieldWalker
from ..utils.uuid_generator import DeterministicUUIDGenerator
from ..utils.validators import DataValidator


class FHIROrganization(FHIRResource):
    """Enhanced Organization class with original ID preservation"""
    
    def __init__(self, *, full_url: str, resource_data: Dict[str, Any], vendor_name: str):
        # Extract original ID
        original_id = resource_data.get('id', '')
        
        # Generate deterministic UUID5
        uuid_id = DeterministicUUIDGenerator.generate_organization_uuid(
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
            resource_type='Organization',
            uuid_id=uuid_id,
            original_id=original_id,
            full_url=full_url,
            raw_data=resource_data,
            vendor_name=vendor_name,
            field_tracker=field_tracker
        )
        
        # Extract specific fields and mark them as processed
        self.name = json_walker.extract_and_track_field(resource_data, 'name', '')
        self.active = json_walker.extract_and_track_field(resource_data, 'active', False)
        self.identifiers = json_walker.extract_and_track_field(resource_data, 'identifier', [])
        self.addresses = json_walker.extract_and_track_field(resource_data, 'address', [])
        self.telecoms = json_walker.extract_and_track_field(resource_data, 'telecom', [])
        self.endpoints = json_walker.extract_and_track_field(resource_data, 'endpoint', [])
        
        # Track nested fields
        for i, identifier in enumerate(self.identifiers):
            json_walker.walk_and_track(identifier, f"identifier[{i}]")
            json_walker.mark_field_processed(f"identifier[{i}].system")
            json_walker.mark_field_processed(f"identifier[{i}].value")
        
        for i, address in enumerate(self.addresses):
            json_walker.walk_and_track(address, f"address[{i}]")
            json_walker.mark_field_processed(f"address[{i}].line")
            json_walker.mark_field_processed(f"address[{i}].city")
            json_walker.mark_field_processed(f"address[{i}].state")
            json_walker.mark_field_processed(f"address[{i}].postalCode")
            json_walker.mark_field_processed(f"address[{i}].country")
        
        for i, telecom in enumerate(self.telecoms):
            json_walker.walk_and_track(telecom, f"telecom[{i}]")
            json_walker.mark_field_processed(f"telecom[{i}].system")
            json_walker.mark_field_processed(f"telecom[{i}].value")
            json_walker.mark_field_processed(f"telecom[{i}].use")
        
        for i, endpoint in enumerate(self.endpoints):
            json_walker.walk_and_track(endpoint, f"endpoint[{i}]")
            json_walker.mark_field_processed(f"endpoint[{i}].reference")
        
        # Initialize validator for data processing with shared NPI validator
        self.validator = DataValidator.create_with_shared_npi_validator()
    
    def to_postgres_records(self) -> Dict[str, List[Dict[str, Any]]]:
        """Returns records for multiple PostgreSQL tables"""
        
        # Base organization record (FHIR-focused)
        base_org_record = {
            'id': self.uuid_id,  # UUID5 for referential integrity
            'original_id': self._clean_string_value(self.original_id, 200),
            'full_url': self._clean_string_value(self.full_url, 500),
            'name': self._clean_string_value(self.name, 500),
            'active': self._safe_bool_conversion(self.active),
            'vendor_name': self._clean_string_value(self.vendor_name, 200),
            'created_at': datetime.now().isoformat()
        }
        
        # NPD organization record (matches full_npd.sql schema)
        # Note: FHIR Organizations don't have authorized_official_id, so we skip NPD organization records
        # or create placeholder records. For now, we skip them since we can't properly map the relationships.
        
        return {
            'organization': [base_org_record],
            'endpoint_instance_to_other_id': self._extract_npi_records(),
            'npd_endpoint_instance_to_other_id': self._extract_npd_npi_records(),
            'data_lineage': [self.get_data_lineage_info()],
        }
    
    def _extract_npi_records(self) -> List[Dict[str, Any]]:
        """Extract NPI records with original ID lineage"""
        npi_records = []
        
        for identifier in self.identifiers:
            if not isinstance(identifier, dict):
                continue
                
            system = identifier.get('system', '')
            value = identifier.get('value', '')
            
            # Check if this looks like an NPI
            if ('npi' in system.lower() or 
                self.validator._is_valid_npi_format(str(value))):
                
                # Validate the NPI
                validation_result = self.validator.validate_npi(
                    npi_value=str(value),
                    npi_system=system
                )
                
                # Generate issuer UUID
                issuer_uuid = DeterministicUUIDGenerator.generate_npi_issuer_uuid(
                    npi_system=system
                )
                
                npi_record = {
                    'endpoint_instance_id': self.uuid_id,  # Using org UUID as foreign key
                    'other_id': self._clean_string_value(value, 100),
                    'system': self._clean_string_value(system, 200),
                    'issuer_id': issuer_uuid,
                    # Additional validation info that could be stored elsewhere
                    'is_valid_format': validation_result.get('is_valid_format', False),
                    'is_valid_api': validation_result.get('is_valid_api', False),
                    'validation_error': self._clean_string_value(
                        validation_result.get('validation_error', ''), 500
                    )
                }
                
                npi_records.append(npi_record)
        
        return npi_records
    
    def _extract_npd_npi_records(self) -> List[Dict[str, Any]]:
        """Extract NPI records that match NPD schema (no validation columns)"""
        npi_records = []
        
        for identifier in self.identifiers:
            if not isinstance(identifier, dict):
                continue
                
            system = identifier.get('system', '')
            value = identifier.get('value', '')
            
            # Check if this looks like an NPI
            if ('npi' in system.lower() or 
                self.validator._is_valid_npi_format(str(value))):
                
                # Generate issuer UUID
                issuer_uuid = DeterministicUUIDGenerator.generate_npi_issuer_uuid(
                    npi_system=system
                )
                
                # NPD record with only schema-compliant columns
                npi_record = {
                    'endpoint_instance_id': self.uuid_id,  # Using org UUID as foreign key
                    'other_id': self._clean_string_value(value, 100),
                    'system': self._clean_string_value(system, 200),
                    'issuer_id': issuer_uuid
                }
                
                npi_records.append(npi_record)
        
        return npi_records
    
    def get_address_records(self) -> List[Dict[str, Any]]:
        """Extract address records for separate processing"""
        address_records = []
        
        for i, address in enumerate(self.addresses):
            if not isinstance(address, dict):
                continue
            
            # Extract address lines
            lines = address.get('line', [])
            address_line1 = lines[0] if len(lines) > 0 else ''
            address_line2 = lines[1] if len(lines) > 1 else ''
            
            address_record = {
                'organization_id': self.uuid_id,
                'address_type': self._clean_string_value(address.get('type', ''), 50),
                'text': self._clean_string_value(address.get('text', ''), 500),
                'address_line1': self._clean_string_value(address_line1, 200),
                'address_line2': self._clean_string_value(address_line2, 200),
                'city': self._clean_string_value(address.get('city', ''), 100),
                'state': self._clean_string_value(address.get('state', ''), 50),
                'postal_code': self._clean_string_value(address.get('postalCode', ''), 20),
                'country': self._clean_string_value(address.get('country', ''), 50),
                'use': self._clean_string_value(address.get('use', ''), 20),
                'sequence': i  # To maintain order
            }
            
            address_records.append(address_record)
        
        return address_records
    
    def get_telecom_records(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract telecom records split by type"""
        phone_records = []
        email_records = []
        url_records = []
        
        for i, telecom in enumerate(self.telecoms):
            if not isinstance(telecom, dict):
                continue
                
            system = telecom.get('system', '').lower()
            value = telecom.get('value', '')
            use = telecom.get('use', '')
            
            if system == 'phone':
                # Normalize phone number
                phone_validation = self.validator.normalize_phone_number(value)
                
                phone_record = {
                    'organization_id': self.uuid_id,
                    'original_value': self._clean_string_value(phone_validation.get('original_value', ''), 50),
                    'normalized_number': self._clean_string_value(phone_validation.get('normalized_number', ''), 50),
                    'extension': self._clean_string_value(phone_validation.get('extension', ''), 20),
                    'country_code': self._clean_string_value(phone_validation.get('country_code', ''), 10),
                    'is_valid': phone_validation.get('is_valid', False),
                    'parse_error': self._clean_string_value(phone_validation.get('parse_error', ''), 200),
                    'use': self._clean_string_value(use, 20),
                    'sequence': i
                }
                phone_records.append(phone_record)
                
            elif system == 'email':
                email_validation = self.validator.validate_email(value)
                
                email_record = {
                    'organization_id': self.uuid_id,
                    'email_value': self._clean_string_value(value, 200),
                    'use': self._clean_string_value(use, 20),
                    'is_valid': email_validation.get('is_valid', False),
                    'validation_error': self._clean_string_value(email_validation.get('validation_error', ''), 200),
                    'sequence': i
                }
                email_records.append(email_record)
                
            elif system == 'url':
                url_validation = self.validator.validate_url(value)
                
                url_record = {
                    'organization_id': self.uuid_id,
                    'url_value': self._clean_string_value(value, 500),
                    'use': self._clean_string_value(use, 20),
                    'is_valid': url_validation.get('is_valid', False),
                    'validation_error': self._clean_string_value(url_validation.get('validation_error', ''), 200),
                    'sequence': i
                }
                url_records.append(url_record)
        
        return {
            'phones': phone_records,
            'emails': email_records,
            'urls': url_records
        }
    
    def get_endpoint_references(self) -> List[Dict[str, Any]]:
        """Extract endpoint reference records"""
        endpoint_refs = []
        
        for i, endpoint_ref in enumerate(self.endpoints):
            if not isinstance(endpoint_ref, dict):
                continue
                
            reference = endpoint_ref.get('reference', '')
            
            endpoint_record = {
                'organization_id': self.uuid_id,
                'endpoint_reference': self._clean_string_value(reference, 500),
                'sequence': i
            }
            endpoint_refs.append(endpoint_record)
        
        return endpoint_refs
