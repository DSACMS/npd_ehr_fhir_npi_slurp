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
        
        # Extract contact data
        address_records = self.get_address_records()
        telecom_records = self.get_telecom_records()
        
        # Extract NPD contact data
        npd_contact_data = self._extract_npd_contact_data()
        
        return {
            'organization': [base_org_record],
            'endpoint_instance_to_other_id': self._extract_npi_records(),
            'npd_endpoint_instance_to_other_id': self._extract_npd_npi_records(),
            'data_lineage': [self.get_data_lineage_info()],
            
            # Native FHIR contact tables
            'fhir_organization_address': self._format_fhir_address_records(address_records),
            'fhir_organization_phone': self._format_fhir_phone_records(telecom_records['phones']),
            'fhir_organization_email': self._format_fhir_email_records(telecom_records['emails']),
            'fhir_organization_contact_url': self._format_fhir_url_records(telecom_records['urls']),
            
            # NPD contact tables
            'npd_organization_to_address': npd_contact_data['addresses'],
            'npd_organization_to_phone': npd_contact_data['phones'],
            'npd_address': npd_contact_data['address_entities'],
            'npd_address_us': npd_contact_data['address_us'],
            'npd_address_international': npd_contact_data['address_international'],
            'npd_address_nonstandard': npd_contact_data['address_nonstandard'],
            'npd_fhir_address_use': npd_contact_data['address_use_codes'],
            'npd_fhir_phone_use': npd_contact_data['phone_use_codes'],
            'npd_fhir_email_use': npd_contact_data['email_use_codes']
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
                    'endpoint_instance_original_id': self._clean_string_value(self.original_id, 200),
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
    
    def _format_fhir_address_records(self, address_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format address records for FHIR table with timestamps and original IDs"""
        formatted_records = []
        
        for addr in address_records:
            formatted_record = addr.copy()
            formatted_record['organization_original_id'] = self._clean_string_value(self.original_id, 200)
            formatted_record['created_at'] = datetime.now().isoformat()
            formatted_records.append(formatted_record)
        
        return formatted_records
    
    def _extract_npd_contact_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract contact data in NPD format"""
        import uuid
        
        npd_data = {
            'addresses': [],
            'phones': [],
            'address_entities': [],
            'address_us': [],
            'address_international': [],
            'address_nonstandard': [],
            'address_use_codes': [],
            'phone_use_codes': [],
            'email_use_codes': []
        }
        
        # Process addresses
        for i, address in enumerate(self.addresses):
            if not isinstance(address, dict):
                continue
                
            # Generate address UUID
            address_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{self.uuid_id}:address:{i}"))
            
            # Extract address components
            lines = address.get('line', [])
            address_line1 = lines[0] if len(lines) > 0 else ''
            address_line2 = lines[1] if len(lines) > 1 else ''
            city = address.get('city', '')
            state = address.get('state', '')
            postal_code = address.get('postalCode', '')
            country = address.get('country', '')
            use = address.get('use', '')
            
            # Determine address type - default to US if no country or country is US
            # Handle case where country might be a list or other type
            country_str = ''
            if isinstance(country, list) and country:
                country_str = str(country[0])
            elif isinstance(country, str):
                country_str = country
            else:
                country_str = str(country) if country else ''
            
            is_us_address = not country_str or country_str.upper() in ['US', 'USA', 'UNITED STATES']
            
            if is_us_address and city and state:
                # US Address
                us_address_id = f"US_{address_uuid[:10]}"
                
                us_address_record = {
                    'id': us_address_id,
                    'addressee': '',
                    'delivery_line_1': self._clean_string_value(address_line1, 64),
                    'delivery_line_2': self._clean_string_value(address_line2, 64),
                    'last_line': '',
                    'city_name': self._clean_string_value(city, 64),
                    'state_code': self._clean_string_value(state, 2),
                    'zipcode': self._clean_string_value(postal_code[:5], 5) if postal_code else '',
                    'plus4_code': self._clean_string_value(postal_code[6:10], 4) if len(postal_code) > 5 else '',
                    'latitude': None,
                    'longitude': None
                }
                npd_data['address_us'].append(us_address_record)
                
                # Address entity
                address_entity = {
                    'id': address_uuid,
                    'address_us_id': us_address_id,
                    'address_international_id': None,
                    'address_nonstandard_id': None
                }
                npd_data['address_entities'].append(address_entity)
                
            elif country and not is_us_address:
                # International Address
                intl_address_id = f"INTL_{address_uuid[:10]}"
                
                intl_address_record = {
                    'id': intl_address_id,
                    'country_code': self._clean_string_value(country_str[:2].upper(), 2),
                    'address1': self._clean_string_value(address_line1, 64),
                    'address2': self._clean_string_value(address_line2, 64),
                    'address3': '',
                    'address4': '',
                    'locality': self._clean_string_value(city, 64),
                    'administrative_area': self._clean_string_value(state, 32),
                    'postal_code': self._clean_string_value(postal_code, 16),
                    'latitude': None,
                    'longitude': None
                }
                npd_data['address_international'].append(intl_address_record)
                
                # Address entity
                address_entity = {
                    'id': address_uuid,
                    'address_us_id': None,
                    'address_international_id': intl_address_id,
                    'address_nonstandard_id': None
                }
                npd_data['address_entities'].append(address_entity)
                
            else:
                # Nonstandard Address
                nonstandard_address_id = f"NS_{address_uuid[:10]}"
                
                # Combine address lines for raw address
                raw_address_parts = [address_line1, address_line2, city, state, postal_code, country]
                raw_address = ', '.join([part for part in raw_address_parts if part])
                
                nonstandard_address_record = {
                    'id': nonstandard_address_id,
                    'addressee': '',
                    'delivery_line_1': self._clean_string_value(address_line1, 64),
                    'delivery_line_2': self._clean_string_value(address_line2, 64),
                    'last_line': self._clean_string_value(f"{city}, {state} {postal_code}", 64),
                    'address_type': 'fhir_organization',
                    'raw_address': self._clean_string_value(raw_address, 1000),
                    'latitude': None,
                    'longitude': None
                }
                npd_data['address_nonstandard'].append(nonstandard_address_record)
                
                # Address entity
                address_entity = {
                    'id': address_uuid,
                    'address_us_id': None,
                    'address_international_id': None,
                    'address_nonstandard_id': nonstandard_address_id
                }
                npd_data['address_entities'].append(address_entity)
            
            # Organization to address relationship
            if use:
                # Add use code to lookup table
                use_code_record = {
                    'id': len(npd_data['address_use_codes']) + 1,
                    'value': self._clean_string_value(use, 20)
                }
                # Check for duplicates
                if not any(code['value'] == use_code_record['value'] for code in npd_data['address_use_codes']):
                    npd_data['address_use_codes'].append(use_code_record)
                
                # Get use code ID
                use_id = next((code['id'] for code in npd_data['address_use_codes'] if code['value'] == use_code_record['value']), 1)
            else:
                use_id = 1  # Default use code
            
            org_to_address = {
                'organization_id': self.uuid_id,
                'address_id': address_uuid,
                'address_use_id': use_id
            }
            npd_data['addresses'].append(org_to_address)
        
        # Process phones
        phone_id_counter = 1
        for i, telecom in enumerate(self.telecoms):
            if not isinstance(telecom, dict):
                continue
                
            system = telecom.get('system', '').lower()
            if system != 'phone':
                continue
                
            value = telecom.get('value', '')
            use = telecom.get('use', '')
            
            # Normalize phone number
            phone_validation = self.validator.normalize_phone_number(value)
            normalized_number = phone_validation.get('normalized_number', value)
            extension = phone_validation.get('extension', '')
            
            # Add use code to lookup table
            if use:
                use_code_record = {
                    'id': len(npd_data['phone_use_codes']) + 1,
                    'value': self._clean_string_value(use, 20)
                }
                # Check for duplicates
                if not any(code['value'] == use_code_record['value'] for code in npd_data['phone_use_codes']):
                    npd_data['phone_use_codes'].append(use_code_record)
                
                # Get use code ID
                use_id = next((code['id'] for code in npd_data['phone_use_codes'] if code['value'] == use_code_record['value']), 1)
            else:
                use_id = 1  # Default use code
            
            # Generate phone UUID
            phone_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{self.uuid_id}:phone:{i}"))
            
            org_to_phone = {
                'organization_id': self.uuid_id,
                'phone_number': self._clean_string_value(normalized_number, 20),
                'extension': self._clean_string_value(extension, 10) if extension else None,
                'phone_use_id': use_id,
                'id': phone_uuid
            }
            npd_data['phones'].append(org_to_phone)
            phone_id_counter += 1
        
        # Process emails for lookup codes (NPD doesn't have organization_to_email in schema)
        for i, telecom in enumerate(self.telecoms):
            if not isinstance(telecom, dict):
                continue
                
            system = telecom.get('system', '').lower()
            if system != 'email':
                continue
                
            use = telecom.get('use', '')
            
            # Add use code to lookup table
            if use:
                use_code_record = {
                    'id': len(npd_data['email_use_codes']) + 1,
                    'value': self._clean_string_value(use, 20)
                }
                # Check for duplicates
                if not any(code['value'] == use_code_record['value'] for code in npd_data['email_use_codes']):
                    npd_data['email_use_codes'].append(use_code_record)
        
        return npd_data
    
    def _format_fhir_phone_records(self, phone_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format phone records for FHIR table with timestamps and original IDs"""
        formatted_records = []
        
        for phone in phone_records:
            formatted_record = phone.copy()
            formatted_record['organization_original_id'] = self._clean_string_value(self.original_id, 200)
            formatted_record['created_at'] = datetime.now().isoformat()
            formatted_records.append(formatted_record)
        
        return formatted_records
    
    def _format_fhir_email_records(self, email_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format email records for FHIR table with timestamps and original IDs"""
        formatted_records = []
        
        for email in email_records:
            formatted_record = email.copy()
            formatted_record['organization_original_id'] = self._clean_string_value(self.original_id, 200)
            formatted_record['created_at'] = datetime.now().isoformat()
            formatted_records.append(formatted_record)
        
        return formatted_records
    
    def _format_fhir_url_records(self, url_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format URL records for FHIR table with timestamps and original IDs"""
        formatted_records = []
        
        for url in url_records:
            formatted_record = url.copy()
            formatted_record['organization_original_id'] = self._clean_string_value(self.original_id, 200)
            formatted_record['created_at'] = datetime.now().isoformat()
            formatted_records.append(formatted_record)
        
        return formatted_records
