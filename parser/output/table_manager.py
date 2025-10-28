"""
PostgreSQL table management using pandas DataFrames
"""
import pandas as pd
from typing import Dict, List, Any, Optional
from pathlib import Path
import csv
from datetime import datetime

from ..utils.field_tracker import FieldTracker


class PostgreSQLTableManager:
    """Manages pandas DataFrames for each PostgreSQL table"""
    
    def __init__(self):
        # Original FHIR-focused tables for analysis and debugging
        self.tables = {
            'ehr_vendor': pd.DataFrame(columns=['id', 'name', 'is_cms_aligned_network']),
            'organization': pd.DataFrame(columns=['id', 'original_id', 'full_url', 'name', 'active', 'vendor_name', 'created_at']),
            'endpoint_instance': pd.DataFrame(columns=['id', 'original_id', 'full_url', 'ehr_vendor_id', 'address', 'endpoint_connection_type_id', 'name', 'description', 'environment_type_id', 'status', 'vendor_name', 'created_at']),
            'endpoint_instance_to_other_id': pd.DataFrame(columns=['endpoint_instance_id', 'endpoint_instance_original_id', 'other_id', 'system', 'issuer_id', 'is_valid_format', 'is_valid_api', 'validation_error']),
            'endpoint_instance_to_payload': pd.DataFrame(columns=['endpoint_instance_id', 'endpoint_instance_original_id', 'payload_type_id', 'mime_type_id']),
            'endpoint_connection_type': pd.DataFrame(columns=['id', 'display', 'definition']),
            'environment_type': pd.DataFrame(columns=['id', 'description']),
            'data_lineage': pd.DataFrame(columns=['entity_uuid', 'entity_type', 'original_id', 'full_url', 'vendor_name', 'generation_method', 'created_at']),
            'field_coverage_log': pd.DataFrame(columns=['vendor_name', 'resource_type', 'total_fields_seen', 'fields_processed', 'fields_ignored', 'coverage_percentage', 'ignored_fields_json', 'created_at']),
            
            # NPD-focused tables matching full_npd.sql schema exactly
            'npd_ehr_vendor': pd.DataFrame(columns=['id', 'name', 'is_cms_aligned_network']),
            'npd_endpoint_connection_type': pd.DataFrame(columns=['id', 'display', 'definition']),
            'npd_environment_type': pd.DataFrame(columns=['id', 'display', 'definition']),
            'npd_endpoint_instance': pd.DataFrame(columns=['id', 'ehr_vendor_id', 'address', 'endpoint_connection_type_id', 'name', 'description', 'environment_type_id']),
            'npd_endpoint_instance_to_other_id': pd.DataFrame(columns=['endpoint_instance_id', 'other_id', 'system', 'issuer_id']),
            'npd_endpoint_instance_to_payload': pd.DataFrame(columns=['endpoint_instance_id', 'mime_type_id', 'payload_type_id']),
            'npd_organization': pd.DataFrame(columns=['id', 'authorized_official_id', 'ein_id', 'parent_id']),
            'npd_organization_to_address': pd.DataFrame(columns=['organization_id', 'address_id', 'address_use_id']),
            'npd_organization_to_phone': pd.DataFrame(columns=['organization_id', 'phone_number', 'extension', 'phone_use_id', 'id']),
            'npd_address_us': pd.DataFrame(columns=['id', 'addressee', 'delivery_line_1', 'delivery_line_2', 'last_line', 'city_name', 'state_code', 'zipcode', 'plus4_code', 'latitude', 'longitude']),
            'npd_address_international': pd.DataFrame(columns=['id', 'country_code', 'address1', 'address2', 'address3', 'address4', 'locality', 'administrative_area', 'postal_code', 'latitude', 'longitude']),
            'npd_address_nonstandard': pd.DataFrame(columns=['id', 'addressee', 'delivery_line_1', 'delivery_line_2', 'last_line', 'address_type', 'raw_address', 'latitude', 'longitude']),
            'npd_address': pd.DataFrame(columns=['id', 'address_us_id', 'address_international_id', 'address_nonstandard_id']),
            'npd_fhir_address_use': pd.DataFrame(columns=['id', 'value']),
            'npd_fhir_email_use': pd.DataFrame(columns=['id', 'value']),
            'npd_fhir_name_use': pd.DataFrame(columns=['id', 'value']),
            'npd_fhir_phone_system': pd.DataFrame(columns=['id', 'value']),
            'npd_fhir_phone_use': pd.DataFrame(columns=['id', 'value']),
            'npd_payload_type': pd.DataFrame(columns=['id', 'value', 'description']),
            'npd_mime_type': pd.DataFrame(columns=['id', 'value']),
            
            # Native FHIR contact tables (full metadata preservation)
            'fhir_organization_address': pd.DataFrame(columns=['organization_id', 'organization_original_id', 'address_type', 'text', 'address_line1', 'address_line2', 'city', 'state', 'postal_code', 'country', 'use', 'sequence', 'created_at']),
            'fhir_organization_phone': pd.DataFrame(columns=['organization_id', 'organization_original_id', 'original_value', 'normalized_number', 'extension', 'country_code', 'is_valid', 'parse_error', 'use', 'sequence', 'created_at']),
            'fhir_organization_email': pd.DataFrame(columns=['organization_id', 'organization_original_id', 'email_value', 'use', 'is_valid', 'validation_error', 'sequence', 'created_at']),
            'fhir_organization_contact_url': pd.DataFrame(columns=['organization_id', 'organization_original_id', 'url_value', 'use', 'is_valid', 'validation_error', 'sequence', 'created_at'])
        }
        
        self._vendor_cache = {}  # Cache vendor UUIDs
        
    def add_records(self, *, table_name: str, records: List[Dict[str, Any]]):
        """Add records to a specific table DataFrame"""
        if table_name not in self.tables:
            print(f"Warning: Unknown table {table_name}")
            return
            
        if records:
            new_df = pd.DataFrame(records)
            # Handle empty DataFrame concatenation to avoid FutureWarning
            if self.tables[table_name].empty:
                self.tables[table_name] = new_df
            else:
                self.tables[table_name] = pd.concat([self.tables[table_name], new_df], ignore_index=True)
    
    def add_vendor(self, *, vendor_uuid: str, vendor_name: str, directory_path: str):
        """Add vendor record"""
        if vendor_uuid not in self._vendor_cache:
            vendor_record = {
                'id': vendor_uuid,
                'name': vendor_name,
                'is_cms_aligned_network': False  # Default, could be enhanced
            }
            self.add_records(table_name='ehr_vendor', records=[vendor_record])
            self._vendor_cache[vendor_uuid] = vendor_name
    
    def add_field_coverage_log(self, *, vendor_name: str, resource_type: str, field_tracker: FieldTracker):
        """Add field coverage log record"""
        coverage_report = field_tracker.get_coverage_report()
        
        coverage_record = {
            'vendor_name': vendor_name,
            'resource_type': resource_type,
            'total_fields_seen': coverage_report['total_fields_seen'],
            'fields_processed': coverage_report['fields_processed'],
            'fields_ignored': coverage_report['fields_ignored'],
            'coverage_percentage': coverage_report['coverage_percentage'],
            'ignored_fields_json': str(coverage_report['ignored_fields_list']),  # Simple string conversion
            'created_at': datetime.now().isoformat()
        }
        
        self.add_records(table_name='field_coverage_log', records=[coverage_record])
    
    def deduplicate_all_tables(self):
        """Remove duplicates from all tables"""
        for table_name, df in self.tables.items():
            if not df.empty:
                try:
                    # Use all columns for deduplication except timestamps
                    subset_cols = [col for col in df.columns if 'created_at' not in col.lower()]
                    if subset_cols:
                        self.tables[table_name] = df.drop_duplicates(subset=subset_cols, keep='first')
                except TypeError as e:
                    # Handle unhashable types (like dicts) by converting to string first
                    print(f"Warning: Cannot deduplicate {table_name} due to unhashable types: {e}")
                    print(f"Skipping deduplication for {table_name}")
    
    def export_csv_files(self, *, output_dir: Path):
        """Export all tables as PostgreSQL-ready CSV files"""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        exported_files = []
        for table_name, df in self.tables.items():
            if not df.empty:
                csv_path = output_dir / f"{table_name}.csv"
                df.to_csv(
                    csv_path, 
                    index=False, 
                    na_rep='\\N',  # PostgreSQL NULL representation
                    quoting=csv.QUOTE_MINIMAL,
                    date_format='%Y-%m-%d %H:%M:%S'
                )
                exported_files.append(csv_path)
                print(f"Exported {len(df)} records to {csv_path}")
        
        return exported_files
    
    def get_summary_stats(self) -> Dict[str, int]:
        """Get summary statistics for all tables"""
        stats = {}
        for table_name, df in self.tables.items():
            stats[table_name] = len(df)
        return stats
