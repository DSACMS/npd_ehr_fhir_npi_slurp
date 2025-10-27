"""
Main FHIR cache processing pipeline
"""
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid

from .models.factories import ResourceFactoryRegistry, default_factory_registry
from .models import FHIRResource, FHIROrganization, FHIREndpoint
from .output import PostgreSQLTableManager
from .utils import DeterministicUUIDGenerator, FieldTracker
from .utils.validators import get_npi_validator_singleton


class ProcessingRunTracker:
    """Tracks a single processing run for reporting"""
    
    def __init__(self):
        self.run_id = str(uuid.uuid4())
        self.vendor_trackers: Dict[str, FieldTracker] = {}
        self.start_time = datetime.now()
        self.processed_files = 0
        self.failed_files = 0
        self.total_files = 0
        
    def get_vendor_tracker(self, *, vendor_name: str) -> FieldTracker:
        """Get or create field tracker for vendor"""
        if vendor_name not in self.vendor_trackers:
            self.vendor_trackers[vendor_name] = FieldTracker()
        return self.vendor_trackers[vendor_name]
    
    def increment_processed(self):
        """Increment processed file counter"""
        self.processed_files += 1
    
    def increment_failed(self):
        """Increment failed file counter"""
        self.failed_files += 1
    
    def set_total_files(self, *, total: int):
        """Set total file count"""
        self.total_files = total
    
    def generate_coverage_report(self) -> Dict[str, Any]:
        """Generate comprehensive coverage report"""
        report = {
            'run_id': self.run_id,
            'start_time': self.start_time.isoformat(),
            'end_time': datetime.now().isoformat(),
            'vendors_processed': len(self.vendor_trackers),
            'files_processed': self.processed_files,
            'files_failed': self.failed_files,
            'total_files': self.total_files,
            'success_rate': (self.processed_files / self.total_files * 100) if self.total_files > 0 else 0,
            'vendor_coverage': {}
        }
        
        for vendor_name, tracker in self.vendor_trackers.items():
            report['vendor_coverage'][vendor_name] = tracker.get_coverage_report()
            
        return report


class VendorProcessor:
    """Processes a single EHR vendor's directory structure"""
    
    def __init__(self, *, vendor_path: Path, vendor_name: str, run_tracker: ProcessingRunTracker, test_mode: bool = False):
        self.vendor_path = vendor_path
        self.vendor_name = vendor_name
        self.run_tracker = run_tracker
        self.test_mode = test_mode
        
        # Generate vendor UUID
        self.vendor_uuid = DeterministicUUIDGenerator.generate_vendor_uuid(
            vendor_name=vendor_name,
            directory_path=str(vendor_path)
        )
        
        # Resource processing counters
        self.organizations_processed = 0
        self.endpoints_processed = 0
        
    def process_all_resources(self) -> List[FHIRResource]:
        """Process both organizations and endpoints for this vendor"""
        resources = []
        
        # Process organization files
        org_dir = self.vendor_path / 'organization'
        if org_dir.exists():
            org_resources = self._process_directory(directory_path=org_dir, resource_type='Organization')
            resources.extend(org_resources)
            self.organizations_processed = len(org_resources)
        
        # Process endpoint files
        endpoint_dir = self.vendor_path / 'endpoint'
        if endpoint_dir.exists():
            endpoint_resources = self._process_directory(directory_path=endpoint_dir, resource_type='Endpoint')
            resources.extend(endpoint_resources)
            self.endpoints_processed = len(endpoint_resources)
        
        print(f"Vendor {self.vendor_name}: {self.organizations_processed} orgs, {self.endpoints_processed} endpoints")
        
        return resources
    
    def _process_directory(self, *, directory_path: Path, resource_type: str) -> List[FHIRResource]:
        """Process all JSON files in a directory"""
        resources = []
        
        if not directory_path.exists():
            return resources
        
        json_files = list(directory_path.glob('*.json'))
        
        # Test mode: limit files processed
        if self.test_mode and len(json_files) > 100:
            json_files = json_files[:100]
            print(f"  TEST MODE: Limited to first 100 {resource_type} files")
        
        for json_file in json_files:
            try:
                resource = default_factory_registry.create_resource_from_file(
                    file_path=str(json_file),
                    vendor_name=self.vendor_name
                )
                
                if resource:
                    resources.append(resource)
                    self.run_tracker.increment_processed()
                    
                    # Aggregate field tracking
                    vendor_tracker = self.run_tracker.get_vendor_tracker(vendor_name=self.vendor_name)
                    for field_path in resource.field_tracker.all_seen_fields:
                        vendor_tracker.track_field_access(field_path, field_path in resource.field_tracker.processed_fields)
                else:
                    self.run_tracker.increment_failed()
                    
            except Exception as e:
                print(f"Error processing {json_file}: {e}")
                self.run_tracker.increment_failed()
        
        return resources


class FHIRCacheProcessor:
    """Main orchestrator for processing the entire FHIR cache"""
    
    def __init__(self, *, cache_root: Path, output_dir: Path):
        self.cache_root = cache_root
        self.output_dir = output_dir
        self.postgres_manager = PostgreSQLTableManager()
        self.run_tracker = ProcessingRunTracker()
        
        # Initialize NPI validator singleton early to keep it in memory for the entire run
        self.npi_singleton = get_npi_validator_singleton()
        
        print(f"FHIR Cache Processor initialized")
        print(f"Cache root: {cache_root}")
        print(f"Output directory: {output_dir}")
        print(f"Processing run ID: {self.run_tracker.run_id}")
        
        # Report NPI validator status
        if self.npi_singleton.is_available():
            print("✓ NPI validator loaded and ready for validation")
        else:
            print("⚠ NPI validator not available - will use format validation only")
        
    def process_entire_cache(self, *, test_mode: bool = False) -> Dict[str, Any]:
        """Process all vendor directories in the cache"""
        
        print(f"\nStarting FHIR cache processing...")
        if test_mode:
            print("*** TEST MODE ENABLED: Limited file processing per vendor ***")
        
        # Step 1: Discover vendor directories
        vendor_directories = self._get_vendor_directories()
        print(f"Found {len(vendor_directories)} vendor directories")
        
        if not vendor_directories:
            print("No vendor directories found!")
            return self.run_tracker.generate_coverage_report()
        
        # Step 2: Process each vendor directory
        all_resources = []
        for i, vendor_dir in enumerate(vendor_directories, 1):
            print(f"\n[{i}/{len(vendor_directories)}] Processing vendor: {vendor_dir.name}")
            
            vendor_processor = VendorProcessor(
                vendor_path=vendor_dir,
                vendor_name=vendor_dir.name,
                run_tracker=self.run_tracker,
                test_mode=test_mode
            )
            
            # Add vendor to PostgreSQL tables
            self.postgres_manager.add_vendor(
                vendor_uuid=vendor_processor.vendor_uuid,
                vendor_name=vendor_processor.vendor_name,
                directory_path=str(vendor_dir)
            )
            
            # Process resources
            vendor_resources = vendor_processor.process_all_resources()
            all_resources.extend(vendor_resources)
            
            # Add resources to PostgreSQL tables
            self._add_resources_to_tables(resources=vendor_resources, vendor_uuid=vendor_processor.vendor_uuid)
            
            # Progress indicator
            if i % 10 == 0:
                print(f"Processed {i} vendors so far...")
        
        # Step 3: Post-processing
        print(f"\nPost-processing...")
        self._populate_lookup_tables(resources=all_resources)
        self._add_field_coverage_logs()
        self.postgres_manager.deduplicate_all_tables()
        
        # Step 4: Export CSV files
        print(f"\nExporting CSV files...")
        self.postgres_manager.export_csv_files(output_dir=self.output_dir)
        
        # Step 5: Generate summary report
        summary_report = self._generate_summary_report(resources=all_resources)
        
        print(f"\nProcessing complete!")
        print(f"Run ID: {self.run_tracker.run_id}")
        print(f"Total resources processed: {len(all_resources)}")
        print(f"Files processed: {self.run_tracker.processed_files}")
        print(f"Files failed: {self.run_tracker.failed_files}")
        
        return summary_report
    
    def _get_vendor_directories(self) -> List[Path]:
        """Get all vendor directories that have both endpoint and organization subdirs"""
        vendors = []
        
        if not self.cache_root.exists():
            print(f"Cache root directory does not exist: {self.cache_root}")
            return vendors
        
        for vendor_dir in self.cache_root.iterdir():
            if (vendor_dir.is_dir() and 
                (vendor_dir / 'endpoint').exists() and 
                (vendor_dir / 'organization').exists()):
                vendors.append(vendor_dir)
        
        return sorted(vendors)
    
    def _add_resources_to_tables(self, *, resources: List[FHIRResource], vendor_uuid: str):
        """Add FHIR resources to PostgreSQL tables"""
        for resource in resources:
            # Get PostgreSQL records from resource
            postgres_records = resource.to_postgres_records()
            
            # Add to appropriate tables
            for table_name, records in postgres_records.items():
                if records:  # Only add if there are records
                    # Add vendor_id to endpoint_instance records
                    if table_name == 'endpoint_instance':
                        for record in records:
                            record['ehr_vendor_id'] = vendor_uuid
                    
                    self.postgres_manager.add_records(table_name=table_name, records=records)
    
    def _populate_lookup_tables(self, *, resources: List[FHIRResource]):
        """Populate lookup tables with discovered values"""
        connection_types = set()
        environment_types = set()
        
        for resource in resources:
            if isinstance(resource, FHIREndpoint):
                # Collect connection types
                conn_type_record = resource.get_connection_type_record()
                if conn_type_record:
                    connection_types.add((
                        conn_type_record['id'],
                        conn_type_record['display'],
                        conn_type_record['definition']
                    ))
                
                # Collect environment types
                env_type = resource._determine_environment_type()
                environment_types.add(env_type)
        
        # Add connection type records
        conn_type_records = [
            {'id': ct[0], 'display': ct[1], 'definition': ct[2]}
            for ct in connection_types
        ]
        if conn_type_records:
            self.postgres_manager.add_records(table_name='endpoint_connection_type', records=conn_type_records)
        
        # Add environment type records
        env_type_records = [
            {'id': et, 'description': f"{et.title()} environment"}
            for et in environment_types
        ]
        if env_type_records:
            self.postgres_manager.add_records(table_name='environment_type', records=env_type_records)
    
    def _add_field_coverage_logs(self):
        """Add field coverage logs to database"""
        for vendor_name, tracker in self.run_tracker.vendor_trackers.items():
            # Add separate logs for each resource type processed
            if tracker.all_seen_fields:  # Only if we actually processed fields
                self.postgres_manager.add_field_coverage_log(
                    vendor_name=vendor_name,
                    resource_type='Mixed',  # Could be enhanced to separate by type
                    field_tracker=tracker
                )
    
    def _generate_summary_report(self, *, resources: List[FHIRResource]) -> Dict[str, Any]:
        """Generate comprehensive summary report"""
        
        # Count resources by type
        resource_counts = {}
        for resource in resources:
            resource_type = resource.resource_type
            resource_counts[resource_type] = resource_counts.get(resource_type, 0) + 1
        
        # Get table statistics
        table_stats = self.postgres_manager.get_summary_stats()
        
        # Generate coverage report
        coverage_report = self.run_tracker.generate_coverage_report()
        
        summary = {
            'processing_summary': coverage_report,
            'resource_counts': resource_counts,
            'table_statistics': table_stats,
            'output_directory': str(self.output_dir)
        }
        
        return summary
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get current processing summary"""
        return {
            'run_id': self.run_tracker.run_id,
            'start_time': self.run_tracker.start_time.isoformat(),
            'files_processed': self.run_tracker.processed_files,
            'files_failed': self.run_tracker.failed_files,
            'vendors_discovered': len(self.run_tracker.vendor_trackers),
            'table_stats': self.postgres_manager.get_summary_stats()
        }
