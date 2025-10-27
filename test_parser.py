#!/usr/bin/env python3
"""
Simple test script for the new FHIR parser implementation
"""
import json
from pathlib import Path
from parser import FHIRCacheProcessor
from parser.models.factories import create_fhir_resource_from_json


def test_organization_parsing():
    """Test parsing of a simple organization resource"""
    print("Testing Organization parsing...")
    
    # Sample organization data based on our test file
    test_org_data = {
        "fullUrl": "https://example.com/Organization/test123",
        "resource": {
            "resourceType": "Organization",
            "id": "test123",
            "name": "Test Organization",
            "active": True,
            "identifier": [
                {
                    "system": "http://hl7.org/fhir/sid/us-npi",
                    "value": "1234567890"
                }
            ],
            "address": [
                {
                    "line": ["123 Test St", "Suite 456"],
                    "city": "Test City",
                    "state": "TS",
                    "postalCode": "12345",
                    "country": "US"
                }
            ],
            "telecom": [
                {
                    "system": "phone",
                    "value": "+1-555-123-4567",
                    "use": "work"
                },
                {
                    "system": "email",
                    "value": "test@example.com",
                    "use": "work"
                }
            ],
            "endpoint": [
                {
                    "reference": "Endpoint/fhir-endpoint-123"
                }
            ]
        }
    }
    
    try:
        # Create FHIR resource
        resource = create_fhir_resource_from_json(
            json_data=test_org_data,
            vendor_name="test_vendor"
        )
        
        if resource:
            print(f"✓ Successfully created {resource.resource_type} resource")
            print(f"  UUID: {resource.uuid_id}")
            print(f"  Original ID: {resource.original_id}")
            print(f"  Name: {getattr(resource, 'name', 'N/A')}")
            
            # Test PostgreSQL record generation
            postgres_records = resource.to_postgres_records()
            print(f"  Generated {len(postgres_records)} table record sets")
            
            for table_name, records in postgres_records.items():
                print(f"    {table_name}: {len(records)} records")
            
            # Test field coverage
            coverage_report = resource.get_field_coverage_report()
            print(f"  Field coverage: {coverage_report['coverage_percentage']:.1f}%")
            
            return True
        else:
            print("✗ Failed to create resource")
            return False
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def test_endpoint_parsing():
    """Test parsing of a simple endpoint resource"""
    print("\nTesting Endpoint parsing...")
    
    # Sample endpoint data
    test_endpoint_data = {
        "fullUrl": "https://example.com/Endpoint/fhir-endpoint-123",
        "resource": {
            "resourceType": "Endpoint",
            "id": "fhir-endpoint-123",
            "status": "active",
            "connectionType": {
                "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                "code": "hl7-fhir-rest",
                "display": "HL7 FHIR REST"
            },
            "name": "Test FHIR Endpoint",
            "address": "https://example.com/fhir/r4",
            "payloadType": [
                {
                    "coding": [
                        {
                            "system": "http://ihe.net/fhir/ihe.formatcode.fhir/CodeSystem/formatcode",
                            "code": "urn:ihe:pcc:xphr:2007"
                        }
                    ]
                }
            ],
            "payloadMimeType": [
                "application/fhir+json",
                "application/json+fhir"
            ]
        }
    }
    
    try:
        # Create FHIR resource
        resource = create_fhir_resource_from_json(
            json_data=test_endpoint_data,
            vendor_name="test_vendor"
        )
        
        if resource:
            print(f"✓ Successfully created {resource.resource_type} resource")
            print(f"  UUID: {resource.uuid_id}")
            print(f"  Original ID: {resource.original_id}")
            print(f"  Address: {getattr(resource, 'address', 'N/A')}")
            
            # Test PostgreSQL record generation
            postgres_records = resource.to_postgres_records()
            print(f"  Generated {len(postgres_records)} table record sets")
            
            for table_name, records in postgres_records.items():
                print(f"    {table_name}: {len(records)} records")
            
            # Test field coverage
            coverage_report = resource.get_field_coverage_report()
            print(f"  Field coverage: {coverage_report['coverage_percentage']:.1f}%")
            
            return True
        else:
            print("✗ Failed to create resource")
            return False
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def test_processor_initialization():
    """Test processor initialization"""
    print("\nTesting processor initialization...")
    
    try:
        # Create test directories
        test_cache = Path("./test_cache_dir")
        test_output = Path("./test_output_dir")
        
        processor = FHIRCacheProcessor(
            cache_root=test_cache,
            output_dir=test_output
        )
        
        print("✓ Successfully initialized FHIRCacheProcessor")
        print(f"  Cache root: {processor.cache_root}")
        print(f"  Output dir: {processor.output_dir}")
        print(f"  Run ID: {processor.run_tracker.run_id}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def main():
    """Run all tests"""
    print("FHIR Parser Implementation Test")
    print("=" * 50)
    
    tests = [
        ("Organization Parsing", test_organization_parsing),
        ("Endpoint Parsing", test_endpoint_parsing),
        ("Processor Initialization", test_processor_initialization)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n[{passed + 1}/{total}] {test_name}")
        print("-" * 30)
        
        if test_func():
            passed += 1
        
        print()
    
    print("=" * 50)
    print(f"RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed! Implementation looks good.")
        return 0
    else:
        print("✗ Some tests failed. Check the output above.")
        return 1


if __name__ == "__main__":
    exit(main())
