#!/usr/bin/env python3
"""
Test script to verify the CLI fixes work
"""
import sys
from pathlib import Path

# Test 1: Import and basic initialization
def test_imports():
    """Test that all imports work correctly"""
    print("Testing imports...")
    try:
        from parser import FHIRCacheProcessor
        from parser.models.factories import create_fhir_resource_from_json
        from parser.utils.validators import get_npi_validator_singleton
        from parser.output import PostgreSQLTableManager
        print("✓ All imports successful")
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False

# Test 2: NPI Validator singleton
def test_npi_validator_singleton():
    """Test NPI validator singleton initialization"""
    print("\nTesting NPI validator singleton...")
    try:
        from parser.utils.validators import get_npi_validator_singleton
        
        # Get singleton - this should not crash even if NPIValidator is unavailable
        singleton = get_npi_validator_singleton()
        
        if singleton.is_available():
            print("✓ NPI validator singleton initialized successfully")
        else:
            print("⚠ NPI validator not available, but singleton handled gracefully")
        
        return True
    except Exception as e:
        print(f"✗ NPI validator singleton failed: {e}")
        return False

# Test 3: Table manager DataFrame handling
def test_table_manager():
    """Test PostgreSQL table manager DataFrame operations"""
    print("\nTesting table manager...")
    try:
        from parser.output import PostgreSQLTableManager
        
        manager = PostgreSQLTableManager()
        
        # Test adding records to empty DataFrame (this was causing the FutureWarning)
        test_records = [
            {'id': 'test1', 'name': 'Test Vendor 1', 'is_cms_aligned_network': False},
            {'id': 'test2', 'name': 'Test Vendor 2', 'is_cms_aligned_network': True}
        ]
        
        manager.add_records(table_name='ehr_vendor', records=test_records)
        
        # Test getting stats
        stats = manager.get_summary_stats()
        
        if stats.get('ehr_vendor', 0) == 2:
            print("✓ Table manager operations successful")
            return True
        else:
            print(f"✗ Table manager failed - expected 2 records, got {stats.get('ehr_vendor', 0)}")
            return False
            
    except Exception as e:
        print(f"✗ Table manager test failed: {e}")
        return False

# Test 4: Basic resource creation
def test_resource_creation():
    """Test basic FHIR resource creation"""
    print("\nTesting FHIR resource creation...")
    try:
        from parser.models.factories import create_fhir_resource_from_json
        
        # Simple test data
        test_data = {
            "fullUrl": "https://example.com/Organization/test123",
            "resource": {
                "resourceType": "Organization",
                "id": "test123",
                "name": "Test Organization",
                "active": True
            }
        }
        
        resource = create_fhir_resource_from_json(
            json_data=test_data,
            vendor_name="test_vendor"
        )
        
        if resource and resource.resource_type == 'Organization':
            print("✓ FHIR resource creation successful")
            return True
        else:
            print("✗ FHIR resource creation failed")
            return False
            
    except Exception as e:
        print(f"✗ FHIR resource creation failed: {e}")
        return False

def main():
    """Run all tests"""
    print("FHIR Parser Fix Verification")
    print("=" * 50)
    
    tests = [
        ("Import Test", test_imports),
        ("NPI Validator Singleton", test_npi_validator_singleton),
        ("Table Manager", test_table_manager),
        ("Resource Creation", test_resource_creation)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        if test_func():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"VERIFICATION RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All fixes verified! The CLI should work correctly now.")
        return 0
    else:
        print("✗ Some tests failed. There may still be issues.")
        return 1

if __name__ == "__main__":
    exit(main())
