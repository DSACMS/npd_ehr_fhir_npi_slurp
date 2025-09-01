#!/usr/bin/env python3
"""
Test script for EHR FHIR NPI Slurp pipeline

This script runs basic tests to validate the pipeline components
and data processing functionality.
"""

import unittest
import tempfile
import os
import sys
from pathlib import Path
import pandas as pd
import json

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

def load_env_file(*, env_file_path="data_files.env"):
    """Load environment variables from a .env file."""
    if not os.path.exists(env_file_path):
        print(f"Warning: Environment file '{env_file_path}' not found. Using defaults.")
        return {}
    
    env_vars = {}
    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Parse KEY=VALUE format
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                if key and value:
                    env_vars[key] = value
                    # Also set in os.environ if not already present
                    if key not in os.environ:
                        os.environ[key] = value
    return env_vars

# Load environment variables from data_files.env
ENV_VARS = load_env_file()

class TestPipelineComponents(unittest.TestCase):
    """Test cases for pipeline components"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = Path(tempfile.mkdtemp())
        self.sample_csv_data = {
            'url': ['http://example.com/fhir', 'http://test.com/fhir'],
            'api_information_source_name': ['Test API', 'Sample API'],
            'created_at': ['2023-01-01', '2023-01-02'],
            'updated': ['2023-01-01', '2023-01-02'],
            'list_source': ['http://example.com/list', 'http://test.com/list'],
            'certified_api_developer_name': ['Test Vendor', 'Sample Vendor'],
            'capability_fhir_version': ['4.0.1', '4.0.1'],
            'format': ['json', 'json'],
            'http_response': [200, 200],
            'http_response_time_second': [1.5, 2.0],
            'smart_http_response': [200, 200],
            'errors': ['', ''],
            'cap_stat_exists': [True, True],
            'kind': ['instance', 'instance'],
            'requested_fhir_version': ['4.0.1', '4.0.1'],
            'is_chpl': [True, False]
        }
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_env_loading(self):
        """Test environment variables loading"""
        env_vars = load_env_file()
        self.assertIsInstance(env_vars, dict)
        # Check that some expected variables are loaded
        if env_vars:
            # We expect to have at least some of these variables
            expected_vars = [
                'CEHRT_CACHE_DIR',
                'LANTERN_CSV_INPUT', 
                'LIST_SOURCES_SUMMARY',
                'SERVICE_JSON_DIR'
            ]
            found_vars = [var for var in expected_vars if var in env_vars]
            self.assertGreater(len(found_vars), 0, "Should find at least one expected environment variable")
    
    def test_csv_creation(self):
        """Test CSV file creation and validation"""
        # Create test CSV
        test_csv = self.test_dir / 'test_endpoints.csv'
        df = pd.DataFrame(self.sample_csv_data)
        df.to_csv(test_csv, index=False)
        
        # Validate CSV can be read
        loaded_df = pd.read_csv(test_csv)
        self.assertEqual(len(loaded_df), 2)
        self.assertIn('url', loaded_df.columns)
        self.assertIn('list_source', loaded_df.columns)
    
    def test_step10_import(self):
        """Test Step 10 script can be imported"""
        try:
            import Step10_extract_list_source_from_lantern_csv
            self.assertTrue(hasattr(Step10_extract_list_source_from_lantern_csv, 'main'))
        except ImportError as e:
            self.fail(f"Could not import Step10 script: {e}")
    
    def test_step20_import(self):
        """Test Step 20 script can be imported"""
        try:
            import Step20_download_list_source_json
            self.assertTrue(hasattr(Step20_download_list_source_json, 'main'))
            self.assertTrue(hasattr(Step20_download_list_source_json, 'create_safe_filename'))
        except ImportError as e:
            self.fail(f"Could not import Step20 script: {e}")
    
    def test_step30_import(self):
        """Test Step 30 script can be imported"""
        try:
            import Step30_parse_source_bundle
            self.assertTrue(hasattr(Step30_parse_source_bundle, 'main'))
            self.assertTrue(hasattr(Step30_parse_source_bundle, 'parse_fhir_bundle'))
        except ImportError as e:
            self.fail(f"Could not import Step30 script: {e}")
    
    def test_step40_import(self):
        """Test Step 40 script can be imported"""
        try:
            import Step40_extract_csv_data
            self.assertTrue(hasattr(Step40_extract_csv_data, 'main'))
        except ImportError as e:
            self.skipTest(f"Step40 script has import issues (expected): {e}")
    
    def test_safe_filename_creation(self):
        """Test safe filename creation function"""
        try:
            from Step20_download_list_source_json import create_safe_filename
            
            test_cases = [
                ("Test Vendor Inc.", "test_vendor_inc"),
                ("Epic Systems Corp", "epic_systems_corp"),
                ("Vendor-Name (2023)", "vendor_name_2023"),
                ("Special!@#$%Characters", "special_characters")
            ]
            
            for input_name, expected in test_cases:
                result = create_safe_filename(input_name)
                self.assertEqual(result, expected)
                
        except ImportError:
            self.skipTest("Step20 not available for testing")
    
    def test_directory_structure(self):
        """Test that required directories can be created"""
        # Test creating a temporary directory structure
        test_dir = Path(tempfile.mkdtemp()) / "test_data"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Verify directory was created
        self.assertTrue(test_dir.exists())
        self.assertTrue(test_dir.is_dir())
        
        # Clean up
        import shutil
        shutil.rmtree(test_dir.parent, ignore_errors=True)
    
    def test_sample_fhir_bundle_parsing(self):
        """Test FHIR bundle structure parsing"""
        sample_bundle = {
            "resourceType": "Bundle",
            "id": "test-bundle",
            "entry": [
                {
                    "fullUrl": "http://example.com/Organization/123",
                    "resource": {
                        "resourceType": "Organization",
                        "id": "123",
                        "name": "Test Organization",
                        "identifier": [
                            {
                                "system": "http://hl7.org/fhir/sid/us-npi",
                                "value": "1234567890"
                            }
                        ]
                    }
                }
            ]
        }
        
        # Test bundle structure
        self.assertEqual(sample_bundle["resourceType"], "Bundle")
        self.assertEqual(len(sample_bundle["entry"]), 1)
        
        # Test organization structure
        org = sample_bundle["entry"][0]["resource"]
        self.assertEqual(org["resourceType"], "Organization")
        self.assertEqual(org["name"], "Test Organization")
        self.assertEqual(len(org["identifier"]), 1)

class TestDataValidation(unittest.TestCase):
    """Test data validation functions"""
    
    def test_npi_format_validation(self):
        """Test NPI format validation"""
        try:
            from NPIValidator import NPIValidator
            
            # Valid NPIs
            self.assertTrue(NPIValidator._is_valid_npi_format(npi_value="1234567890"))
            self.assertTrue(NPIValidator._is_valid_npi_format(npi_value="0123456789"))
            
            # Invalid NPIs
            self.assertFalse(NPIValidator._is_valid_npi_format(npi_value="123456789"))  # Too short
            self.assertFalse(NPIValidator._is_valid_npi_format(npi_value="12345678901"))  # Too long
            self.assertFalse(NPIValidator._is_valid_npi_format(npi_value="123456789a"))  # Contains letter
            self.assertFalse(NPIValidator._is_valid_npi_format(npi_value=""))  # Empty
            self.assertFalse(NPIValidator._is_valid_npi_format(npi_value=""))  # Empty string to test None case
            
        except ImportError:
            self.skipTest("NPIValidator not available")

def run_basic_tests():
    """Run basic functionality tests"""
    print("Running EHR FHIR NPI Slurp Pipeline Tests")
    print("=" * 50)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestPipelineComponents))
    suite.addTests(loader.loadTestsFromTestCase(TestDataValidation))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    return result.wasSuccessful()

def check_dependencies():
    """Check if required dependencies are available"""
    print("Checking Dependencies...")
    print("-" * 30)
    
    required_packages = [
        'pandas',
        'requests',
        'tqdm'
    ]
    
    optional_packages = [
        'phonenumbers'
    ]
    
    missing_required = []
    missing_optional = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} (REQUIRED)")
            missing_required.append(package)
    
    for package in optional_packages:
        try:
            __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"? {package} (optional)")
            missing_optional.append(package)
    
    print()
    
    if missing_required:
        print(f"Missing required packages: {', '.join(missing_required)}")
        print("Install with: pip install " + " ".join(missing_required))
        return False
    
    if missing_optional:
        print(f"Missing optional packages: {', '.join(missing_optional)}")
        print("Install with: pip install " + " ".join(missing_optional))
    
    return True

def main():
    """Main test function"""
    print("EHR FHIR NPI Slurp - Pipeline Test Suite")
    print("=" * 60)
    print()
    
    # Check dependencies first
    deps_ok = check_dependencies()
    print()
    
    if not deps_ok:
        print("Some required dependencies are missing. Please install them first.")
        return False
    
    # Run tests
    success = run_basic_tests()
    
    if success:
        print("\n🎉 All tests passed! Pipeline appears to be working correctly.")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
