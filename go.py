#!/usr/bin/env python3

"""
EHR FHIR NPI Slurp Pipeline
Complete data processing pipeline for FHIR endpoint analysis

Uses legacy pipeline for data download and new parser for processing.
"""

import sys
import os
import subprocess
import time
import glob
from pathlib import Path


def load_env_file(*, env_file_path="data_files.env"):
    """Load environment variables from a .env file."""
    if not os.path.exists(env_file_path):
        print(f"Warning: Environment file '{env_file_path}' not found. Using defaults.")
        return
    
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
                # Only set if not already in environment and value is not empty
                if key and value and key not in os.environ:
                    os.environ[key] = value


def get_env_var(*, key, default_value):
    """Get environment variable with a default fallback."""
    return os.environ.get(key, default_value)


def check_virtual_env():
    """Check if virtual environment is activated."""
    if not os.environ.get('VIRTUAL_ENV'):
        print("Warning: Virtual environment not detected. Consider running:")
        print("source source_me_to_get_venv.sh")
        print("")


def run_step(*, step_num, description, command_args, success_message=None):
    """
    Run a pipeline step with proper error handling and logging.
    
    Args:
        step_num: Step number for display
        description: Description of what this step does
        command_args: List of command arguments to execute
        success_message: Optional custom success message
    """
    print(f"Step {step_num}: {description}...")
    
    try:
        # Run the command
        result = subprocess.run(command_args, check=True, capture_output=False)
        
        # Success message
        success_msg = success_message or f"✓ Step {step_num} completed"
        print(success_msg)
        print("")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Step {step_num} failed with return code {e.returncode}")
        print(f"Command: {' '.join(command_args)}")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"❌ Step {step_num} failed: {e}")
        print(f"Command: {' '.join(command_args)}")
        sys.exit(1)


def main():
    """Main pipeline execution."""
    print("Starting EHR FHIR NPI Slurp Pipeline...")
    print("========================================")
    
    # Load environment variables from data_files.env
    load_env_file()
    
    # Check virtual environment
    check_virtual_env()

    # Cache preparation steps (legacy pipeline)
    print("PHASE 1: CACHE PREPARATION")
    print("Using legacy pipeline for data download and preparation")
    print("")

    # Step 1: Extract list sources from Lantern CSV
    run_step(
        step_num=1,
        description="Extracting list sources from Lantern CSV",
        command_args=[
            "python", "Step10_extract_list_source_from_lantern_csv.py",
            "--input_file", get_env_var(key="LANTERN_CSV_INPUT", default_value="local_data/lantern_csv/fhir_endpoints.csv"),
            "--output_file", get_env_var(key="LIST_SOURCES_SUMMARY", default_value="../npd_ehr_scrape_cache/list_sources_summary.csv")
        ]
    )
    
    # Step 2: Download service JSON files
    run_step(
        step_num=2,
        description="Downloading CEHRT JSON files",
        command_args=[
            "python", "Step20_download_list_source_json.py",
            "--input_file", get_env_var(key="LIST_SOURCES_SUMMARY", default_value="../npd_ehr_scrape_cache/list_sources_summary.csv"),
            "--output_dir", get_env_var(key="CEHRT_CACHE_DIR", default_value="../npd_ehr_scrape_cache/cache/fhir_json_cache/"),
            "--delay", get_env_var(key="DOWNLOAD_DELAY", default_value="1.0")
        ]
    )

    # Step 3: Parse FHIR bundles
    run_step(
        step_num=3,
        description="Parsing FHIR bundles into individual resource files",
        command_args=[
            "python", "Step30_parse_source_bundle.py",
            "--input_dir", get_env_var(key="SERVICE_JSON_DIR", default_value="../npd_ehr_scrape_cache/cache/fhir_json_cache/")
        ]
    )

    print("PHASE 2: DATA PROCESSING")
    print("Using new FHIR Cache Parser for all data processing")
    print("")

    # Step 4: Process FHIR cache with new parser
    cache_dir = get_env_var(key="SERVICE_JSON_DIR", default_value="../npd_ehr_scrape_cache/cache/fhir_json_cache/")
    output_dir = get_env_var(key="PARSER_OUTPUT_DIR", default_value="./parser_output")
    
    print("Step 4: Processing FHIR cache data...")
    print(f"  - Input: {cache_dir}")
    print(f"  - Output: {output_dir}")
    print("  - Generating both FHIR analysis and NPD-compliant CSV files")
    print("  - Validating NPIs with 9M+ cached entries")
    print("  - Creating complete data lineage and coverage reports")
    
    parser_command = [
        "python", "-m", "parser.cli",
        "--cache-dir", cache_dir,
        "--output-dir", output_dir
    ]
    
    # Add test mode if requested
    if os.environ.get("TEST_MODE", "").lower() in ["true", "1", "yes"]:
        parser_command.append("--test")
        print("  - TEST MODE: Processing limited data for validation")
    
    # Add verbose mode if requested  
    if os.environ.get("VERBOSE_MODE", "").lower() in ["true", "1", "yes"]:
        parser_command.append("--verbose")
        print("  - VERBOSE MODE: Detailed processing output")

    run_step(
        step_num=4,
        description="",  # Already printed above
        command_args=parser_command,
        success_message="✓ FHIR cache processing completed with new parser"
    )

    # Success summary
    print("========================================")
    print("Pipeline completed successfully!")
    print("")
    print("FHIR Cache Processing Results:")
    print(f"  - Output directory: {output_dir}/")
    print("")
    print("FHIR Analysis Files (for debugging and analysis):")
    print(f"  - {output_dir}/organization.csv - FHIR Organization resources")
    print(f"  - {output_dir}/endpoint_instance.csv - FHIR Endpoint resources")
    print(f"  - {output_dir}/endpoint_instance_to_other_id.csv - NPI validation results")
    print(f"  - {output_dir}/data_lineage.csv - Complete data traceability")
    print(f"  - {output_dir}/field_coverage_log.csv - Data processing coverage")
    print("")
    print("NPD Schema Files (PostgreSQL ready):")
    print(f"  - {output_dir}/npd_endpoint_instance.csv - Clean endpoint data")
    print(f"  - {output_dir}/npd_endpoint_instance_to_other_id.csv - Clean NPI relationships")
    print(f"  - {output_dir}/npd_endpoint_instance_to_payload.csv - Payload mappings")
    print("")
    print("Processing Report:")
    processing_report = f"{output_dir}/processing_report_*.json"
    print(f"  - {processing_report} - Detailed processing statistics")
    print("")
    print("Next Steps:")
    print(f"  - Review processing report: ls {output_dir}/processing_report_*.json")
    print(f"  - Import NPD files to PostgreSQL using schema: data_model/full_npd.sql")
    print(f"  - Analyze data coverage: cat {output_dir}/field_coverage_log.csv")
    print("")
    print("Testing Options:")
    print("  - Run with test mode: TEST_MODE=true python go.py")
    print("  - Run with verbose output: VERBOSE_MODE=true python go.py")
    print("  - Run tests: python test_parser.py")


if __name__ == "__main__":
    main()
