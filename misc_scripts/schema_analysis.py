#!/usr/bin/env python3
"""
FHIR Schema Analysis Tool

Analyzes JSON files in CEHRT cache directories to understand schema variations
in real-world FHIR implementations. Creates a meta JSON structure documenting
all discovered keys and common values.

In the CEHRT_CACHE_DIR (which is defined in data_files.env)
There are a series of subdirectories which contain seperate json files that 
represent different implementations of FHIR endpoint standard. 

While there is a simple schema for this type of data, this is the "schemas in the wild" problem.
I need to know what the actual schemas are, and what actually typically appears in the values. 

I would like a script which loops over every file and creates a "meta json" file which tracks 
the different structure of the data. 

From this I would like to have json instance, that includes every actual key (not value.. 
that would be way too much data). that appears in any of the files..
Then I would like to infer a schema that generaously covers all the different structures.

I would like to to "peek" at the values to see if they are repeating. For each key in the 
structure, track the different values that appear until there are more than 10 of them. 
Create a markdown document that documents what the different structures are, and what the 
common values are when there are less than 10 of them

This is a very large data set, so lets also create a test mode that will load 2 files from 
the first 10 subdirectories to test things out. 

Also, DO NOT load the jsons files that are in the CEHRT_CACHE_DIR itself. **ONLY** Load the 
json files in the subdirectories.

Do not overwrite these instructions as you code. 
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Any, Optional, Union
from collections import defaultdict, Counter
import argparse
from dataclasses import dataclass, field


@dataclass
class ValueTracker:
    """Tracks unique values for a specific key path, limited to 10 items"""
    values: Set[str] = field(default_factory=set)
    is_truncated: bool = False
    
    def add_value(self, *, value: Any) -> None:
        """Add a value to tracking, converting to string and limiting to 10 items"""
        if len(self.values) < 10:
            # Convert value to string representation for tracking
            str_value = str(value) if value is not None else "null"
            self.values.add(str_value)
        else:
            self.is_truncated = True


class FHIRSchemaAnalyzer:
    """Analyzes FHIR JSON files to extract schema patterns and common values"""
    
    @staticmethod
    def load_environment_config() -> str:
        """Load CEHRT_CACHE_DIR from data_files.env"""
        # Check current directory first, then parent directory
        env_file_path = Path("data_files.env")
        if not env_file_path.exists():
            env_file_path = Path("../data_files.env")
        
        if not env_file_path.exists():
            raise FileNotFoundError("FHIRSchemaAnalyzer Error: data_files.env file not found in current or parent directory")
        
        with open(env_file_path, 'r') as env_file:
            for line in env_file:
                line = line.strip()
                if line.startswith('CEHRT_CACHE_DIR='):
                    cache_dir = line.split('=', 1)[1].strip()
                    return cache_dir
        
        raise ValueError("FHIRSchemaAnalyzer Error: CEHRT_CACHE_DIR not found in data_files.env")

    @staticmethod
    def discover_json_files(*, cache_directory: str, test_mode: bool = False) -> List[Path]:
        """
        Discover all JSON files in subdirectories of the cache directory
        
        Args:
            cache_directory: Path to the CEHRT cache directory
            test_mode: If True, only process 2 files from first 10 subdirectories
            
        Returns:
            List of Path objects pointing to JSON files
        """
        # Try both relative path and resolved absolute path
        cache_path = Path(cache_directory)
        if not cache_path.exists():
            # Try resolving the path
            try:
                cache_path = cache_path.resolve()
                if not cache_path.exists():
                    # Try alternative path locations
                    alt_path = Path("../../npd_ehr_scrape_cache/cehrt_fhir_json/")
                    if alt_path.exists():
                        cache_path = alt_path
                    else:
                        raise FileNotFoundError(f"FHIRSchemaAnalyzer Error: Cache directory not found at {cache_directory} or alternative locations")
            except (OSError, RuntimeError):
                raise FileNotFoundError(f"FHIRSchemaAnalyzer Error: Cache directory not accessible: {cache_directory}")
        
        json_files = []
        
        # Get all subdirectories, sorted for consistent ordering
        subdirectories = [d for d in cache_path.iterdir() if d.is_dir()]
        subdirectories.sort()
        
        if test_mode:
            subdirectories = subdirectories[:10]  # First 10 subdirectories only
        
        for subdirectory in subdirectories:
            subdir_json_files = list(subdirectory.glob("*.json"))
            
            if test_mode:
                # Take only first 2 JSON files from each subdirectory
                subdir_json_files = subdir_json_files[:2]
            
            json_files.extend(subdir_json_files)
        
        return json_files

    @staticmethod
    def extract_keys_and_values(*, json_data: Any, key_path: str = "", 
                               key_registry: Set[str], value_trackers: Dict[str, ValueTracker]) -> None:
        """
        Recursively extract all keys and track values from JSON data structure
        
        Args:
            json_data: The JSON data to analyze
            key_path: Current path in the JSON structure (for nested keys)
            key_registry: Set to store all discovered key paths
            value_trackers: Dictionary mapping key paths to ValueTracker objects
        """
        if isinstance(json_data, dict):
            for key, value in json_data.items():
                current_path = f"{key_path}.{key}" if key_path else key
                key_registry.add(current_path)
                
                # Initialize value tracker if not exists
                if current_path not in value_trackers:
                    value_trackers[current_path] = ValueTracker()
                
                # Track the value for this key
                if not isinstance(value, (dict, list)):
                    value_trackers[current_path].add_value(value=value)
                
                # Recurse into nested structures
                FHIRSchemaAnalyzer.extract_keys_and_values(
                    json_data=value,
                    key_path=current_path,
                    key_registry=key_registry,
                    value_trackers=value_trackers
                )
                
        elif isinstance(json_data, list):
            for i, item in enumerate(json_data):
                array_path = f"{key_path}[{i}]" if key_path else f"[{i}]"
                FHIRSchemaAnalyzer.extract_keys_and_values(
                    json_data=item,
                    key_path=array_path,
                    key_registry=key_registry,
                    value_trackers=value_trackers
                )

    @staticmethod
    def process_json_files(*, json_file_paths: List[Path]) -> tuple[Set[str], Dict[str, ValueTracker]]:
        """
        Process all JSON files to extract keys and track values
        
        Args:
            json_file_paths: List of paths to JSON files to process
            
        Returns:
            Tuple of (key_registry, value_trackers)
        """
        key_registry = set()
        value_trackers = {}
        
        processed_count = 0
        error_count = 0
        
        for json_file_path in json_file_paths:
            try:
                with open(json_file_path, 'r', encoding='utf-8') as file:
                    json_data = json.load(file)
                
                FHIRSchemaAnalyzer.extract_keys_and_values(
                    json_data=json_data,
                    key_registry=key_registry,
                    value_trackers=value_trackers
                )
                
                processed_count += 1
                
                if processed_count % 100 == 0:
                    print(f"Processed {processed_count} files...")
                    
            except (json.JSONDecodeError, UnicodeDecodeError, IOError) as e:
                error_count += 1
                print(f"FHIRSchemaAnalyzer Warning: Could not process {json_file_path}: {str(e)}")
                continue
        
        print(f"Processing complete: {processed_count} files processed, {error_count} errors")
        return key_registry, value_trackers

    @staticmethod
    def generate_meta_json(*, key_registry: Set[str]) -> Dict[str, Any]:
        """
        Generate a meta JSON structure containing all discovered keys
        
        Args:
            key_registry: Set of all discovered key paths
            
        Returns:
            Dictionary representing the meta JSON structure
        """
        meta_structure = {}
        
        for key_path in sorted(key_registry):
            FHIRSchemaAnalyzer._add_key_to_structure(
                structure=meta_structure,
                key_path=key_path
            )
        
        return meta_structure

    @staticmethod
    def _add_key_to_structure(*, structure: Dict[str, Any], key_path: str) -> None:
        """
        Add a key path to the meta structure
        
        Args:
            structure: The structure to add the key to
            key_path: Dot-separated key path (e.g., "resource.resourceType")
        """
        parts = key_path.split('.')
        current = structure
        
        try:
            for i, part in enumerate(parts[:-1]):
                # Handle array indices
                if '[' in part and ']' in part:
                    base_key = part.split('[')[0]
                    if base_key not in current:
                        current[base_key] = []
                    # For arrays, we use a placeholder object
                    if not current[base_key]:
                        current[base_key] = [{}]
                    current = current[base_key][0]
                else:
                    if part not in current:
                        current[part] = {}
                    elif not isinstance(current[part], dict):
                        # If it was previously set to a value, convert to dict
                        current[part] = {}
                    
                    # Ensure current is still a dict before navigating
                    if not isinstance(current, dict):
                        print(f"FHIRSchemaAnalyzer Warning: Current is not dict at part {i} '{part}' for path '{key_path}', type: {type(current)}")
                        return
                    
                    current = current[part]
            
            # Add the final key
            if not isinstance(current, dict):
                print(f"FHIRSchemaAnalyzer Warning: Cannot add final key to non-dict for path '{key_path}', type: {type(current)}")
                return
                
            final_key = parts[-1]
            if '[' in final_key and ']' in final_key:
                base_key = final_key.split('[')[0]
                if base_key not in current:
                    current[base_key] = []
                if not current[base_key]:
                    current[base_key] = ["<value>"]
            else:
                # Only set to "<value>" if it doesn't already exist or isn't a dict
                if final_key not in current or not isinstance(current[final_key], (dict, list)):
                    current[final_key] = "<value>"
                    
        except Exception as e:
            print(f"FHIRSchemaAnalyzer Warning: Error processing key path '{key_path}': {str(e)}")

    @staticmethod
    def generate_inferred_schema(*, key_registry: Set[str], value_trackers: Dict[str, ValueTracker]) -> Dict[str, Any]:
        """
        Generate an inferred JSON schema based on discovered patterns
        
        Args:
            key_registry: Set of all discovered key paths
            value_trackers: Dictionary of value trackers for each key path
            
        Returns:
            Dictionary representing the inferred schema
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "Inferred FHIR Schema from Real-World Data",
            "description": "Schema inferred from analysis of actual FHIR endpoint implementations",
            "properties": {},
            "additionalProperties": True
        }
        
        # Group keys by their root level
        root_properties = defaultdict(set)
        for key_path in key_registry:
            root_key = key_path.split('.')[0].split('[')[0]
            root_properties[root_key].add(key_path)
        
        for root_key, paths in root_properties.items():
            schema["properties"][root_key] = {
                "type": ["object", "array", "string", "number", "boolean", "null"],
                "description": f"Property with {len(paths)} discovered key variations"
            }
        
        return schema

    @staticmethod
    def generate_markdown_report(*, key_registry: Set[str], value_trackers: Dict[str, ValueTracker], 
                               total_files_processed: int, test_mode: bool) -> str:
        """
        Generate a markdown report documenting the schema analysis results
        
        Args:
            key_registry: Set of all discovered key paths
            value_trackers: Dictionary of value trackers for each key path
            total_files_processed: Number of files that were processed
            test_mode: Whether analysis was run in test mode
            
        Returns:
            String containing the markdown report
        """
        report_lines = [
            "# FHIR Schema Analysis Report",
            "",
            f"**Analysis Date:** {FHIRSchemaAnalyzer._get_current_timestamp()}",
            f"**Files Processed:** {total_files_processed}",
            f"**Test Mode:** {'Yes' if test_mode else 'No'}",
            f"**Unique Key Paths Discovered:** {len(key_registry)}",
            "",
            "## Executive Summary",
            "",
            "This report analyzes the structure of FHIR JSON files from real-world healthcare",
            "implementations to understand schema variations and common value patterns.",
            "",
            "## Key Structure Analysis",
            ""
        ]
        
        # Group keys by their structure levels
        structure_groups = FHIRSchemaAnalyzer._group_keys_by_structure(key_registry=key_registry)
        
        for level, keys in structure_groups.items():
            report_lines.append(f"### {level} Level Keys ({len(keys)} keys)")
            report_lines.append("")
            
            for key in sorted(keys)[:20]:  # Show first 20 keys per level
                report_lines.append(f"- `{key}`")
                
                # Add value information if available and limited
                if key in value_trackers and value_trackers[key].values:
                    tracker = value_trackers[key]
                    if len(tracker.values) <= 10:
                        values_str = ", ".join(f'"{v}"' for v in sorted(tracker.values))
                        report_lines.append(f"  - **Common values:** {values_str}")
                    else:
                        report_lines.append(f"  - **Values:** More than 10 unique values found")
                    
                    if tracker.is_truncated:
                        report_lines.append(f"  - **Note:** Additional values exist (truncated at 10)")
            
            if len(keys) > 20:
                report_lines.append(f"- ... and {len(keys) - 20} more keys")
            
            report_lines.append("")
        
        # Add common patterns section
        report_lines.extend([
            "## Common Value Patterns",
            "",
            "Keys with limited value sets (≤10 unique values):",
            ""
        ])
        
        limited_value_keys = {k: v for k, v in value_trackers.items() 
                            if v.values and len(v.values) <= 10}
        
        for key_path in sorted(limited_value_keys.keys()):
            tracker = limited_value_keys[key_path]
            values_str = ", ".join(f'`{v}`' for v in sorted(tracker.values))
            report_lines.append(f"**{key_path}:**")
            report_lines.append(f"- Values: {values_str}")
            report_lines.append("")
        
        return "\n".join(report_lines)

    @staticmethod
    def _group_keys_by_structure(*, key_registry: Set[str]) -> Dict[str, List[str]]:
        """Group keys by their structural level (root, nested, etc.)"""
        groups = {
            "Root": [],
            "First Level": [],
            "Deep Nested": [],
            "Array Elements": []
        }
        
        for key_path in key_registry:
            if '[' in key_path:
                groups["Array Elements"].append(key_path)
            elif '.' not in key_path:
                groups["Root"].append(key_path)
            elif key_path.count('.') == 1:
                groups["First Level"].append(key_path)
            else:
                groups["Deep Nested"].append(key_path)
        
        return groups

    @staticmethod
    def _get_current_timestamp() -> str:
        """Get current timestamp in readable format"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def save_results(*, meta_json: Dict[str, Any], inferred_schema: Dict[str, Any], 
                    markdown_report: str, output_directory: str = "local_data") -> None:
        """
        Save all analysis results to files
        
        Args:
            meta_json: The meta JSON structure
            inferred_schema: The inferred schema
            markdown_report: The markdown report
            output_directory: Directory to save results in
        """
        output_path = Path(output_directory)
        output_path.mkdir(exist_ok=True)
        
        # Save meta JSON
        meta_json_path = output_path / "fhir_meta_structure.json"
        with open(meta_json_path, 'w', encoding='utf-8') as f:
            json.dump(meta_json, f, indent=2, ensure_ascii=False)
        print(f"Meta JSON saved to: {meta_json_path}")
        
        # Save inferred schema
        schema_path = output_path / "fhir_inferred_schema.json"
        with open(schema_path, 'w', encoding='utf-8') as f:
            json.dump(inferred_schema, f, indent=2, ensure_ascii=False)
        print(f"Inferred schema saved to: {schema_path}")
        
        # Save markdown report
        report_path = output_path / "fhir_schema_analysis_report.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(markdown_report)
        print(f"Analysis report saved to: {report_path}")

    @staticmethod
    def run_analysis(*, test_mode: bool = False) -> None:
        """
        Main method to run the complete FHIR schema analysis
        
        Args:
            test_mode: If True, runs in test mode (2 files from first 10 subdirs)
        """
        print("Starting FHIR Schema Analysis...")
        print(f"Test mode: {'Enabled' if test_mode else 'Disabled'}")
        
        try:
            # Load configuration
            cache_directory = FHIRSchemaAnalyzer.load_environment_config()
            print(f"Cache directory: {cache_directory}")
            
            # Discover JSON files
            print("Discovering JSON files...")
            json_files = FHIRSchemaAnalyzer.discover_json_files(
                cache_directory=cache_directory,
                test_mode=test_mode
            )
            print(f"Found {len(json_files)} JSON files to process")
            
            if not json_files:
                print("FHIRSchemaAnalyzer Error: No JSON files found to process")
                return
            
            # Process files
            print("Processing JSON files...")
            key_registry, value_trackers = FHIRSchemaAnalyzer.process_json_files(
                json_file_paths=json_files
            )
            
            # Generate outputs
            print("Generating meta JSON structure...")
            meta_json = FHIRSchemaAnalyzer.generate_meta_json(key_registry=key_registry)
            
            print("Generating inferred schema...")
            inferred_schema = FHIRSchemaAnalyzer.generate_inferred_schema(
                key_registry=key_registry,
                value_trackers=value_trackers
            )
            
            print("Generating markdown report...")
            markdown_report = FHIRSchemaAnalyzer.generate_markdown_report(
                key_registry=key_registry,
                value_trackers=value_trackers,
                total_files_processed=len(json_files),
                test_mode=test_mode
            )
            
            # Save results
            print("Saving results...")
            FHIRSchemaAnalyzer.save_results(
                meta_json=meta_json,
                inferred_schema=inferred_schema,
                markdown_report=markdown_report
            )
            
            print("\nAnalysis complete!")
            print(f"Discovered {len(key_registry)} unique key paths")
            print(f"Tracked values for {len(value_trackers)} keys")
            
        except Exception as e:
            print(f"FHIRSchemaAnalyzer Error: Analysis failed: {str(e)}")
            raise


def main():
    """Command-line interface for the FHIR Schema Analyzer"""
    parser = argparse.ArgumentParser(
        description="Analyze FHIR JSON files to understand schema variations"
    )
    parser.add_argument(
        '--test-mode', 
        action='store_true',
        help="Run in test mode (process 2 files from first 10 subdirectories)"
    )
    
    args = parser.parse_args()
    
    try:
        FHIRSchemaAnalyzer.run_analysis(test_mode=args.test_mode)
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Analysis failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
