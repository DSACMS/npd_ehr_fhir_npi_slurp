#!/usr/bin/env python3

"""
NPI Identifier Analysis Looper

Analyzes identifier entries in FHIR JSON files that have "http://hl7.org/fhir/sid/us-npi" as the system.
- Validates NPI format (10 digits starting with '1')
- Tracks single vs multiple NPIs per record
- Provides examples of bad NPI, good single NPI, multiple NPIs, and record with most NPIs
- Uses NPI list length (not character length) for longest/shortest/random examples
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class NPILooper(EndPointLooperParent):
    """
    Child class that analyzes NPI identifiers in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the NPI analyzer with data tracking structures."""
        super().__init__()
        
        # Track NPI analysis results
        self.npi_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with NPI count and relative paths
        # Structure: {category: [(npi_values_list, npi_count, filename, relative_path), ...]}
        self.npi_examples: Dict[str, List[Tuple[List[str], int, str, str]]] = defaultdict(list)
        
        # Track files without NPI identifiers
        self.files_without_npi = 0
        
        # Track current file's relative path
        self.current_relative_path = ""
        
        # Track the record with the most NPIs
        self.max_npis_record = None
        self.max_npis_count = 0
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract and validate NPI identifier information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for identifier array under the 'resource' element
        npis_found = []
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            identifiers = resource.get('identifier', [])
            
            if isinstance(identifiers, list):
                for identifier in identifiers:
                    if isinstance(identifier, dict):
                        system = identifier.get('system')
                        value = identifier.get('value')
                        
                        if system == 'http://hl7.org/fhir/sid/us-npi' and value:
                            npis_found.append(str(value))
        
        if npis_found:
            # Validate each NPI
            valid_npis = []
            invalid_npis = []
            
            for npi in npis_found:
                if self.validate_npi(npi=npi):
                    valid_npis.append(npi)
                else:
                    invalid_npis.append(npi)
            
            # Determine category based on NPI analysis
            if len(npis_found) > 1:
                if invalid_npis:
                    category = 'multiple_npis_some_invalid'
                else:
                    category = 'multiple_npis_all_valid'
            else:  # Single NPI
                if invalid_npis:
                    category = 'single_invalid_npi'
                else:
                    category = 'single_valid_npi'
            
            # Count this category
            self.npi_counts[category] += 1
            
            # Store example (limit to prevent memory issues)
            if len(self.npi_examples[category]) < 10:
                self.npi_examples[category].append((
                    npis_found,
                    len(npis_found),
                    source_filename,
                    self.current_relative_path
                ))
            
            # Track record with most NPIs
            if len(npis_found) > self.max_npis_count:
                self.max_npis_count = len(npis_found)
                self.max_npis_record = (
                    npis_found,
                    source_filename,
                    self.current_relative_path
                )
        else:
            # Track files without NPI identifiers
            self.files_without_npi += 1
    
    def run_loop(self, *, test_mode: bool = False) -> None:
        """
        Override parent run_loop to track relative paths for web URLs.
        """
        print("Starting JSON processing...")
        print(f"Test mode: {'Enabled' if test_mode else 'Disabled'}")
        
        try:
            # Load configuration
            cache_directory = self.load_environment_config()
            print(f"Cache directory: {cache_directory}")
            
            # Discover JSON files
            print("Discovering JSON files...")
            json_files = self.discover_json_files(
                cache_directory=cache_directory,
                test_mode=test_mode
            )
            print(f"Found {self.total_files_found} JSON files to process")
            
            if not json_files:
                print("EndPointLooperParent Warning: No JSON files found to process")
                self.print_summary()
                return
            
            # Process files
            print("Processing JSON files...")
            for json_file_path in json_files:
                try:
                    # Extract relative path for web URL generation
                    cache_path = Path(cache_directory)
                    if not cache_path.exists():
                        alternative_paths = [
                            Path("../../../npd_ehr_scrape_cache/cehrt_fhir_json/"),  
                            Path("../../npd_ehr_scrape_cache/cehrt_fhir_json/"),    
                            Path("../npd_ehr_scrape_cache/cehrt_fhir_json/"),       
                            Path("npd_ehr_scrape_cache/cehrt_fhir_json/"),          
                        ]
                        for alt_path in alternative_paths:
                            if alt_path.exists():
                                cache_path = alt_path
                                break
                    
                    try:
                        relative_path = str(json_file_path.relative_to(cache_path))
                        self.current_relative_path = relative_path
                    except ValueError:
                        self.current_relative_path = f"{json_file_path.parent.name}/{json_file_path.name}"
                    
                    with open(json_file_path, 'r', encoding='utf-8') as file:
                        json_data = json.load(file)
                    
                    # Call the child's analysis method
                    self.analyze_this_json_data(
                        json_data=json_data,
                        source_filename=str(json_file_path.name)
                    )
                    
                    self.processed_count += 1
                    
                    if self.processed_count % 100 == 0:
                        print(f"Processed {self.processed_count} files...")
                        
                except (json.JSONDecodeError, UnicodeDecodeError, IOError):
                    self.failure_count += 1
                    continue
            
            print(f"\nProcessing complete: {self.processed_count} files processed, {self.failure_count} failures")
            self.print_summary()
            
        except Exception as e:
            print(f"EndPointLooperParent Error: Processing failed: {str(e)}")
            raise
    
    def _get_example_files(self, *, category: str) -> Tuple[str, str, str]:
        """
        Get three example files for a category: longest, shortest, and random by NPI count.
        
        Args:
            category: The category to get examples for
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        examples_with_counts = self.npi_examples[category]
        
        if not examples_with_counts:
            return "None", "None", "None"
        
        # Sort by NPI count to find longest and shortest
        sorted_examples = sorted(examples_with_counts, key=lambda x: x[1])
        
        # Get longest (last in sorted list)
        longest_entry = sorted_examples[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[3])
        
        # Get shortest (first in sorted list)
        shortest_entry = sorted_examples[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[3])
        
        # Get random example
        random_entry = random.choice(examples_with_counts)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[3])
        
        return longest_url, shortest_url, random_url
    
    def _get_descriptive_category_name(self, *, category: str) -> str:
        """
        Get descriptive name for NPI categories.
        
        Args:
            category: The internal category name
            
        Returns:
            Descriptive category name for display
        """
        category_descriptions = {
            'single_valid_npi': 'Single Valid NPI',
            'single_invalid_npi': 'Single Invalid NPI',
            'multiple_npis_all_valid': 'Multiple NPIs (All Valid)',
            'multiple_npis_some_invalid': 'Multiple NPIs (Some Invalid)'
        }
        
        return category_descriptions.get(category, category.replace('_', ' ').title())
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of NPI analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# NPI Identifier Analysis Summary")
        lines.append("")
        lines.append("## What This Analysis Does")
        lines.append("This analysis examines identifier entries in FHIR JSON files that have ")
        lines.append("\"http://hl7.org/fhir/sid/us-npi\" as the system. It validates NPI format (10 digits ")
        lines.append("starting with '1') and tracks single vs multiple NPIs per record. Special tracking ")
        lines.append("identifies the record with the most NPIs.")
        lines.append("")
        lines.append("- **Validation Method:** 10 digits starting with '1' (no Luhn checksum)")
        lines.append("- **Categories:** Single valid/invalid NPI, multiple NPIs (all/some valid)")
        lines.append("- **Special Features:** Tracks record with most NPIs, uses NPI count for examples")
        lines.append("- **Examples:** Longest/shortest/random by NPI list length (not character length)")
        lines.append("")
        lines.append("## Processing Results")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Files Without NPI Identifiers:** {self.files_without_npi}")
        lines.append(f"**Total NPI Categories Found:** {len(self.npi_counts)}")
        lines.append("")
        
        if not self.npi_counts:
            lines.append("No NPI identifiers found in processed files.")
        else:
            lines.append("## NPI Analysis Results")
            lines.append("")
            lines.append("| NPI Category | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|--------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.npi_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                
                # Create readable category names
                category_display = self._get_descriptive_category_name(category=category)
                
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total NPI Records Found:** {sum(self.npi_counts.values())}")
            
            # Add record with most NPIs
            if self.max_npis_record:
                lines.append("")
                lines.append("## Record with Most NPIs")
                npi_list, filename, relative_path = self.max_npis_record
                web_url = self.get_web_url_of_cache_file(relative_path=relative_path)
                lines.append(f"**File:** {web_url}")
                lines.append(f"**NPI Count:** {len(npi_list)}")
                lines.append(f"**NPIs:** {', '.join([f'`{npi}`' for npi in npi_list])}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of NPI analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=NPILooper,
        description="Analyze NPI identifiers in FHIR JSON files"
    )
