#!/usr/bin/env python3

"""
Address Component Analysis Looper

Analyzes address entries in FHIR JSON files for subcomponent presence.
- Tracks presence of address field and subcomponents: line, line1, line2, city, state, postalCode, country
- Calculates percentages of each subcomponent
- Provides longest/shortest/random examples based on filename length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class AddressLooper(EndPointLooperParent):
    """
    Child class that analyzes address component structure in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the address analyzer with data tracking structures."""
        super().__init__()
        
        # Track address analysis results
        self.address_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with filename lengths and relative paths
        # Structure: {category: [(filename, filename_length, relative_path), ...]}
        self.address_examples: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
        
        # Track current file's relative path
        self.current_relative_path = ""
        
        # Track all address components dynamically discovered
        self.all_address_components: Set[str] = set()
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract address component information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for address field under the 'resource' element
        has_address = False
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            address_field = resource.get('address')
            
            if address_field is not None:
                # Address can be a list or single dict
                addresses_to_analyze = []
                if isinstance(address_field, list):
                    addresses_to_analyze.extend([addr for addr in address_field if isinstance(addr, dict)])
                elif isinstance(address_field, dict):
                    addresses_to_analyze.append(address_field)
                
                if addresses_to_analyze:
                    has_address = True
                    
                    # Dynamically discover all components in all addresses
                    components_found = set()
                    for address in addresses_to_analyze:
                        for component_key, component_value in address.items():
                            if component_value:  # Only count non-empty components
                                # Add to global set of all discovered components
                                self.all_address_components.add(component_key)
                                components_found.add(component_key)
                    
                    # Count each component found
                    for component in components_found:
                        self.address_counts[f'has_{component}'] += 1
        
        # Count address presence
        if has_address:
            self.address_counts['has_address'] += 1
        else:
            self.address_counts['no_address'] += 1
        
        # Store examples for address presence
        category = 'has_address' if has_address else 'no_address'
        if len(self.address_examples[category]) < 10:
            self.address_examples[category].append((
                source_filename,
                len(source_filename),
                self.current_relative_path
            ))
    
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
        Get three example files for a category: longest, shortest, and random.
        
        Args:
            category: The category to get examples for
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        examples_with_lengths = self.address_examples[category]
        
        if not examples_with_lengths:
            return "None", "None", "None"
        
        # Sort by filename length to find longest and shortest
        sorted_examples = sorted(examples_with_lengths, key=lambda x: x[1])
        
        # Get longest (last in sorted list)
        longest_entry = sorted_examples[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[2])
        
        # Get shortest (first in sorted list)
        shortest_entry = sorted_examples[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[2])
        
        # Get random example
        random_entry = random.choice(examples_with_lengths)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[2])
        
        return longest_url, shortest_url, random_url
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of address analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Address Component Analysis Summary")
        lines.append("")
        lines.append("## What This Analysis Does")
        lines.append("This analysis examines address entries in FHIR JSON files and dynamically discovers all ")
        lines.append("address subcomponents present in the data. It tracks the presence of address fields and ")
        lines.append("calculates percentages for each subcomponent found (such as line, city, state, postalCode, country, etc.).")
        lines.append("")
        lines.append("- **Discovery Method:** Dynamic scanning of all address dictionary keys")
        lines.append("- **Components Tracked:** All subfields found in address objects (not just predefined ones)")
        lines.append("- **Percentage Calculations:** Based on files that have address fields")
        lines.append("- **Examples:** Longest/shortest/random by filename length")
        lines.append("")
        lines.append("## Processing Results")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append("")
        
        # Address presence
        total_files = self.processed_count
        has_address_count = self.address_counts.get('has_address', 0)
        no_address_count = self.address_counts.get('no_address', 0)
        
        if total_files > 0:
            has_address_pct = (has_address_count / total_files) * 100
            no_address_pct = (no_address_count / total_files) * 100
            
            lines.append("## Address Field Presence")
            lines.append("")
            lines.append("| Category | Count | Percentage | Longest Example | Shortest Example | Random Example |")
            lines.append("|----------|-------|------------|-----------------|------------------|----------------|")
            
            longest_addr, shortest_addr, random_addr = self._get_example_files(category='has_address')
            longest_no_addr, shortest_no_addr, random_no_addr = self._get_example_files(category='no_address')
            
            lines.append(f"| Has Address | {has_address_count} | {has_address_pct:.1f}% | {longest_addr} | {shortest_addr} | {random_addr} |")
            lines.append(f"| No Address | {no_address_count} | {no_address_pct:.1f}% | {longest_no_addr} | {shortest_no_addr} | {random_no_addr} |")
        
        # Component breakdown
        if has_address_count > 0:
            lines.append("")
            lines.append("## Address Component Breakdown")
            lines.append("*(Percentages are of files that have address fields)*")
            lines.append("")
            lines.append("| Component | Count | Percentage |")
            lines.append("|-----------|-------|------------|")
            
            # Sort components for consistent output
            for component in sorted(self.all_address_components):
                component_key = f'has_{component}'
                component_count = self.address_counts.get(component_key, 0)
                component_pct = (component_count / has_address_count) * 100 if has_address_count > 0 else 0
                component_display = component.replace('_', ' ').title()
                lines.append(f"| {component_display} | {component_count} | {component_pct:.1f}% |")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of address analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=AddressLooper,
        description="Analyze address component structure in FHIR JSON files"
    )
