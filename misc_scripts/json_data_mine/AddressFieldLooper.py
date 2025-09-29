#!/usr/bin/env python3

"""
Address Field Classification Looper

Analyzes address field contents using regex classification from parent class.
- Uses classify_address_content static function to categorize address values
- Tracks address field presence and classification results
- Provides longest/shortest/random examples based on filename length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class AddressFieldLooper(EndPointLooperParent):
    """
    Child class that classifies address field contents in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the address field classifier with data tracking structures."""
        super().__init__()
        
        # Track address classification results
        self.address_classification_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with filename lengths and relative paths
        # Structure: {category: [(address_value, filename_length, filename, relative_path), ...]}
        self.address_examples: Dict[str, List[Tuple[str, int, str, str]]] = defaultdict(list)
        
        # Track files without address fields
        self.files_without_address = 0
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to classify address field contents.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for address field under the 'resource' element
        addresses_found = []
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            address_field = resource.get('address')
            
            if address_field is not None:
                # Address can be a list or single dict
                if isinstance(address_field, list):
                    for addr in address_field:
                        if isinstance(addr, dict):
                            # Extract address components and classify them
                            for key, value in addr.items():
                                if isinstance(value, str) and value.strip():
                                    addresses_found.append(value.strip())
                                elif isinstance(value, list):
                                    for item in value:
                                        if isinstance(item, str) and item.strip():
                                            addresses_found.append(item.strip())
                elif isinstance(address_field, dict):
                    # Extract address components and classify them
                    for key, value in address_field.items():
                        if isinstance(value, str) and value.strip():
                            addresses_found.append(value.strip())
                        elif isinstance(value, list):
                            for item in value:
                                if isinstance(item, str) and item.strip():
                                    addresses_found.append(item.strip())
        
        if addresses_found:
            for address_value in addresses_found:
                # Classify using parent class method
                classification = self.classify_address_content(content=address_value)
                
                if classification:
                    category = classification
                else:
                    category = 'unclassified'
                
                # Count this category
                self.address_classification_counts[category] += 1
                
                # Store example (limit to prevent memory issues)
                if len(self.address_examples[category]) < 10:
                    self.address_examples[category].append((
                        address_value,
                        len(source_filename),
                        source_filename,
                        self.current_relative_path
                    ))
        else:
            # Track files without address fields
            self.files_without_address += 1
    
    def run_loop(self, *, test_mode: bool = False) -> None:
        """
        Override parent run_loop to track relative paths for web URLs.
        """
        print("Starting JSON processing...")
        print(f"Test mode: {'Enabled' if test_mode else 'Disabled'}")
        
        try:
            # Load configuration and process files (similar to other loopers)
            cache_directory = self.load_environment_config()
            print(f"Cache directory: {cache_directory}")
            
            json_files = self.discover_json_files(
                cache_directory=cache_directory,
                test_mode=test_mode
            )
            print(f"Found {self.total_files_found} JSON files to process")
            
            if not json_files:
                print("EndPointLooperParent Warning: No JSON files found to process")
                self.print_summary()
                return
            
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
        """
        examples_with_lengths = self.address_examples[category]
        
        if not examples_with_lengths:
            return "None", "None", "None"
        
        # Sort by filename length
        sorted_examples = sorted(examples_with_lengths, key=lambda x: x[1])
        
        longest_entry = sorted_examples[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[3])
        
        shortest_entry = sorted_examples[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[3])
        
        random_entry = random.choice(examples_with_lengths)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[3])
        
        return longest_url, shortest_url, random_url
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of address classification as markdown string.
        """
        lines = []
        lines.append("# Address Field Classification Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Files Without Address Fields:** {self.files_without_address}")
        lines.append(f"**Total Classification Categories:** {len(self.address_classification_counts)}")
        lines.append("")
        
        if not self.address_classification_counts:
            lines.append("No address field content found in processed files.")
        else:
            lines.append("## Address Classification Results")
            lines.append("")
            lines.append("| Classification | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|----------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.address_classification_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                category_display = category.replace('_', ' ').title()
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total Address Components Classified:** {sum(self.address_classification_counts.values())}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of address classification.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=AddressFieldLooper,
        description="Classify address field contents in FHIR JSON files"
    )
