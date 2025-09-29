#!/usr/bin/env python3

"""
Telecom Phone Analysis Looper

Analyzes telecom entries in FHIR JSON files that have "phone" as the system.
- Validates phone numbers (10-11 digits total)
- Categorizes as valid 10-digit, valid 11-digit, or invalid
- Tracks files with no phone telecoms
- Provides longest/shortest/random examples based on phone number character length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class TelecomPhoneLooper(EndPointLooperParent):
    """
    Child class that analyzes telecom phone entries in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the telecom phone analyzer with data tracking structures."""
        super().__init__()
        
        # Track phone validation results
        self.phone_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with phone lengths and relative paths
        # Structure: {category: [(phone_value, phone_length, filename, relative_path), ...]}
        self.phone_examples: Dict[str, List[Tuple[str, int, str, str]]] = defaultdict(list)
        
        # Track files without phone telecoms
        self.files_without_phone = 0
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract and validate telecom phone information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for telecom array under the 'resource' element
        phones_found = []
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            telecoms = resource.get('telecom', [])
            
            if isinstance(telecoms, list):
                for telecom in telecoms:
                    if isinstance(telecom, dict):
                        system = telecom.get('system')
                        value = telecom.get('value')
                        
                        if system == 'phone' and value:
                            phones_found.append(str(value))
        
        if phones_found:
            for phone in phones_found:
                # Validate phone using parent class method
                validation_result = self.validate_phone(phone=phone)
                
                # Handle None case (shouldn't happen but for type safety)
                if validation_result is None:
                    validation_result = 'invalid'
                
                # Count this category
                self.phone_counts[validation_result] += 1
                
                # Store example (limit to prevent memory issues)
                if len(self.phone_examples[validation_result]) < 10:
                    self.phone_examples[validation_result].append((
                        phone,
                        len(phone),
                        source_filename,
                        self.current_relative_path
                    ))
        else:
            # Track files without phone telecoms
            self.files_without_phone += 1
    
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
        examples_with_lengths = self.phone_examples[category]
        
        if not examples_with_lengths:
            return "None", "None", "None"
        
        # Sort by phone length to find longest and shortest
        sorted_examples = sorted(examples_with_lengths, key=lambda x: x[1])
        
        # Get longest (last in sorted list)
        longest_entry = sorted_examples[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[3])
        
        # Get shortest (first in sorted list)
        shortest_entry = sorted_examples[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[3])
        
        # Get random example
        random_entry = random.choice(examples_with_lengths)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[3])
        
        return longest_url, shortest_url, random_url
    
    def _get_descriptive_category_name(self, *, category: str) -> str:
        """
        Get descriptive name for phone validation categories.
        
        Args:
            category: The internal category name
            
        Returns:
            Descriptive category name for display
        """
        category_descriptions = {
            'valid_10_digit': 'Valid 10-Digit Phone',
            'valid_11_digit': 'Valid 11-Digit Phone',
            'invalid': 'Invalid Phone Format'
        }
        
        return category_descriptions.get(category, category.replace('_', ' ').title())
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of telecom phone analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Telecom Phone Analysis Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Files Without Phone Telecoms:** {self.files_without_phone}")
        lines.append(f"**Total Phone Categories Found:** {len(self.phone_counts)}")
        lines.append("")
        
        if not self.phone_counts:
            lines.append("No telecom phone entries found in processed files.")
        else:
            lines.append("## Phone Validation Results")
            lines.append("")
            lines.append("| Phone Category | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|----------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.phone_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                
                # Create readable category names
                category_display = self._get_descriptive_category_name(category=category)
                
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total Phone Telecoms Found:** {sum(self.phone_counts.values())}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of telecom phone analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=TelecomPhoneLooper,
        description="Analyze telecom phone entries in FHIR JSON files"
    )
