#!/usr/bin/env python3

"""
Name Field Analysis Looper

Analyzes the presence of "name" fields in FHIR JSON files.
- Categorizes files as having name field vs not having name field
- Tracks files with each status
- Provides longest/shortest/random examples based on filename length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class NameLooper(EndPointLooperParent):
    """
    Child class that analyzes name field presence in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the name field analyzer with data tracking structures."""
        super().__init__()
        
        # Track name field presence results
        self.name_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with filename lengths and relative paths
        # Structure: {category: [(filename, filename_length, relative_path), ...]}
        self.name_examples: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to check for name field presence.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for name field under the 'resource' element
        has_name = False
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            name_field = resource.get('name')
            
            # Check if name field exists and has content
            if name_field is not None:
                if isinstance(name_field, str) and name_field.strip():
                    has_name = True
                elif isinstance(name_field, list) and len(name_field) > 0:
                    has_name = True
                elif isinstance(name_field, dict) and name_field:
                    has_name = True
        
        # Categorize name field presence
        if has_name:
            category = 'has_name_field'
        else:
            category = 'no_name_field'
        
        # Count this category
        self.name_counts[category] += 1
        
        # Store example (limit to prevent memory issues)
        if len(self.name_examples[category]) < 10:
            self.name_examples[category].append((
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
        examples_with_lengths = self.name_examples[category]
        
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
    
    def _get_descriptive_category_name(self, *, category: str) -> str:
        """
        Get descriptive name for name field categories.
        
        Args:
            category: The internal category name
            
        Returns:
            Descriptive category name for display
        """
        category_descriptions = {
            'has_name_field': 'Has Name Field',
            'no_name_field': 'No Name Field'
        }
        
        return category_descriptions.get(category, category.replace('_', ' ').title())
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of name field analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Name Field Analysis Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Total Name Field Categories:** {len(self.name_counts)}")
        lines.append("")
        
        if not self.name_counts:
            lines.append("No name field data found in processed files.")
        else:
            lines.append("## Name Field Distribution")
            lines.append("")
            lines.append("| Name Field Status | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|-------------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.name_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                
                # Create readable category names
                category_display = self._get_descriptive_category_name(category=category)
                
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total Files Analyzed:** {sum(self.name_counts.values())}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of name field analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=NameLooper,
        description="Analyze name field presence in FHIR JSON files"
    )
