"""
This is a resource looper misc_scripts/json_data_mine/EndPointLooperParent.py

Whose sole purpose is to loop over the resourceType json files in the CEHRT cache directory
And count how many of each resourceType there are, and print the summary out as a markdown table with 
resourceType, count and a list of three example files for each resourceType. The first example file should be the one that is the longest by character count, the second should be the one that is the shortest by character count and the third should be a random example file.



"""

#!/usr/bin/env python3

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class ResourceTypeLooper(EndPointLooperParent):
    """
    Child class that analyzes resourceType distribution in FHIR JSON files.
    Counts occurrences of each resourceType and tracks example files by length.
    """
    
    def __init__(self):
        """Initialize the ResourceType analyzer with data tracking structures."""
        super().__init__()
        
        # Track resourceType counts
        self.resource_type_counts: Dict[str, int] = defaultdict(int)
        
        # Track files for each resourceType with their character lengths and relative paths
        # Structure: {resourceType: [(filename, char_length, relative_path), ...]}
        self.resource_type_files: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
        
        # Track files without resourceType
        self.files_without_resource_type = 0
        
        # Track current file's relative path (set during processing)
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract and count resourceType information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Calculate character length of the JSON data
        json_str = str(json_data)
        char_length = len(json_str)
        
        # Extract resourceType from the JSON data - check both top level and nested under 'resource'
        resource_type = json_data.get('resourceType')
        
        # If not found at top level, check if it's nested under 'resource' key
        if not resource_type and 'resource' in json_data:
            resource_type = json_data['resource'].get('resourceType')
        
        if resource_type:
            # Count this resourceType occurrence
            self.resource_type_counts[resource_type] += 1
            
            # Track this file with its length and relative path for this resourceType
            self.resource_type_files[resource_type].append((source_filename, char_length, self.current_relative_path))
        else:
            # Track files that don't have a resourceType
            self.files_without_resource_type += 1
    
    def run_loop(self, *, test_mode: bool = False) -> None:
        """
        Override parent run_loop to track relative paths for web URLs.
        
        Args:
            test_mode: If True, runs in test mode (4 files from 10 random subdirs)
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
                    # Example: /path/to/cache/subdir/file.json -> subdir/file.json
                    # Get the cache directory path used for discovery
                    cache_directory = self.load_environment_config()
                    cache_path = Path(cache_directory)
                    if not cache_path.exists():
                        # Use the same logic as discover_json_files
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
                        # Fallback if relative path calculation fails
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
                    # Skip silently but track failure
                    self.failure_count += 1
                    continue
            
            print(f"\nProcessing complete: {self.processed_count} files processed, {self.failure_count} failures")
            
            # Always call print_summary at the end
            self.print_summary()
            
        except Exception as e:
            print(f"EndPointLooperParent Error: Processing failed: {str(e)}")
            raise

    def _get_example_files(self, *, resource_type: str) -> Tuple[str, str, str]:
        """
        Get three example files for a resourceType as web URLs: longest, shortest, and random.
        
        Args:
            resource_type: The resourceType to get examples for
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        files_with_lengths_paths = self.resource_type_files[resource_type]
        
        if not files_with_lengths_paths:
            return "None", "None", "None"
        
        # Sort by character length to find longest and shortest
        sorted_files = sorted(files_with_lengths_paths, key=lambda x: x[1])
        
        # Get longest (last in sorted list) - (filename, length, relative_path)
        longest_entry = sorted_files[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[2])
        
        # Get shortest (first in sorted list)
        shortest_entry = sorted_files[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[2])
        
        # Get random file
        random_entry = random.choice(files_with_lengths_paths)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[2])
        
        return longest_url, shortest_url, random_url
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of resourceType analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Resource Type Analysis Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Files Without resourceType:** {self.files_without_resource_type}")
        lines.append(f"**Total Resource Types Found:** {len(self.resource_type_counts)}")
        lines.append("")
        
        if not self.resource_type_counts:
            lines.append("No resourceTypes found in processed files.")
            return "\n".join(lines)
        
        lines.append("## Resource Type Distribution")
        lines.append("")
        lines.append("| Resource Type | Count | Longest Example | Shortest Example | Random Example |")
        lines.append("|---------------|-------|-----------------|------------------|----------------|")
        
        # Sort by count (descending) then by resourceType name
        sorted_resource_types = sorted(
            self.resource_type_counts.items(),
            key=lambda x: (-x[1], x[0])  # First by count desc, then by name asc
        )
        
        for resource_type, count in sorted_resource_types:
            longest, shortest, random_example = self._get_example_files(resource_type=resource_type)
            
            lines.append(f"| `{resource_type}` | {count} | {longest} | {shortest} | {random_example} |")
        
        lines.append("")
        lines.append(f"**Total Files Analyzed:** {sum(self.resource_type_counts.values())}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of resourceType analysis as a markdown table.
        """
        # Use the markdown generator for consistent output
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=ResourceTypeLooper,
        description="Analyze resourceType distribution in FHIR JSON files"
    )
