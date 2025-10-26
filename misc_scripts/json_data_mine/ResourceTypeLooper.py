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
    

    def _get_example_files(self, *, resource_type: str) -> Tuple[str, str, str]:
        """
        Get three example files for a resourceType as web URLs: longest, shortest, and random.
        
        Args:
            resource_type: The resourceType to get examples for
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        # Use the parent's generic method
        # Tuple structure: (source_filename, char_length, relative_path)
        # metric_index=1 (char_length), relative_path_index=2
        return self.get_example_files_by_metric(
            examples=self.resource_type_files[resource_type], 
            metric_index=1, 
            relative_path_index=2
        )
    
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
