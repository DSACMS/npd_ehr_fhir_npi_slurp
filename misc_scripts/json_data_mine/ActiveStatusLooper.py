#!/usr/bin/env python3

"""
Active Status Analysis Looper

Analyzes the "active" field in FHIR JSON files.
- Categorizes as "active": true, "active": false, or no "active" field
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


class ActiveStatusLooper(EndPointLooperParent):
    """
    Child class that analyzes active status in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the active status analyzer with data tracking structures."""
        super().__init__()
        
        # Track active status results
        self.active_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with filename lengths and relative paths
        # Structure: {category: [(filename, filename_length, relative_path), ...]}
        self.active_examples: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract active status information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for active field under the 'resource' element
        active_value = None
        category = None
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            active_value = resource.get('active')
        
        # Categorize active status
        if active_value is True:
            category = 'active_true'
        elif active_value is False:
            category = 'active_false'
        else:
            category = 'no_active_field'
        
        # Count this category
        self.active_counts[category] += 1
        
        # Store example (limit to prevent memory issues)
        if len(self.active_examples[category]) < 10:
            self.active_examples[category].append((
                source_filename,
                len(source_filename),
                self.current_relative_path
            ))
    
    
    def _get_example_files(self, *, category: str) -> Tuple[str, str, str]:
        """
        Get three example files for a category: longest, shortest, and random.
        
        Args:
            category: The category to get examples for
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        # Use the parent's generic method
        # Tuple structure: (filename, filename_length, relative_path)
        # metric_index=1 (filename_length), relative_path_index=2
        return self.get_example_files_by_metric(
            examples=self.active_examples[category], 
            metric_index=1, 
            relative_path_index=2
        )
    
    def _get_descriptive_category_name(self, *, category: str) -> str:
        """
        Get descriptive name for active status categories.
        
        Args:
            category: The internal category name
            
        Returns:
            Descriptive category name for display
        """
        category_descriptions = {
            'active_true': 'Active: True',
            'active_false': 'Active: False',
            'no_active_field': 'No Active Field'
        }
        
        return category_descriptions.get(category, category.replace('_', ' ').title())
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of active status analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Active Status Analysis Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Total Active Status Categories:** {len(self.active_counts)}")
        lines.append("")
        
        if not self.active_counts:
            lines.append("No active status data found in processed files.")
        else:
            lines.append("## Active Status Distribution")
            lines.append("")
            lines.append("| Active Status | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|---------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.active_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                
                # Create readable category names
                category_display = self._get_descriptive_category_name(category=category)
                
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total Files Analyzed:** {sum(self.active_counts.values())}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of active status analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=ActiveStatusLooper,
        description="Analyze active status fields in FHIR JSON files"
    )
