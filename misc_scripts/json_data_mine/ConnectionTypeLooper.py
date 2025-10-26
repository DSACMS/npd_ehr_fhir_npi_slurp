#!/usr/bin/env python3

"""
Connection Type Analysis Looper

Analyzes the "connectionType" field in FHIR JSON files.
- Tracks presence of connectionType and analyzes system/code values underneath
- Provides system and code statistics
- Provides longest/shortest/random examples based on filename length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class ConnectionTypeLooper(EndPointLooperParent):
    """
    Child class that analyzes connectionType fields in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the connection type analyzer with data tracking structures."""
        super().__init__()
        
        # Track connection type analysis results
        self.connection_type_counts: Dict[str, int] = defaultdict(int)
        
        # Track system and code statistics
        self.system_counts: Dict[str, int] = defaultdict(int)
        self.code_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with filename lengths and relative paths
        # Structure: {category: [(filename, filename_length, relative_path), ...]}
        self.connection_examples: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract connectionType information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for connectionType field under the 'resource' element
        has_connection_type = False
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            connection_type = resource.get('connectionType')
            
            if connection_type is not None:
                has_connection_type = True
                
                # Analyze connectionType structure - can be dict or list
                connection_types_to_analyze = []
                if isinstance(connection_type, dict):
                    connection_types_to_analyze.append(connection_type)
                elif isinstance(connection_type, list):
                    connection_types_to_analyze.extend([ct for ct in connection_type if isinstance(ct, dict)])
                
                # Extract system and code values
                for ct in connection_types_to_analyze:
                    system = ct.get('system')
                    code = ct.get('code')
                    
                    if system:
                        self.system_counts[str(system)] += 1
                    if code:
                        self.code_counts[str(code)] += 1
        
        # Categorize connectionType presence
        if has_connection_type:
            category = 'has_connection_type'
        else:
            category = 'no_connection_type'
        
        # Count this category
        self.connection_type_counts[category] += 1
        
        # Store example (limit to prevent memory issues)
        if len(self.connection_examples[category]) < 10:
            self.connection_examples[category].append((
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
            examples=self.connection_examples[category], 
            metric_index=1, 
            relative_path_index=2
        )
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of connectionType analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Connection Type Analysis Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append("")
        
        # ConnectionType presence
        if not self.connection_type_counts:
            lines.append("No connectionType data found in processed files.")
        else:
            lines.append("## Connection Type Presence")
            lines.append("")
            lines.append("| Category | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|----------|-------|-----------------|------------------|----------------|")
            
            for category, count in sorted(self.connection_type_counts.items()):
                longest, shortest, random_example = self._get_example_files(category=category)
                category_display = category.replace('_', ' ').title()
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
        
        # System statistics
        if self.system_counts:
            lines.append("")
            lines.append("## System Values")
            lines.append("")
            lines.append("| System | Count |")
            lines.append("|---------|-------|")
            
            # Sort by count (descending) then by system name
            sorted_systems = sorted(self.system_counts.items(), key=lambda x: (-x[1], x[0]))
            for system, count in sorted_systems:
                lines.append(f"| `{system}` | {count} |")
        
        # Code statistics
        if self.code_counts:
            lines.append("")
            lines.append("## Code Values")
            lines.append("")
            lines.append("| Code | Count |")
            lines.append("|------|-------|")
            
            # Sort by count (descending) then by code name
            sorted_codes = sorted(self.code_counts.items(), key=lambda x: (-x[1], x[0]))
            for code, count in sorted_codes:
                lines.append(f"| `{code}` | {count} |")
        
        lines.append("")
        lines.append(f"**Total Files Analyzed:** {self.processed_count}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of connectionType analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=ConnectionTypeLooper,
        description="Analyze connectionType fields in FHIR JSON files"
    )
