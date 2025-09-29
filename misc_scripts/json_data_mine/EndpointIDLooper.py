#!/usr/bin/env python3

"""
Underneath the "resource" element in FHIR json files is an "id" element that seems to come with lots of different contents. 

I would like to have a looper that categorizes the different "id" values using a series of regex matches. 

The series of regex matches will be defined in this child class of misc_scripts/json_data_mine/EndPointLooperParent.py and will include: 

* a regex if the id is a http URL
* a regex if the id is a https URL 
* e regex if the id is a http URL that uses a non-standard port (e.g., :8080)
* a regex if the id is a https URL that uses a non-standard port (e.g :8443)
* a regex if the id is a UUID (version 1)
* a regex if the id is a UUID (version 2)
* a regex if the id is a UUID (version 3)
* a regex if the id is a UUID (version 4)
* a regex if the id is a UUID (version 5)
* a regex if the id is a UUID format, but is not a valid UUID because it has some number higher than 5 in the version position
* a regex if the id is a UUID format, but is not a valid UUID because it violates some other rule (e.g., invalid characters, wrong length, etc.)
* a regex if the id is an email address
* a regex if the id is a simple alphanumeric string (e.g., "12345" or "abcde" or "A1B2C3") or other ids that have only letters, numbers and either underscore _ or hyphen - characters.
* a regex if the id is a string that contains special characters (e.g., !@#$%^&*()+=[]{}|;:'",.<>?/`~)
* a regex if the id is a string that contains spaces
* a regex if the id is a string that contains unicode characters (e.g., emojis, accented characters, non-Latin scripts, etc.)
* a regex if the id is a string that is a hexadecimal number (e.g., "1A2B3C4D" or "deadbeef")
* a regex if the id is a string that is a base64 encoded string (e.g., "SGVsbG8gd29ybGQ=")

I want a count of the above categories, and I want to see examples of each category in the markdown. 

Also include, in a bullet list in the markdown output, 100 random files that match with no regex

Please look at misc_scripts/json_data_mine/ResourceTypeLooper.py to understand how to generally structure this child class of EndPointLooperParent.py and to see how to generate the markdown output, including links to the files on GitHub.

Do not modify these instructions as you code.
"""

import json
import random
import argparse
import re
import uuid
import base64
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class EndpointIDLooper(EndPointLooperParent):
    """
    Child class that analyzes ID patterns in FHIR JSON files.
    Categorizes ID values found under the "resource" element using regex patterns.
    """
    
    def __init__(self):
        """Initialize the ID analyzer with data tracking structures."""
        super().__init__()
        
        # Track counts for each ID category
        self.id_category_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files for each category with their relative paths
        # Structure: {category: [(id_value, filename, relative_path), ...]}
        self.id_category_examples: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
        
        # Track files with IDs that don't match any regex
        self.unmatched_files: List[Tuple[str, str, str]] = []  # (id_value, filename, relative_path)
        
        # Track files without ID field
        self.files_without_id = 0
        
        # Track current file's relative path (set during processing)
        self.current_relative_path = ""
        
        # Define regex patterns for different ID categories
        self._compile_regex_patterns()
    
    def _compile_regex_patterns(self) -> None:
        """Compile all regex patterns for ID categorization."""
        self.patterns = {
            'http_url': re.compile(r'^http://[^\s:]+(?::[0-9]+)?(?:/.*)?$', re.IGNORECASE),
            'https_url': re.compile(r'^https://[^\s:]+(?::[0-9]+)?(?:/.*)?$', re.IGNORECASE),
            'http_nonstandard_port': re.compile(r'^http://[^\s:]+:(?!80(?:/|$))[0-9]+(?:/.*)?$', re.IGNORECASE),
            'https_nonstandard_port': re.compile(r'^https://[^\s:]+:(?!443(?:/|$))[0-9]+(?:/.*)?$', re.IGNORECASE),
            'uuid_v1': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v2': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-2[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v3': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-3[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v4': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v5': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_invalid_version': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[6-9a-f][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_invalid_format': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE),
            'email_address': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'simple_alphanumeric': re.compile(r'^[a-zA-Z0-9_-]+$'),
            'special_characters': re.compile(r'[!@#$%^&*()+=\[\]{}|;:\'",.<>?/`~]'),
            'contains_spaces': re.compile(r'\s'),
            'unicode_characters': re.compile(r'[^\x00-\x7F]'),
            'hexadecimal': re.compile(r'^[0-9a-fA-F]+$'),
            'base64_encoded': re.compile(r'^[A-Za-z0-9+/]*={0,2}$')
        }
    
    def _categorize_id(self, *, id_value: str) -> Optional[str]:
        """
        Categorize an ID value based on regex patterns.
        
        Args:
            id_value: The ID string to categorize
            
        Returns:
            Category name if matched, None if no match
        """
        if not id_value or not isinstance(id_value, str):
            return None
        
        # Check patterns in priority order to avoid conflicts
        
        # URLs first (more specific)
        if self.patterns['https_nonstandard_port'].match(id_value):
            return 'https_nonstandard_port'
        elif self.patterns['http_nonstandard_port'].match(id_value):
            return 'http_nonstandard_port'
        elif self.patterns['https_url'].match(id_value):
            return 'https_url'
        elif self.patterns['http_url'].match(id_value):
            return 'http_url'
        
        # UUIDs (check specific versions first, then invalid formats)
        elif self.patterns['uuid_v1'].match(id_value):
            return 'uuid_v1'
        elif self.patterns['uuid_v2'].match(id_value):
            return 'uuid_v2'
        elif self.patterns['uuid_v3'].match(id_value):
            return 'uuid_v3'
        elif self.patterns['uuid_v4'].match(id_value):
            return 'uuid_v4'
        elif self.patterns['uuid_v5'].match(id_value):
            return 'uuid_v5'
        elif self.patterns['uuid_invalid_version'].match(id_value):
            return 'uuid_invalid_version'
        elif self.patterns['uuid_invalid_format'].match(id_value) and not self._is_valid_uuid_format(id_value=id_value):
            return 'uuid_invalid_format'
        
        # Email addresses
        elif self.patterns['email_address'].match(id_value):
            return 'email_address'
        
        # Character-based patterns (check in order of specificity)
        elif self.patterns['contains_spaces'].search(id_value):
            return 'contains_spaces'
        elif self.patterns['unicode_characters'].search(id_value):
            return 'unicode_characters'
        elif self.patterns['special_characters'].search(id_value):
            return 'special_characters'
        
        # Check if it's a valid base64 string (length must be multiple of 4)
        elif len(id_value) % 4 == 0 and len(id_value) >= 4 and self.patterns['base64_encoded'].match(id_value) and self._is_likely_base64(id_value=id_value):
            return 'base64_encoded'
        
        # Hexadecimal (check after base64 to avoid conflicts)
        elif len(id_value) > 0 and self.patterns['hexadecimal'].match(id_value) and not id_value.isdigit():
            return 'hexadecimal'
        
        # Simple alphanumeric (catch-all for basic strings)
        elif self.patterns['simple_alphanumeric'].match(id_value):
            return 'simple_alphanumeric'
        
        return None
    
    def _is_valid_uuid_format(self, *, id_value: str) -> bool:
        """
        Check if a string is a properly formatted UUID.
        
        Args:
            id_value: The string to check
            
        Returns:
            True if valid UUID format, False otherwise
        """
        try:
            uuid.UUID(id_value)
            return True
        except ValueError:
            return False
    
    def _is_likely_base64(self, *, id_value: str) -> bool:
        """
        Check if a string is likely base64 encoded by trying to decode it.
        
        Args:
            id_value: The string to check
            
        Returns:
            True if likely base64, False otherwise
        """
        try:
            # Add padding if needed
            missing_padding = len(id_value) % 4
            if missing_padding:
                id_value += '=' * (4 - missing_padding)
            
            base64.b64decode(id_value)
            # Additional heuristic: base64 strings are usually longer and contain mixed case
            return len(id_value) >= 8 and any(c.islower() for c in id_value) and any(c.isupper() for c in id_value)
        except Exception:
            return False

    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract and categorize ID information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Look for ID under the 'resource' element
        id_value = None
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            id_value = json_data['resource'].get('id')
        
        if id_value:
            # Categorize the ID
            category = self._categorize_id(id_value=str(id_value))
            
            if category:
                # Count this category
                self.id_category_counts[category] += 1
                
                # Store example (limit to prevent memory issues)
                if len(self.id_category_examples[category]) < 10:
                    self.id_category_examples[category].append((
                        str(id_value),
                        source_filename,
                        self.current_relative_path
                    ))
            else:
                # Track unmatched IDs (limit to prevent memory issues)
                if len(self.unmatched_files) < 1000:
                    self.unmatched_files.append((
                        str(id_value),
                        source_filename,
                        self.current_relative_path
                    ))
        else:
            # Track files without ID field
            self.files_without_id += 1

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

    def _get_example_files(self, *, category: str, max_examples: int = 3) -> List[str]:
        """
        Get example files for a category as web URLs.
        
        Args:
            category: The category to get examples for
            max_examples: Maximum number of examples to return
            
        Returns:
            List of web URLs
        """
        examples = self.id_category_examples[category]
        
        if not examples:
            return []
        
        # Get up to max_examples, shuffle for variety
        selected_examples = random.sample(examples, min(len(examples), max_examples))
        
        # Convert to web URLs
        web_urls = []
        for id_value, filename, relative_path in selected_examples:
            web_url = self.get_web_url_of_cache_file(relative_path=relative_path)
            # Include the ID value in the display
            web_urls.append(f"{web_url} (ID: `{id_value}`)")
        
        return web_urls
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of ID analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Endpoint ID Analysis Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Files Without ID:** {self.files_without_id}")
        lines.append(f"**Total ID Categories Found:** {len(self.id_category_counts)}")
        lines.append(f"**Files with Unmatched IDs:** {len(self.unmatched_files)}")
        lines.append("")
        
        if not self.id_category_counts:
            lines.append("No categorizable IDs found in processed files.")
        else:
            lines.append("## ID Category Distribution")
            lines.append("")
            lines.append("| Category | Count | Examples |")
            lines.append("|----------|-------|----------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.id_category_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                examples = self._get_example_files(category=category, max_examples=3)
                examples_str = "<br>".join(examples) if examples else "No examples stored"
                
                # Create readable category names
                category_display = category.replace('_', ' ').title()
                
                lines.append(f"| {category_display} | {count} | {examples_str} |")
            
            lines.append("")
            lines.append(f"**Total Categorized IDs:** {sum(self.id_category_counts.values())}")
        
        # Add section for unmatched files
        if self.unmatched_files:
            lines.append("")
            lines.append("## Files with Unmatched IDs")
            lines.append("")
            lines.append("The following files contain IDs that don't match any of our regex patterns:")
            lines.append("")
            
            # Get up to 100 random unmatched files
            random_unmatched = random.sample(
                self.unmatched_files,
                min(len(self.unmatched_files), 100)
            )
            
            for id_value, filename, relative_path in random_unmatched:
                web_url = self.get_web_url_of_cache_file(relative_path=relative_path)
                lines.append(f"* {web_url} (ID: `{id_value}`)")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of ID analysis as a markdown table.
        """
        # Use the markdown generator for consistent output
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=EndpointIDLooper,
        description="Analyze ID patterns in FHIR JSON resource elements"
    )
