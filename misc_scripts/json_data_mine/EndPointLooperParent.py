"""
I would like to have a series of classes that loop over the Endpoint JSON cache in the the manner that misc_scripts/schema_analysis.py does, but with a more general purpose design.

I would like to have the logic which finds and loops over the json be held in a parent class and have an abstract function on this class for 
children to implement with their own logic. That function "analyze_this_json_data" should get the json data as a dict, and the name of the source file as a string.

there should be a tail __main__ section that runs the loop function on the parent class (which will call analyze_this_json_data over and over again)

Then there should be a second function, which is always called at the end of the loop called "print_summary" which children can implement to print out a summary of their findings.

Lets implement a test-mode for this program, where if you run it with the argument --test-mode it will process 4 files each from 10 random subdirectories

When a JSON file fails to load, should the parent class: Should Skip it silently and continue. But always produce a summary of how many failures to load JSON files there were.

In this case, it does not make sense to use static functions. analyze_this_json_data shuould be able accrue data in local variables in order to analyze the data later using the print_summary function, which must also have access to those local variables.
Do not over-write this comment as you implement this program. 

TODO add a static function to the parent class called "get_web_url_of_cache_file" that accepts a filename and returns the link to our online scrape cache. 
The local directory name corresponds to the name of the git project.. so the subdirectory of 

../npd_ehr_scrape_cache/cehrt_fhir_json/citiustech_inc_dddab3b714c651b71131540f5d1afbaf/entry_Endpoint-2.json

Should return the web url of

https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/citiustech_inc_dddab3b714c651b71131540f5d1afbaf/entry_Endpoint-2.json

This will be use to create markdown links to files in the summary printouts. 

In fact, go ahead and have the function return the url as a markdown link with the filename as the link text, 


"""

#!/usr/bin/env python3

import json
import os
import sys
import random
import argparse
import re
import uuid
import base64
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from abc import ABC, abstractmethod


class EndPointLooperParent(ABC):
    """
    Abstract parent class for processing JSON files in the CEHRT cache directory.
    Children should implement analyze_this_json_data and print_summary methods.
    """
    
    def __init__(self):
        """Initialize the parent class with tracking variables."""
        self.failure_count = 0
        self.processed_count = 0
        self.total_files_found = 0
    
    @staticmethod
    def classify_id_content(*, content: str) -> Optional[str]:
        """
        Classify ID/URL content using regex patterns from EndpointIDLooper.
        
        Args:
            content: The content string to classify
            
        Returns:
            Category name if matched, None if no match
        """
        if not content or not isinstance(content, str):
            return None
        
        # Define regex patterns for ID classification
        patterns = {
            'test_related': re.compile(r'^test$|^test.*|.*test$', re.IGNORECASE),
            'http_url': re.compile(r'^http://[^\s:]+(?::[0-9]+)?(?:/.*)?$', re.IGNORECASE),
            'https_url': re.compile(r'^https://[^\s:]+(?::[0-9]+)?(?:/.*)?$', re.IGNORECASE),
            'http_nonstandard_port': re.compile(r'^http://[^\s:]+:(?!80(?:/|$))[0-9]+(?:/.*)?$', re.IGNORECASE),
            'https_nonstandard_port': re.compile(r'^https://[^\s:]+:(?!443(?:/|$))[0-9]+(?:/.*)?$', re.IGNORECASE),
            'uuid_v1': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v2': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-2[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v3': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-3[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v4': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v5': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v6': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-6[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v7': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_v8': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_invalid_version': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[9a-f][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE),
            'uuid_invalid_format': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE),
            'email_address': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'simple_alphanumeric': re.compile(r'^[a-zA-Z0-9_-]+$'),
            'special_characters': re.compile(r'[!@#$%^&*()+=\[\]{}|;:\'",.<>?/`~]'),
            'contains_spaces': re.compile(r'\s'),
            'unicode_characters': re.compile(r'[^\x00-\x7F]'),
            'hexadecimal': re.compile(r'^[0-9a-fA-F]+$'),
            'base64_encoded': re.compile(r'^[A-Za-z0-9+/]*={0,2}$')
        }
        
        # Helper functions
        def _is_valid_uuid_format(value):
            try:
                uuid.UUID(value)
                return True
            except ValueError:
                return False
        
        def _is_likely_base64(value):
            try:
                missing_padding = len(value) % 4
                if missing_padding:
                    value += '=' * (4 - missing_padding)
                base64.b64decode(value)
                return len(value) >= 8 and any(c.islower() for c in value) and any(c.isupper() for c in value)
            except Exception:
                return False
        
        # Check patterns in priority order
        if patterns['test_related'].match(content):
            return 'test_related'
        elif patterns['https_nonstandard_port'].match(content):
            return 'https_nonstandard_port'
        elif patterns['http_nonstandard_port'].match(content):
            return 'http_nonstandard_port'
        elif patterns['https_url'].match(content):
            return 'https_url'
        elif patterns['http_url'].match(content):
            return 'http_url'
        elif patterns['uuid_v1'].match(content):
            return 'uuid_v1'
        elif patterns['uuid_v2'].match(content):
            return 'uuid_v2'
        elif patterns['uuid_v3'].match(content):
            return 'uuid_v3'
        elif patterns['uuid_v4'].match(content):
            return 'uuid_v4'
        elif patterns['uuid_v5'].match(content):
            return 'uuid_v5'
        elif patterns['uuid_v6'].match(content):
            return 'uuid_v6'
        elif patterns['uuid_v7'].match(content):
            return 'uuid_v7'
        elif patterns['uuid_v8'].match(content):
            return 'uuid_v8'
        elif patterns['uuid_invalid_version'].match(content):
            return 'uuid_invalid_version'
        elif patterns['uuid_invalid_format'].match(content) and not _is_valid_uuid_format(content):
            return 'uuid_invalid_format'
        elif patterns['email_address'].match(content):
            return 'email_address'
        elif patterns['contains_spaces'].search(content):
            return 'contains_spaces'
        elif patterns['unicode_characters'].search(content):
            return 'unicode_characters'
        elif patterns['special_characters'].search(content):
            return 'special_characters'
        elif len(content) % 4 == 0 and len(content) >= 4 and patterns['base64_encoded'].match(content) and _is_likely_base64(content):
            return 'base64_encoded'
        elif len(content) > 0 and patterns['hexadecimal'].match(content) and not content.isdigit():
            return 'hexadecimal'
        elif patterns['simple_alphanumeric'].match(content):
            return 'simple_alphanumeric'
        
        return None
    
    @staticmethod
    def validate_email(*, email: str) -> bool:
        """
        Validate email address using regex.
        
        Args:
            email: Email address to validate
            
        Returns:
            True if valid email format, False otherwise
        """
        if not email or not isinstance(email, str):
            return False
        
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        return bool(email_pattern.match(email))
    
    @staticmethod
    def validate_phone(*, phone: str) -> Optional[str]:
        """
        Validate phone number format (10-11 digits total).
        
        Args:
            phone: Phone number to validate
            
        Returns:
            'valid_10_digit', 'valid_11_digit', or 'invalid'
        """
        if not phone or not isinstance(phone, str):
            return 'invalid'
        
        # Extract only digits
        digits_only = re.sub(r'[^\d]', '', phone)
        
        if len(digits_only) == 10:
            return 'valid_10_digit'
        elif len(digits_only) == 11:
            return 'valid_11_digit'
        else:
            return 'invalid'
    
    @staticmethod
    def validate_npi(*, npi: str) -> bool:
        """
        Validate NPI format (10 digits starting with '1').
        
        Args:
            npi: NPI to validate
            
        Returns:
            True if valid NPI format, False otherwise
        """
        if not npi or not isinstance(npi, str):
            return False
        
        # Remove any non-digits
        digits_only = re.sub(r'[^\d]', '', npi)
        
        # Check if it's 10 digits and starts with '1'
        return len(digits_only) == 10 and digits_only.startswith('1')
    
    @staticmethod
    def classify_address_content(*, content: str) -> Optional[str]:
        """
        Classify address content using patterns specific to addresses.
        
        Args:
            content: The address content string to classify
            
        Returns:
            Category name if matched, None if no match
        """
        if not content or not isinstance(content, str):
            return None
        
        address_patterns = {
            'po_box': re.compile(r'^(po|p\.o\.?|post office)\s*box\s*\d+', re.IGNORECASE),
            'suite_unit': re.compile(r'\b(suite|ste|unit|apt|apartment|#)\s*\w+', re.IGNORECASE),
            'street_number': re.compile(r'^\d+\s+[a-zA-Z]', re.IGNORECASE),
            'directional': re.compile(r'\b(north|south|east|west|n|s|e|w|ne|nw|se|sw|northeast|northwest|southeast|southwest)\b', re.IGNORECASE),
            'street_type': re.compile(r'\b(street|st|avenue|ave|road|rd|drive|dr|lane|ln|boulevard|blvd|court|ct|place|pl|circle|cir|way)\b', re.IGNORECASE),
            'zip_code': re.compile(r'\b\d{5}(-\d{4})?\b'),
            'international': re.compile(r'[^\x00-\x7F]'),  # Non-ASCII characters
            'simple_text': re.compile(r'^[a-zA-Z0-9\s,.-]+$')
        }
        
        # Check patterns in priority order
        if address_patterns['po_box'].search(content):
            return 'po_box'
        elif address_patterns['suite_unit'].search(content):
            return 'suite_unit'
        elif address_patterns['street_number'].match(content):
            return 'street_number'
        elif address_patterns['zip_code'].search(content):
            return 'zip_code'
        elif address_patterns['directional'].search(content):
            return 'directional'
        elif address_patterns['street_type'].search(content):
            return 'street_type'
        elif address_patterns['international'].search(content):
            return 'international'
        elif address_patterns['simple_text'].match(content):
            return 'simple_text'
        
        return 'unclassified'
    
    @staticmethod
    def get_web_url_of_cache_file(*, relative_path: str) -> str:
        """
        Convert a cache file path to a GitHub URL with HTML link.
        
        Args:
            relative_path: Path relative to the cache directory, including subdirectory and filename
                          Example: "citiustech_inc_dddab3b714c651b71131540f5d1afbaf/entry_Endpoint-2.json"
        
        Returns:
            Markdown link
        """
        # Base GitHub URL for the scrape cache repository
        base_github_url = "https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json"
        
        # Extract just the filename for the link text
        filename = Path(relative_path).name
        
        # Construct the full GitHub URL
        github_url = f"{base_github_url}/{relative_path}"
        
        # Return aa standard markdown link
        return f'[{filename}]({github_url})'

    @staticmethod
    def load_environment_config() -> str:
        """Load CEHRT_CACHE_DIR from data_files.env"""
        # Check multiple possible paths for data_files.env
        possible_paths = [
            Path("data_files.env"),           # Current directory
            Path("../data_files.env"),        # Parent directory
            Path("../../data_files.env"),     # Grandparent directory (for json_data_mine subdirectory)
        ]
        
        env_file_path = None
        for path in possible_paths:
            if path.exists():
                env_file_path = path
                break
        
        if env_file_path is None:
            raise FileNotFoundError("EndPointLooperParent Error: data_files.env file not found in current, parent, or grandparent directory")
        
        with open(env_file_path, 'r') as env_file:
            for line in env_file:
                line = line.strip()
                if line.startswith('CEHRT_CACHE_DIR='):
                    cache_dir = line.split('=', 1)[1].strip()
                    return cache_dir
        
        raise ValueError("EndPointLooperParent Error: CEHRT_CACHE_DIR not found in data_files.env")
    
    def discover_json_files(self, *, cache_directory: str, test_mode: bool = False) -> List[Path]:
        """
        Discover JSON files in subdirectories of the cache directory
        
        Args:
            cache_directory: Path to the CEHRT cache directory
            test_mode: If True, process 4 files each from 10 random subdirectories
            
        Returns:
            List of Path objects pointing to JSON files
        """
        # Try both relative path and resolved absolute path
        cache_path = Path(cache_directory)
        if not cache_path.exists():
            # Try alternative path locations based on current working directory
            alternative_paths = [
                Path("../../npd_ehr_scrape_cache/cache/fhir_json_cache/"),  # From json_data_mine subdirectory
                Path("../npd_ehr_scrape_cache/cache/fhir_json_cache/"),     # From misc_scripts directory  
                Path("npd_ehr_scrape_cache/cache/fhir_json_cache/"),        # From root directory
                Path("../../../npd_ehr_scrape_cache/cehrt_fhir_json/"),     # Legacy path structure
                Path("../../npd_ehr_scrape_cache/cehrt_fhir_json/"),        # Legacy path structure
                Path("../npd_ehr_scrape_cache/cehrt_fhir_json/"),           # Legacy path structure
                Path("npd_ehr_scrape_cache/cehrt_fhir_json/"),              # Legacy path structure
            ]
            
            cache_path_found = False
            for alt_path in alternative_paths:
                if alt_path.exists():
                    cache_path = alt_path
                    cache_path_found = True
                    break
            
            if not cache_path_found:
                # Try resolving the original path
                try:
                    cache_path = Path(cache_directory).resolve()
                    if not cache_path.exists():
                        raise FileNotFoundError(f"EndPointLooperParent Error: Cache directory not found at {cache_directory} or alternative locations")
                except (OSError, RuntimeError):
                    raise FileNotFoundError(f"EndPointLooperParent Error: Cache directory not accessible: {cache_directory}")
        
        json_files = []
        
        # Get all subdirectories, sorted for consistent ordering
        subdirectories = [d for d in cache_path.iterdir() if d.is_dir()]
        subdirectories.sort()
        
        if test_mode:
            # Select 10 random subdirectories, or all if fewer than 10
            num_subdirs = min(10, len(subdirectories))
            if num_subdirs > 0:
                subdirectories = random.sample(subdirectories, num_subdirs)
        
        for subdirectory in subdirectories:
            subdir_json_files = []
            
            # Look for JSON files in endpoint/ subdirectory
            endpoint_dir = subdirectory / "endpoint"
            if endpoint_dir.exists() and endpoint_dir.is_dir():
                subdir_json_files.extend(list(endpoint_dir.glob("*.json")))
            
            # Look for JSON files in organization/ subdirectory  
            organization_dir = subdirectory / "organization"
            if organization_dir.exists() and organization_dir.is_dir():
                subdir_json_files.extend(list(organization_dir.glob("*.json")))
            
            if test_mode:
                # Take only first 4 JSON files from each subdirectory (combined from both resource types)
                subdir_json_files = subdir_json_files[:4]
            
            json_files.extend(subdir_json_files)
        
        self.total_files_found = len(json_files)
        return json_files
    
    @abstractmethod
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Abstract method for children to implement their analysis logic.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        pass
    
    @abstractmethod
    def print_summary(self) -> None:
        """
        Abstract method for children to implement summary printing.
        Should have access to all instance variables including self.failure_count.
        """
        pass
    
    @abstractmethod
    def generate_summary_markdown(self) -> str:
        """
        Abstract method for children to implement summary generation as markdown string.
        Should return the same content as print_summary but as a string for file output.
        """
        pass
    
    def _resolve_relative_path(self, *, json_file_path: Path) -> str:
        """
        Extract relative path for web URL generation with fallback logic.
        
        Args:
            json_file_path: Path to the JSON file
            
        Returns:
            Relative path string suitable for GitHub URL generation
        """
        try:
            cache_directory = self.load_environment_config()
            cache_path = Path(cache_directory)
            if not cache_path.exists():
                # Use the same logic as discover_json_files
                alternative_paths = [
                    Path("../../npd_ehr_scrape_cache/cache/fhir_json_cache/"),  
                    Path("../npd_ehr_scrape_cache/cache/fhir_json_cache/"),     
                    Path("npd_ehr_scrape_cache/cache/fhir_json_cache/"),        
                    Path("../../../npd_ehr_scrape_cache/cehrt_fhir_json/"),     
                    Path("../../npd_ehr_scrape_cache/cehrt_fhir_json/"),        
                    Path("../npd_ehr_scrape_cache/cehrt_fhir_json/"),           
                    Path("npd_ehr_scrape_cache/cehrt_fhir_json/"),              
                ]
                for alt_path in alternative_paths:
                    if alt_path.exists():
                        cache_path = alt_path
                        break
            
            relative_path = str(json_file_path.relative_to(cache_path))
            return relative_path
        except ValueError:
            # Fallback: construct path as company_dir/resource_type/filename.json
            resource_type_dir = json_file_path.parent.name  # "endpoint" or "organization"
            company_dir = json_file_path.parent.parent.name  # company directory
            filename = json_file_path.name
            return f"{company_dir}/{resource_type_dir}/{filename}"

    def get_example_files_by_metric(self, *, examples: List[tuple], metric_index: int, relative_path_index: int) -> Tuple[str, str, str]:
        """
        Generic method to get longest/shortest/random examples by any metric.
        
        Args:
            examples: List of tuples containing example data
            metric_index: Index in tuple that contains the metric to sort by (e.g., length)
            relative_path_index: Index in tuple that contains the relative_path for web URL generation
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        if not examples:
            return "None", "None", "None"
        
        # Sort by the specified metric to find longest and shortest
        sorted_examples = sorted(examples, key=lambda x: x[metric_index])
        
        # Get longest (last in sorted list)
        longest_entry = sorted_examples[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[relative_path_index])
        
        # Get shortest (first in sorted list)
        shortest_entry = sorted_examples[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[relative_path_index])
        
        # Get random example
        random_entry = random.choice(examples)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[relative_path_index])
        
        return longest_url, shortest_url, random_url

    def run_loop(self, *, test_mode: bool = False) -> None:
        """
        Main method to run the complete JSON processing loop
        
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
                    # Set current relative path for child classes to use
                    if hasattr(self, 'current_relative_path'):
                        self.current_relative_path = self._resolve_relative_path(json_file_path=json_file_path)
                    
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


def run_endpoint_analyzer(*, analyzer_class, description: str = "Process JSON files from CEHRT cache directory"):
    """
    Universal command-line interface for running any EndPoint Looper child class
    
    Args:
        analyzer_class: The child class to instantiate and run
        description: Description for the argument parser
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '--test-mode', 
        action='store_true',
        help="Run in test mode (process 4 files from 10 random subdirectories)"
    )
    parser.add_argument(
        '--output_to',
        type=str,
        help="Save markdown output to specified file (without debug messages)"
    )
    
    args = parser.parse_args()
    
    try:
        # Create and run the analyzer
        analyzer = analyzer_class()
        analyzer.run_loop(test_mode=args.test_mode)
        
        # Save markdown output if requested
        if args.output_to:
            markdown_content = analyzer.generate_summary_markdown()
            with open(args.output_to, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            print(f"\nMarkdown report saved to: {args.output_to}")
            
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Analysis failed: {str(e)}")
        sys.exit(1)


def main():
    """Command-line interface for running the EndPoint Looper"""
    parser = argparse.ArgumentParser(
        description="Process JSON files from CEHRT cache directory"
    )
    parser.add_argument(
        '--test-mode', 
        action='store_true',
        help="Run in test mode (process 4 files from 10 random subdirectories)"
    )
    
    args = parser.parse_args()
    
    # Since this is an abstract class, we can't instantiate it directly
    # This main function serves as an example for child classes
    print("EndPointLooperParent is an abstract class.")
    print("Create a child class that implements analyze_this_json_data and print_summary methods.")
    print("Then instantiate your child class and call run_loop() method.")
    print(f"Test mode would be: {'Enabled' if args.test_mode else 'Disabled'}")


if __name__ == "__main__":
    main()
