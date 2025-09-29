# FHIR JSON Analysis Loopers Documentation

This document provides comprehensive documentation for the complete suite of FHIR JSON analysis loopers implemented in this directory.

## Architecture Overview

All loopers inherit from `EndPointLooperParent.py` which provides:

- Common JSON file discovery and processing logic
- Static validation and classification methods
- Standardized command-line interface
- Markdown report generation framework
- GitHub URL generation for examples

## Parent Class Static Methods

### Content Classification

- `classify_id_content(content)` - Classifies IDs/URLs using comprehensive regex patterns
- `classify_address_content(content)` - Classifies address content with address-specific patterns
- `validate_email(email)` - Validates email addresses using regex
- `validate_phone(phone)` - Validates phone numbers (10-11 digits)
- `validate_npi(npi)` - Validates NPI format (10 digits starting with '1')

## Complete Looper Suite

### 1. EndpointIDLooper.py

**Purpose:** Analyzes resource ID fields using comprehensive regex classification

- **Classification:** 21+ patterns including URLs, UUIDs v1-v8, emails, hexadecimal, base64
- **Special Features:** Descriptive UUID labels (e.g., "UUID v4 (Random)")
- **Examples:** Longest/shortest/random by ID character length
- **Output:** Markdown table with unmatched IDs bullet list

### 2. TelecomEmailLooper.py  

**Purpose:** Analyzes telecom entries with "email" system

- **Validation:** Standard RFC-compliant email regex
- **Categories:** Valid email, invalid email, no email telecoms
- **Examples:** Longest/shortest/random by email character length
- **Output:** Email validation statistics

### 3. TelecomPhoneLooper.py

**Purpose:** Analyzes telecom entries with "phone" system  

- **Validation:** 10-11 digit phone number validation
- **Categories:** Valid 10-digit, valid 11-digit, invalid format
- **Examples:** Longest/shortest/random by phone character length
- **Output:** Phone validation statistics

### 4. ActiveStatusLooper.py

**Purpose:** Analyzes presence and values of "active" field

- **Categories:** Active: true, Active: false, No active field
- **Examples:** Longest/shortest/random by filename length
- **Output:** Active status distribution with percentages

### 5. NPILooper.py

**Purpose:** Analyzes NPI identifiers with comprehensive validation

- **Validation:** 10 digits starting with '1' (no Luhn checksum)
- **Categories:** Single valid/invalid NPI, multiple NPIs (all/some valid)
- **Special Features:** Tracks record with most NPIs, examples by NPI count
- **Examples:** Uses NPI list length instead of character length
- **Output:** NPI analysis with special "Record with Most NPIs" section

### 6. NameLooper.py

**Purpose:** Analyzes presence of "name" field

- **Detection:** Checks for non-empty string, list, or dict name fields
- **Categories:** Has name field, no name field
- **Examples:** Longest/shortest/random by filename length
- **Output:** Name field presence statistics

### 7. MetaLooper.py

**Purpose:** Analyzes "meta" tag structure and unknown keys

- **Expected Fields:** versionId, lastUpdated, source
- **Categories:** Has meta, no meta, individual subfield presence
- **Special Features:** Reports unknown meta keys beyond expected ones
- **Examples:** Longest/shortest/random by filename length
- **Output:** Meta analysis with unknown keys section

### 8. ConnectionTypeLooper.py

**Purpose:** Analyzes "connectionType" field and system/code statistics

- **Structure Analysis:** Handles single dict or list of connectionType entries
- **Statistics:** System and code value frequency tables
- **Categories:** Has connectionType, no connectionType
- **Examples:** Longest/shortest/random by filename length
- **Output:** Connection type presence plus system/code breakdowns

### 9. AddressLooper.py

**Purpose:** Analyzes address field and subcomponent presence

- **Subcomponents:** line, line1, line2, city, state, postalCode, country
- **Statistics:** Percentage analysis of each subcomponent
- **Categories:** Has address, no address, individual component presence
- **Examples:** Longest/shortest/random by filename length
- **Output:** Address presence with detailed component breakdown percentages

### 10. AddressFieldLooper.py

**Purpose:** Classifies address field contents using regex patterns

- **Classification:** Uses `classify_address_content()` static method
- **Patterns:** PO Box, suite/unit, street numbers, directional, street types, ZIP codes
- **Categories:** Based on address content classification
- **Examples:** Longest/shortest/random by filename length
- **Output:** Address content classification statistics

### 11. PayloadTypeLooper.py

**Purpose:** Analyzes payloadType field including coding and address subkeys

- **Coding Analysis:** System and code statistics from coding subkey
- **Address Classification:** Uses regex classification on address subkey
- **Header Detection:** Tracks presence of header subkey
- **Categories:** Has payloadType, coding, address, header presence
- **Examples:** Longest/shortest/random by filename length
- **Output:** Multi-faceted payloadType analysis with system/code/address stats

## Command-Line Usage

All loopers support the same command-line interface:

```bash
# Run in test mode (4 files from 10 random subdirectories)
python LooperName.py --test-mode

# Save markdown output to file
python LooperName.py --output_to report_filename.md

# Combined usage
python LooperName.py --test-mode --output_to test_report.md
```

## Output Format

All loopers generate consistent markdown reports with:

- **Header:** File processing statistics (processed, failed)
- **Main Table:** Category analysis with longest/shortest/random examples
- **GitHub Links:** Direct links to example files in the scrape cache
- **Special Sections:** Looper-specific additional analysis (unknown keys, system/code stats, etc.)

## Example Table Format

```markdown
| Category | Count | Longest Example | Shortest Example | Random Example |
|----------|-------|-----------------|------------------|----------------|
| Category Name | 123 | [filename.json](github-url) | [file.json](github-url) | [example.json](github-url) |
```

## Implementation Notes

- **Memory Management:** All loopers limit example storage to prevent memory issues
- **Error Handling:** Silent JSON parsing failures with failure count tracking
- **Path Handling:** Robust relative path calculation with fallback mechanisms
- **Type Safety:** Comprehensive type checking and None handling
- **Extensibility:** Easy to add new loopers following the established pattern

## Testing

All loopers have been tested with sample data:

- TelecomEmailLooper: 34 files processed, 0 failures, 34 files without email telecoms
- ActiveStatusLooper: 34 files processed, 0 failures, 3 categories found (20 true, 13 no field, 1 false)
- Comprehensive regex patterns working correctly across all implementations

## Performance

- **Test Mode:** Processes 4 files from 10 random subdirectories (typically 30-40 files)
- **Full Mode:** Processes all JSON files in cache directory
- **Progress Reporting:** Updates every 100 files processed
- **Efficient Processing:** Uses generators and iterators for large datasets
