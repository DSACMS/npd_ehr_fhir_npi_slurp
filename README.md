# EHR FHIR Entity Slurp

A comprehensive data processing pipeline for extracting, analyzing, and normalizing EHR (Electronic Health Records) FHIR endpoint data from healthcare providers. This tool helps assess HTI-2 compliance and generates normalized datasets for healthcare interoperability analysis.



# TODO 
* We need to streamline the whole process to account for the fact that some entry blocks, like 
data/service_json/athena-fhir-service-base-urls/entry_a1c9c7fe-6d45-5a92-922c-7bfcd55a062d.json
"forget" the URL they are sourced from. We need to figure out how to retain that information from a previous step so that we can keep everything in https. 


## Overview

This project processes FHIR endpoint data through a multi-step pipeline:

1. **Extract List Sources** - Analyzes Lantern CSV data to identify unique EHR vendor service endpoints
2. **Download Service Data** - Retrieves FHIR Bundle JSON files from EHR vendors
3. **Parse FHIR Bundles** - Breaks down large FHIR bundles into individual resource files
4. **Extract & Normalize** - Creates normalized CSV datasets with proper data validation

## Features

- **NPI Validation**: Real-time validation against CMS NPI Registry API
- **Phone Number Normalization**: International phone number parsing and validation
- **Address Standardization**: Structured address parsing and normalization (waiting on Smarty Streets for full implementation)  
- **Data Deduplication**: Hash-based deduplication for efficient storage
- **Error Handling**: Comprehensive error tracking and reporting
- **Test Mode**: Limited processing for development and validation
- **Progress Tracking**: Visual progress indicators for long-running operations

## Quick Start

### Prerequisites

- Python 3.8+
- Virtual environment (recommended)


### Basic Usage

First download the endpoint data from the [Lantern Dashboard download page](https://lantern.healthit.gov/?tab=downloads_tab)
Put that data in local_data/prod_data/fhir_endpoints.csv

Then choose either go.sh or manual runnning of the pipeline step-by-step:

```bash
# Run the complete pipeline
./go.sh

# Or run individual steps:
python Step10_extract_list_source_from_lantern_csv.py --input_file local_data/prod_data/fhir_endpoints.csv --output_file local_data/prod_data/list_sources_summary.csv
python Step20_download_list_source_json.py --input_file local_data/prod_data/list_sources_summary.csv --output_dir ./data/service_json/
python Step30_parse_source_bundle.py --input_dir ./data/service_json/
python Step40_extract_csv_data.py
```

### Test Mode

For development and validation, use test mode to process only a subset of data:

```bash
python Step40_extract_csv_data.py --test
```

## Pipeline Steps

### Step 1: Extract List Sources
**File**: `Step10_extract_list_source_from_lantern_csv.py`

Processes Lantern FHIR endpoint CSV files to extract unique service list sources by EHR vendor.

**Input**: CSV with FHIR endpoint data
**Output**: Summary CSV with distinct list sources and URL counts

### Step 2: Download Service Data  
**File**: `Step20_download_list_source_json.py`

Downloads FHIR Bundle JSON files from EHR vendor service endpoints.

**Features**:
- Respectful rate limiting
- Safe filename generation
- Error handling and retry logic
- Progress tracking

### Step 3: Parse FHIR Bundles
**File**: `Step30_parse_source_bundle.py`

Breaks down large FHIR Bundle files into individual resource entries for easier processing.

**Features**:
- Batch processing of multiple files
- Resource type categorization
- Progress reporting
- Error handling

### Step 4: Extract & Normalize Data
**File**: `Step40_extract_csv_data.py`

Creates normalized CSV datasets from FHIR Organization resources.

**Output Files**:
- `distinct_organizations.csv` - Unique organizations with counts
- `distinct_addresses.csv` - Normalized address data
- `distinct_endpoints.csv` - FHIR endpoint references
- `distinct_phones.csv` - Validated phone numbers
- `distinct_contact_urls.csv` - Contact URLs and emails
- `org_to_*.csv` - Relationship mapping files
- `processing_errors.csv` - Error log

## Data Validation

### NPI Validation
- Format validation (10-digit requirement)
- API validation against the list of valid NPIs in ./npi_validation_data/, which falls back to using the Registery for missing npis. See [NPIValidator_README.md](NPIValidator_README.md) for more info.
- Invalid NPI flagging based on format validation

### Phone Number Validation
- International format parsing using `phonenumbers` library
- Extension extraction and normalization
- Country code standardization
- Validation status tracking

### Data Quality Requirements
Organizations must have:
- At least one valid NPI identifier
- At least one FHIR endpoint
- Valid organizational name

## Configuration

### Environment Variables
```bash
# Optional: Set custom timeout for API requests
export FHIR_REQUEST_TIMEOUT=30

# Optional: Set custom delay between downloads
export DOWNLOAD_DELAY=1.0
```

## Output Data Structure

### Organizations Table
- `org_id` - FHIR Organization ID
- `org_name` - Organization name
- `vendor_name` - EHR vendor name
- `active` - Organization status
- `*_count` - Counts of related data elements

### Relationship Tables
Link organizations to their associated data:
- NPIs (with validation status)
- Addresses (normalized)
- Phone numbers (validated)
- Endpoints (FHIR references)
- Contact information

## Error Handling

The pipeline includes comprehensive error handling:
- File processing errors logged to `processing_errors.csv`
- API validation errors tracked per NPI
- Network timeout handling with retries
- Malformed data detection and reporting

## Performance Considerations

- **Memory Usage**: Large FHIR bundles are processed incrementally
- **API Rate Limiting**: Built-in delays for NPI validation API
- **Disk Space**: Intermediate files can be large; monitor disk usage
- **Processing Time**: Full pipeline may take several hours for large datasets

## Development

### Testing
```bash
# Run tests
pytest

# Run with coverage
pytest --cov=.
```

### Adding New Steps
1. Create new `StepXX_description.py` file
2. Follow existing patterns for argument parsing
3. Add comprehensive error handling
4. Update `go.sh` script
5. Document in README

## License

See LICENSE file for details.

