# EHR FHIR Entity Slurp

A comprehensive data processing pipeline for extracting, analyzing, and normalizing EHR (Electronic Health Records) FHIR endpoint data from healthcare providers. This tool helps assess HTI-2 compliance and generates normalized datasets for healthcare interoperability analysis.

Current home is [/DSACMS/npd_ehr_fhir_npi_slurp](https://github.com/DSACMS/npd_ehr_fhir_npi_slurp).
Assumes that the json data is being saved to [https://github.com/ftrotter-gov/npd_ehr_scrape_cache](https://github.com/ftrotter-gov/npd_ehr_scrape_cache)

## Problem Documentaion

* [Understanding Endpoints in SAAS vs on-prem EHR instances](./docs/fhir_tenancy_explained.md)

## Overview

This project processes FHIR endpoint data through a multi-step pipeline:

1. **Extract List Sources** - Analyzes Lantern CSV data to identify unique EHR vendor service endpoints
2. **Download Service Data** - Retrieves FHIR Bundle JSON files from EHR vendors
3. **Parse FHIR Bundles** - Breaks down large FHIR bundles into individual resource files
4. **Extract & Normalize** - Creates normalized CSV datasets with proper data validation

## Features

### Now

* **NPI Validation**: Real-time validation against CMS NPI Registry API
* **Data Deduplication**: Hash-based deduplication for efficient storage
* **Error Handling**: Comprehensive error tracking and reporting
* **Test Mode**: Limited processing for development and validation
* **Progress Tracking**: Visual progress indicators for long-running operations

### Future

We can and will handle these later on in the process, so we are not implementing them up-front for now.

* **Phone Number Normalization**: International phone number parsing and validation
* **Address Standardization**: Structured address parsing and normalization (waiting on Smarty Streets for full implementation)  

## Quick Start

### Prerequisites

* Python 3.8+
* Virtual environment (recommended)

### Basic Usage

First download the endpoint data from the [Lantern Dashboard download page](https://lantern.healthit.gov/?tab=downloads_tab)
Put that data in local_data/prod_data/fhir_endpoints.csv

Then choose either go.py or manual runnning of the pipeline step-by-step:

```bash
# Run the complete pipeline
python go.py
```

Or you can look inside py to understand what specific steps should be run in.

## Pipeline Steps

### Step 1: Extract List Sources

**File**: `Step10_extract_list_source_from_lantern_csv.py`

Processes Lantern FHIR endpoint CSV files to extract unique service list sources by EHR vendor.

**Input**: CSV with FHIR endpoint data from Lantern
**Output**: `list_sources_summary.csv` with distinct list sources and URL counts

### Step 2: Download Service Data

**File**: `Step20_download_list_source_json.py`

Downloads FHIR Bundle JSON files from EHR vendor service endpoints.

**Features**:
* Respectful rate limiting with configurable delays
* Safe filename generation from vendor names
* Error handling and retry logic
* Progress tracking
* Creates JSON files in CEHRT cache directory

### Step 3: Parse FHIR Bundles

**File**: `Step30_parse_source_bundle.py`

Breaks down large FHIR Bundle files into individual resource entries for easier processing.

**Features**:
* Batch processing of multiple JSON files
* Extracts individual FHIR Bundle entries into separate JSON files
* Resource type categorization
* Comprehensive error reporting with CSV logs
* Progress reporting

### Step 4: Extract & Normalize Data

**File**: `Step40_extract_csv_data.py`

Creates normalized CSV datasets from FHIR Organization resources with NPI validation.

**Features**:
* Two-pass processing for endpoint reference mapping
* NPI validation using NPIValidator class
* Phone number normalization using international standards
* Hash-based deduplication
* Test mode support (first 1000 files per vendor)

**Output Files**:
* `step40_distinct_organizations.csv` - Valid organizations (with NPI + endpoint)
* `step40_distinct_addresses.csv` - Normalized address data
* `step40_distinct_endpoints.csv` - FHIR endpoint references
* `step40_distinct_phones.csv` - Validated phone numbers with international formatting
* `step40_distinct_contact_urls.csv` - Contact URLs
* `step40_distinct_contact_emails.csv` - Email addresses
* `step40_org_to_*.csv` - Relationship mapping files
* `step40_processing_errors.csv` - Error log

### Step 5: Clean and Validate Data

**File**: `Step50_simple_clean_output.py`

Cleans the org_to_npi data by filtering for valid HTTPS URLs and 10-digit NPIs, then checks domain responsiveness.

**Features**:
* Filters for valid HTTPS URLs and 10-digit NPI numbers
* Tests domain responsiveness (accepts 200-499 status codes)
* Respectful rate limiting between domain checks
* Comprehensive logging

**Input**: `step40_org_to_npi.csv`
**Output**: `step50_clean_npi_to_org_fhir_url.csv` with cleaned data

### Step 6: Discover FHIR Endpoints

**File**: `Step60_CalculateOpenEndpoints.py`

Enriches data by discovering well-known FHIR endpoints at multiple directory levels for each domain.

**Features**:
* Tests multiple directory levels for each domain
* Discovers 6 endpoint types: Capability Statement, SMART Config, OpenAPI docs/JSON, Swagger docs/JSON
* Chooses best HTTPS organizational URL
* Rate limiting between requests

**Endpoint Discovery**:
* `/metadata` - FHIR Capability Statement
* `/.well-known/smart-configuration` - SMART on FHIR configuration
* `/api-docs` - OpenAPI documentation
* `/openapi.json` - OpenAPI specification
* `/swagger` - Swagger documentation  
* `/swagger.json` - Swagger specification

**Input**: `step50_clean_npi_to_org_fhir_url.csv`
**Output**: `step60_enriched_endpoints.csv` with endpoint discovery results

### Step 89: Generate CEHRT Dashboard CSV

**File**: `Step89_GenerateCEHRTDashboardCSV.py`

Aggregates compliance results per CEHRT vendor for dashboard visualization.

**Features**:
* Reads vendor mapping from list sources summary
* Combines endpoint discovery with partial compliance data
* Aggregates per-vendor compliance across all endpoints
* Handles vendors with data in different pipeline stages

**Compliance Checks**:
* Reachable (domain responsive)
* Has ONPI (valid 10-digit NPI)
* HTTPS ORG URL (secure endpoint available)
* Findable endpoints (Metadata, SMART, OpenAPI, Swagger)

**Input**: Multiple CSV files from previous steps
**Output**: `step89_CEHRT_FHIR_Report.csv` with vendor compliance summary

### Step 90: Create CEHRT Dashboard

**File**: `Step90_MakeCEHRTDashboard.py`

Creates a visual HTML dashboard showing CEHRT vendor compliance with icons.

**Features**:
* Converts CSV compliance data to visual HTML table
* Uses green icons for passing checks, red X for failures
* Makes successful endpoint URLs clickable links
* Sorts vendors by compliance score (most compliant first)

**Icon Mapping**:
* Green check marks for basic compliance (Up, ONPI)
* Themed icons for different endpoint types (FHIR fire icons)
* Red X for all failures
* Clickable links to actual discovered endpoints

**Input**: `step89_CEHRT_FHIR_Report.csv`
**Output**: `step90_CEHRT_FHIR_Report.md` - Visual compliance dashboard

## Data Validation

### NPI Validation

* Format validation (10-digit requirement)
* API validation against the list of valid NPIs in ./npi_validation_data/, which falls back to using the Registery for missing npis. See [NPIValidator_README.md](NPIValidator_README.md) for more info.
* Invalid NPI flagging based on format validation

### Phone Number Validation

* International format parsing using `phonenumbers` library
* Extension extraction and normalization
* Country code standardization
* Validation status tracking

### Data Quality Requirements

Organizations must have:
* At least one valid NPI identifier
* At least one FHIR endpoint
* Valid organizational name

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

* `org_id` - FHIR Organization ID
* `org_name` - Organization name
* `vendor_name` - EHR vendor name
* `active` - Organization status
* `*_count` - Counts of related data elements

### Relationship Tables

Link organizations to their associated data:

* NPIs (with validation status)
* Addresses (normalized)
* Phone numbers (validated)
* Endpoints (FHIR references)
* Contact information

## Error Handling

The pipeline includes comprehensive error handling:
* File processing errors logged to `processing_errors.csv`
* API validation errors tracked per NPI
* Network timeout handling with retries
* Malformed data detection and reporting

## Performance Considerations

* **Memory Usage**: Large FHIR bundles are processed incrementally
* **API Rate Limiting**: Built-in delays for NPI validation API
* **Disk Space**: Intermediate files can be large; monitor disk usage
* **Processing Time**: Full pipeline may take several hours for large datasets

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

## Policies

### Open Source Policy

We adhere to the [CMS Open Source Policy](https://github.com/CMSGov/cms-open-source-policy). If you have any questions, just [shoot us an email](mailto:opensource@cms.hhs.gov).

### Security and Responsible Disclosure Policy

_Submit a vulnerability:_ Vulnerability reports can be submitted through [Bugcrowd](https://bugcrowd.com/cms-vdp). Reports may be submitted anonymously. If you share contact information, we will acknowledge receipt of your report within 3 business days.

### Software Bill of Materials (SBOM)

A Software Bill of Materials (SBOM) is a formal record containing the details and supply chain relationships of various components used in building software.

In the spirit of [Executive Order 14028 - Improving the Nation's Cyber Security](https://www.gsa.gov/technology/it-contract-vehicles-and-purchasing-programs/information-technology-category/it-security/executive-order-14028), a SBOM for this repository is provided here: <https://github.com/{{> cookiecutter.project_org }}/{{ cookiecutter.project_repo_name }}/network/dependencies.

For more information and resources about SBOMs, visit: <https://www.cisa.gov/sbom>.

## Public domain

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/) as indicated in [LICENSE](LICENSE).

All contributions to this project will be released under the CC0 dedication. By submitting a pull request or issue, you are agreeing to comply with this waiver of copyright interest.
