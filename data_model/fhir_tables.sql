create table if not exists address_nonstandard
(
    id              varchar(10) not null
    constraint pk_address_nonstandard
    primary key,
    addressee       varchar(64),
    delivery_line_1 varchar(64) not null,
    delivery_line_2 varchar(64),
    last_line       varchar(64),
    address_type    varchar(32),
    address_format  varchar(128),
    raw_address     text,
    latitude        numeric(9, 6),
    longitude       numeric(9, 6),
    notes           text
    );

create table if not exists credential_type
(
    id    serial
    constraint pk_credential_type
    primary key,
    value varchar(20)
    );

create table if not exists degree_type
(
    id    serial
    constraint pk_degree_type
    primary key,
    value varchar(50)
    constraint uc_degree_type_value
    unique
    );

create table if not exists ehr_vendor
(
    id                     uuid         not null
    constraint pk_ehr_vendor
    primary key,
    name                   varchar(200) not null,
    is_cms_aligned_network boolean default false
    );

create table if not exists endpoint_instance_type
(
    id    serial
    constraint pk_endpoint_instance_type
    primary key,
    value varchar(50)
    constraint uc_endpoint_instance_type_value
    unique
    );

create table if not exists endpoint_type
(
    id    serial
    constraint pk_endpoint_type
    primary key,
    value varchar(50)
    constraint uc_endpoint_type_value
    unique
    );

create table if not exists fhir_address_use
(
    id    serial
    constraint pk_fhir_address_use
    primary key,
    value varchar(20)
    constraint uc_fhir_address_use_value
    unique
    );

create table if not exists fhir_email_use
(
    id    serial
    constraint pk_fhir_email_use
    primary key,
    value varchar(20)
    constraint uc_fhir_email_use_value
    unique
    );

create table if not exists fhir_name_use
(
    id    serial
    constraint pk_fhir_name_use
    primary key,
    value varchar(20)
    constraint uc_fhir_name_use_value
    unique
    );

create table if not exists fhir_phone_system
(
    id    serial
    constraint pk_fhir_phone_system
    primary key,
    value varchar(20)
    constraint uc_fhir_phone_system_value
    unique
    );

create table if not exists fhir_phone_use
(
    id    serial
    constraint pk_fhir_phone_use
    primary key,
    value varchar(20)
    constraint uc_fhir_phone_use_value
    unique
    );

create table if not exists fips_state
(
    id           char(2)      not null
    constraint pk_fips_state
    primary key,
    name         varchar(100) not null
    constraint uc_fips_state_name
    unique,
    abbreviation char(2)      not null
    constraint uc_fips_state_abbreviation
    unique
    );

create table if not exists fips_county
(
    id            varchar(5)   not null
    constraint pk_fips_county
    primary key,
    name          varchar(200) not null,
    fips_state_id varchar(2)   not null
    constraint fk_fips_county_fips_state_id
    references fips_state,
    constraint uc_fips_county_name_fips_state_id
    unique (name, fips_state_id)
    );

create table if not exists address_us
(
    id                         varchar(10) not null
    constraint pk_address_us
    primary key,
    addressee                  varchar(64),
    delivery_line_1            varchar(64) not null,
    delivery_line_2            varchar(64),
    last_line                  varchar(64),
    delivery_point_barcode     varchar(12),
    urbanization               varchar(64),
    primary_number             varchar(30),
    street_name                varchar(64),
    street_predirection        varchar(16),
    street_postdirection       varchar(16),
    street_suffix              varchar(16),
    secondary_number           varchar(32),
    secondary_designator       varchar(16),
    extra_secondary_number     varchar(32),
    extra_secondary_designator varchar(16),
    pmb_designator             varchar(16),
    pmb_number                 varchar(16),
    city_name                  varchar(64) not null,
    default_city_name          varchar(64),
    state_code                 char(2)     not null
    constraint fk_address_us_state_code
    references fips_state,
    zipcode                    char(5)     not null,
    plus4_code                 varchar(4),
    delivery_point             char(2),
    delivery_point_check_digit char,
    record_type                char,
    zip_type                   varchar(32),
    county_code                char(5)
    constraint fk_address_us_county_code
    references fips_county,
    ews_match                  char(5),
    carrier_route              char(4),
    congressional_district     char(2),
    building_default_indicator char,
    rdi                        varchar(12),
    elot_sequence              varchar(4),
    elot_sort                  varchar(4),
    latitude                   numeric(9, 6),
    longitude                  numeric(9, 6),
    coordinate_license         integer,
    geo_precision              varchar(18),
    time_zone                  varchar(48),
    utc_offset                 numeric(4, 2),
    dst                        char(5),
    dpv_match_code             varchar(1),
    dpv_footnotes              varchar(32),
    dpv_cmra                   varchar(1),
    dpv_vacant                 varchar(1),
    dpv_no_stat                varchar(1),
    active                     varchar(1),
    footnotes                  varchar(24),
    lacslink_code              varchar(2),
    lacslink_indicator         varchar(1),
    suitelink_match            varchar(5),
    enhanced_match             varchar(64)
    );

create table if not exists individual
(
    id         uuid not null
    constraint pk_individual
    primary key,
    ssn_id     uuid,
    gender     char,
    sex        char,
    birth_date date
);

create table if not exists individual_to_email
(
    individual_id uuid          not null
    constraint fk_individual_to_email_individual_id
    references individual
    on delete cascade,
    email_address varchar(1000) not null,
    email_use_id  integer       not null
    constraint fk_individual_to_email_email_use_id
    references fhir_email_use,
    constraint pk_individual_to_email
    primary key (individual_id, email_address, email_use_id)
    );

create table if not exists individual_to_name
(
    individual_id uuid         not null
    constraint fk_individual_to_name_individual_id
    references individual
    on delete cascade,
    prefix        varchar(10),
    first_name    varchar(50)  not null,
    middle_name   varchar(50),
    last_name     varchar(200) not null,
    start_date    date,
    end_date      date,
    name_use_id   integer      not null
    constraint fk_individual_to_name_name_use_id
    references fhir_name_use,
    suffix        varchar(10),
    constraint pk_individual_to_name
    primary key (individual_id, first_name, last_name, name_use_id)
    );

create table if not exists individual_to_phone
(
    individual_id uuid                           not null
    constraint fk_individual_to_phone_individual_id
    references individual
    on delete cascade,
    phone_number  varchar(20)                    not null,
    extension     varchar(10),
    phone_use_id  integer                        not null
    constraint fk_individual_to_phone_phone_use_id
    references fhir_phone_use,
    id            uuid default gen_random_uuid() not null
    constraint pk_individual_to_phone
    primary key,
    constraint uc_individual_to_phone_individual_id_phone_number
    unique (individual_id, phone_number, phone_use_id)
    );

create table if not exists iso_country
(
    code varchar(2) not null
    constraint pk_iso_country
    primary key,
    name varchar(50)
    constraint uc_iso_country_name
    unique
    );

create table if not exists address_international
(
    id                       varchar(10) not null
    constraint pk_address_international
    primary key,
    country_code             varchar(2)  not null
    constraint fk_address_international_country_code
    references iso_country,
    geocode                  varchar(4),
    local_language           varchar(6),
    freeform                 varchar(512),
    address1                 varchar(64) not null,
    address2                 varchar(64),
    address3                 varchar(64),
    address4                 varchar(64),
    organization             varchar(64),
    locality                 varchar(64),
    administrative_area      varchar(32),
    postal_code              varchar(16),
    administrative_area_iso2 varchar(8),
    sub_administrative_area  varchar(64),
    country_iso_3            varchar(3),
    premise                  varchar(64),
    premise_number           varchar(64),
    thoroughfare             varchar(64),
    latitude                 numeric(9, 6),
    longitude                numeric(9, 6),
    geocode_precision        varchar(32),
    max_geocode_precision    varchar(32),
    address_format           varchar(128),
    verification_status      varchar(32),
    address_precision        varchar(32),
    max_address_precision    varchar(32)
    );

create table if not exists address
(
    id                       uuid not null
    constraint pk_address
    primary key,
    barcode_delivery_code    varchar(12),
    smarty_key               varchar(10),
    address_us_id            varchar(10)
    constraint fk_address_address_us_id
    references address_us,
    address_international_id varchar(10)
    constraint fk_address_address_international_id
    references address_international,
    address_nonstandard_id   varchar(10)
    constraint fk_address_address_nonstandard_id
    references address_nonstandard
    );

create table if not exists individual_to_address
(
    individual_id  uuid    not null
    constraint fk_individual_to_address_individual_id
    references individual
    on delete cascade,
    address_id     uuid    not null
    constraint fk_individual_to_address_address_id
    references address,
    address_use_id integer not null
    constraint fk_individual_to_address_address_use_id
    references fhir_address_use
    on delete cascade,
    constraint pk_individual_to_address
    primary key (individual_id, address_id, address_use_id)
    );

create table if not exists language_spoken
(
    id    varchar(2) not null
    constraint pk_language_spoken
    primary key,
    value varchar(100)
    );

create table if not exists individual_to_language_spoken
(
    language_spoken_id char(2) not null
    constraint fk_individual_to_language_spoken_language_spoken_id
    references language_spoken,
    individual_id      uuid    not null,
    constraint pk_individual_to_language_spoken
    primary key (individual_id, language_spoken_id)
    );

create table if not exists legal_entity
(
    ein_id   uuid         not null
    constraint pk_legal_entity
    primary key,
    dba_name varchar(100) not null
    );

create table if not exists medicare_provider_type
(
    id    serial
    constraint pk_medicare_provider_type
    primary key,
    value varchar(20)
    constraint uc_medicare_provider_type_value
    unique
    );

create table if not exists npi
(
    npi                      bigint   not null
    constraint pk_npi
    primary key,
    entity_type_code         smallint not null,
    replacement_npi          varchar(11),
    enumeration_date         date     not null,
    last_update_date         date     not null,
    deactivation_reason_code varchar(3),
    deactivation_date        date,
    reactivation_date        date,
    certification_date       date
    );

create table if not exists nucc
(
    code                  varchar(10) not null
    constraint pk_nucc
    primary key,
    display_name          text        not null,
    definition            text,
    notes                 text,
    certifying_board_name text,
    certifying_board_url  text
    );

create table if not exists nucc_grouping
(
    id           serial
    constraint pk_nucc_grouping
    primary key,
    display_name varchar(100)
    constraint uc_nucc_grouping_display_name
    unique
    );

create table if not exists nucc_classification
(
    id               serial
    constraint pk_nucc_classification
    primary key,
    nucc_code        varchar(10)
    constraint fk_nucc_classification_nucc_code
    references nucc,
    display_name     varchar(100),
    nucc_grouping_id integer
    constraint fk_nucc_classification_nucc_grouping_id
    references nucc_grouping,
    constraint uc_nucc_classification_nucc_code_nucc_grouping
    unique (nucc_code, nucc_grouping_id)
    );

create table if not exists nucc_specialization
(
    id                     serial
    constraint pk_nucc_specialization
    primary key,
    nucc_code              varchar(10)
    constraint fk_nucc_specialization_nucc_code
    references nucc,
    display_name           varchar(100),
    nucc_classification_id integer,
    constraint uc_nucc_specialization_nucc_code_nucc_classification
    unique (nucc_code, nucc_classification_id)
    );

create table if not exists nucc_to_medicare_provider_type
(
    medicare_provider_type_id integer     not null
    constraint fk_nucc_to_medicare_provider_type_medicare_provider_type_id
    references medicare_provider_type,
    nucc_code                 varchar(10) not null
    constraint fk_nucc_to_medicare_provider_type_nucc_code
    references nucc,
    constraint pk_nucc_to_medicare_provider_type
    primary key (medicare_provider_type_id, nucc_code)
    );

create table if not exists organization
(
    id                     uuid not null
    constraint pk_organization
    primary key,
    authorized_official_id uuid not null
    constraint fk_organization_authorized_official_id
    references individual
    on delete cascade,
    ein_id                 uuid
    constraint fk_organization_ein_id
    references legal_entity,
    parent_id              uuid
    constraint fk_organization_parent_id
    references organization
    on delete cascade
);

create table if not exists organization_to_address
(
    organization_id uuid    not null
    constraint fk_organization_to_address_organization_id
    references organization
    on delete cascade,
    address_id      uuid    not null
    constraint fk_organization_to_address_address_id
    references address,
    address_use_id  integer not null
    constraint fk_organization_to_address_address_use_id
    references fhir_address_use
    on delete cascade,
    constraint pk_organization_to_address
    primary key (organization_id, address_id, address_use_id)
    );

create table if not exists organization_to_name
(
    organization_id uuid          not null
    constraint fk_organization_name_organization_id
    references organization
    on delete cascade,
    name            varchar(1000) not null,
    is_primary      boolean default false,
    constraint pk_organization_name
    primary key (organization_id, name)
    );

create table if not exists organization_to_phone
(
    organization_id uuid                           not null
    constraint fk_organization_to_phone_organization_id
    references organization
    on delete cascade,
    phone_number    varchar(20)                    not null,
    extension       varchar(10),
    phone_use_id    integer                        not null
    constraint fk_organization_to_phone_phone_use_id
    references fhir_phone_use,
    id              uuid default gen_random_uuid() not null
    constraint pk_organization_to_phone
    primary key,
    constraint uc_organization_to_phone_organization_id_phone_number
    unique (organization_id, phone_number, extension, phone_use_id)
    );

create table if not exists location
(
    id              uuid not null
    constraint pk_location
    primary key,
    name            varchar(200),
    organization_id uuid not null
    constraint fk_location_organization_id
    references organization
    on delete cascade,
    address_id      uuid not null
    constraint fk_location_address_id
    references address,
    active          boolean default true,
    phone_id        uuid
    constraint fk_location_phone_id
    references organization_to_phone
    );

create table if not exists other_id_type
(
    id    serial
    constraint pk_other_id_type
    primary key,
    value varchar(50)
    );

create table if not exists provider
(
    npi           bigint not null
    constraint pk_provider
    primary key
    constraint fk_provider_npi
    references npi
    on delete cascade,
    individual_id uuid
    constraint pk_provider_individual_id
    unique
    constraint fk_provider_individual_id
    references individual
    on delete cascade
);

create table if not exists provider_education
(
    npi            bigint  not null
    constraint fk_provider_education_npi
    references provider
    on delete cascade,
    school_id      integer not null,
    degree_type_id integer not null
    constraint fk_provider_education_degree_type_id
    references degree_type,
    start_date     date,
    end_date       date,
    constraint pk_provider_education
    primary key (npi, school_id)
    );

create table if not exists provider_to_other_id
(
    npi              bigint       not null
    constraint fk_provider_to_other_id_npi
    references provider
    on delete cascade,
    other_id         varchar(100) not null,
    other_id_type_id integer      not null
    constraint fk_provider_to_other_id_other_id_type_id
    references other_id_type,
    state_code       varchar(2)   not null
    constraint fk_organization_to_other_id_state_code
    references fips_state
    constraint fk_provider_to_other_id_state_code
    references fips_state,
    issuer           varchar(100) not null,
    constraint pk_provider_to_other_id
    primary key (npi, other_id, other_id_type_id, issuer, state_code)
    );

create table if not exists provider_to_taxonomy
(
    npi        bigint                            not null
    constraint fk_provider_to_taxonomy_npi
    references provider
    on delete cascade,
    nucc_code  varchar(10)                       not null
    constraint fk_provider_to_taxonomy_nucc_code
    references nucc,
    is_primary boolean default false,
    id         uuid    default gen_random_uuid() not null
    constraint pk_provider_to_taxonomy
    primary key,
    constraint uc_provider_to_taxonomy
    unique (npi, nucc_code)
    );

create table if not exists provider_to_credential
(
    credential_type_id      integer                        not null
    constraint fk_provider_to_credential_credential_type_id
    references credential_type,
    license_number          varchar(20)                    not null,
    state_code              char(2)                        not null
    constraint fk_provider_to_credential_state_code
    references fips_state,
    provider_to_taxonomy_id uuid default gen_random_uuid() not null
    constraint fk_provider_to_credential_provider_to_taxonomy_id
    references provider_to_taxonomy
    );

create table if not exists relationship_type
(
    id    serial
    constraint pk_relationship_type
    primary key,
    value varchar(20)
    constraint uc_relationship_type_value
    unique
    );

create table if not exists provider_to_organization
(
    individual_id        uuid                              not null
    constraint fk_provider_to_organization_individual_id
    references provider (individual_id),
    organization_id      uuid                              not null
    constraint fk_provider_to_organization_organization_id
    references organization
    on delete cascade,
    relationship_type_id integer                           not null
    constraint fk_provider_to_organization_relationship_type_id
    references relationship_type,
    id                   uuid    default gen_random_uuid() not null
    constraint pk_provider_to_organization
    primary key,
    active               boolean default true,
    constraint uc_provider_to_organization_individual_id_organization_id
    unique (individual_id, organization_id, relationship_type_id)
    );

create table if not exists endpoint_connection_type
(
    id         varchar(20) not null
    primary key,
    display    varchar(20) not null,
    definition varchar(200)
    );

create table if not exists environment_type
(
    id         varchar(10) not null
    primary key,
    display    varchar(20) not null,
    definition varchar(200)
    );

create table if not exists endpoint_instance
(
    id                          uuid         not null
    constraint pk_endpoint_instance
    primary key,
    ehr_vendor_id               uuid         not null
    constraint fk_endpoint_instance_ehr_vendor_id
    references ehr_vendor,
    address                     varchar(200) not null,
    endpoint_connection_type_id varchar(20)
    constraint fk_endpoint_instance_endpoint_connection_type_id
    references endpoint_connection_type,
    name                        varchar(200),
    description                 varchar(1000),
    environment_type_id         varchar(20)
    constraint fk_endpoint_instance_environment_type_id
    references environment_type
    );

create table if not exists clinical_organization
(
    organization_id      uuid
    constraint uc_clinical_organization_organization_id
    unique
    constraint fk_clinical_organization_organization_id
    references organization
    on delete cascade,
    npi                  bigint not null
    constraint pk_clinical_organization
    primary key
    constraint fk_clinical_organization_npi
    references npi
    on delete cascade,
    endpoint_instance_id uuid
    constraint fk_clinical_organization_endpoint_instance_id
    references endpoint_instance
);

create table if not exists endpoint
(
    id                   uuid         not null
    constraint pk_endpoint
    primary key,
    address              varchar(200) not null,
    endpoint_type_id     integer      not null
    constraint fk_endpoint_endpoint_type_id
    references endpoint_type,
    endpoint_instance_id uuid         not null
    constraint fk_endpoint_endpoint_instance_id
    references endpoint_instance,
    name                 varchar(200)
    );

create table if not exists organization_to_other_id
(
    npi              bigint       not null
    constraint fk_organization_to_other_id_npi
    references clinical_organization
    on delete cascade,
    other_id         varchar(100) not null,
    other_id_type_id integer      not null
    constraint fk_organization_to_other_id_other_id_type_id
    references other_id_type,
    state_code       varchar(2)   not null,
    issuer           varchar(200) not null,
    constraint pk_organization_to_other_id
    primary key (npi, other_id, other_id_type_id, issuer, state_code)
    );

create table if not exists organization_to_taxonomy
(
    npi        bigint      not null
    constraint fk_organization_to_taxonomy_npi
    references clinical_organization
    on delete cascade,
    nucc_code  varchar(10) not null
    constraint fk_organization_to_taxonomy_nucc_code
    references nucc,
    is_primary boolean default false,
    constraint pk_organization_to_taxonomy
    primary key (npi, nucc_code)
    );

create table if not exists provider_to_location
(
    location_id                 uuid                              not null
    constraint fk_provider_to_location_location_id
    references location,
    other_address_id            uuid
    constraint fk_provider_to_location_other_address_id
    references address,
    nucc_code                   integer,
    specialty_id                integer,
    id                          uuid    default gen_random_uuid() not null
    constraint pk_provider_to_location
    primary key,
    provider_role_code          varchar(10),
    other_phone_id              uuid
    constraint fk_individual_to_other_phone_id
    references individual_to_phone,
    other_endpoint_id           uuid
    constraint fk_provider_to_location_endpoint_id
    references endpoint,
    active                      boolean default true,
    provider_to_organization_id uuid
    constraint fk_provider_to_location_provider_to_organization_id
    references provider_to_organization
    );

create table if not exists mime_type
(
    id    serial
    primary key,
    value varchar(200)
    );

create table if not exists payload_type
(
    id          varchar(100) not null
    primary key,
    value       varchar(200),
    description varchar(1000)
    );

create table if not exists endpoint_to_payload
(
    endpoint_id     uuid         not null
    constraint fk_endpoint_to_payload_endpoint_id
    references endpoint,
    mime_type_id    integer
    constraint fk_endpoint_to_payload_mime_type_id
    references mime_type,
    payload_type_id varchar(200) not null
    constraint fk_endpoint_to_payload_type_id
    references payload_type,
    primary key (endpoint_id, payload_type_id)
    );

create table if not exists endpoint_instance_to_payload
(
    endpoint_instance_id uuid         not null
    constraint fk_endpoint_instance_to_payload_endpoint_instance_id
    references endpoint_instance,
    mime_type_id         integer
    constraint fk_endpoint_instance_to_payload_mime_type_id
    references mime_type,
    payload_type_id      varchar(200) not null
    constraint fk_endpoint_instance_to_payload_type_id
    references payload_type,
    primary key (endpoint_instance_id, payload_type_id)
    );

create table if not exists endpoint_instance_to_other_id
(
    endpoint_instance_id uuid         not null
    constraint fk_endpoint_instance_to_other_id_endpoint_instance_id
    references endpoint_instance
    on delete cascade,
    other_id             varchar(100) not null,
    system               varchar(200) not null,
    issuer_id            uuid         not null,
    constraint pk_endpoint_instance_to_other_id
    primary key (endpoint_instance_id, other_id, issuer_id)
    );

create table if not exists endpoint_to_other_id
(
    endpoint_id uuid         not null
    constraint fk_endpoint_to_other_id_endpoint_id
    references endpoint
    on delete cascade,
    other_id    varchar(100) not null,
    system      varchar(200) not null,
    issuer_id   uuid         not null,
    constraint pk_endpoint_to_other_id
    primary key (endpoint_id, other_id, issuer_id)
    );

create table if not exists provider_role
(
    code        varchar(10)  not null
    primary key,
    system      varchar(100) not null,
    display     varchar(100) not null,
    description varchar(200)
    );

create table if not exists location_to_endpoint_instance
(
    location_id          uuid not null
    constraint fk_location_to_endpoint_instance_location_id
    references location,
    endpoint_instance_id uuid not null
    constraint fk_location_to_endpoint_instance_endpoint_id
    references endpoint_instance,
    constraint pk_location_to_endpoint_instance
    primary key (location_id, endpoint_instance_id)
    );

