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


create table if not exists endpoint_connection_type
(
    id         varchar(20) not null
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

