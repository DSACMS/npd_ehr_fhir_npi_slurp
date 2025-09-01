# FHIR Tenancy and Patient Search Scope

This document explains how tenancy affects what you see when searching for patients across different FHIR deployment models.
Based on ChatGPT conversation with Fred Trotter Sep 1 2025.

---

## Scenario 1: SaaS, Single Base URL

```diagram
        https://api.platform.athenahealth.com/fhir/r4
                       │
 ┌─────────────────────┼─────────────────────┐
 │                     │                     │
Org A (Practice 1)   Org B (Hospital)      Org C (Clinic)
Patients:            Patients:             Patients:
- John Smith A       - Jane Doe B          - John Smith C
- Mary Jones A       - John Smith B        - ...
```

One **base URL**, but **data partitioned per tenant**.  
When you search `/Patient` with a token from Org A, you **only see Org A’s patients**.  
You won’t see John Smith B or C, even if they exist in other tenants.

---

## Scenario 2: On-prem, Multiple Base URLs

```diagram
  https://ehr1.hospital.org/fhir/r4   (Hospital 1)
  https://ehr2.clinic.net/fhir/r4     (Clinic 2)
  https://ehr3.practice.com/fhir/r4   (Practice 3)

 Each base has:
 - Its own auth
 - Its own /Patient database
 - No shared search
```

To find “John Smith” across multiple hospitals, your app must query each base URL separately.  
No automatic aggregation. Tokens are only valid per base.

---

## Scenario 3: SaaS Base + Multiple Endpoint Resources

```diagram
   https://api.platform.athenahealth.com/fhir/r4
                       │
         Organization/6e55df... (Women's Health of Illinois)
                       │
          ┌────────────┴────────────┐
          │                         │
Endpoint/27a4e967...        Endpoint/a6b028cd...
(type: FHIR REST)           (type: Direct address)
```

Endpoints are **pointers to services for one organization**, not separate patient universes.  
They tell you *how* to connect for a given org, but **don’t expand your search scope**.  
Your `/Patient` search is still confined to the tenant context tied to your token.

---

## Key Takeaway

- **Base URL = security & data boundary.**  
  - Different base → completely different patient set.  
  - Same base (SaaS) → still partitioned; token scopes you to one org’s patients.  

- **Endpoints = metadata.** They describe how an org exposes services (Direct, FHIR REST, etc.), but don’t merge or expand patient search.

- **Patient search scope** is **never global across all tenants**. You only see patients in the tenant bound to your access token.  

---

So when you search for **“John Smith, SSN 123-123-1234”**:  

- In SaaS (like Athena): you’ll only see that patient if he exists in the tenant your token is scoped to.  
- In on-prem: you’ll only see him if you query the right base URL.  
- In both: you won’t get “all Johns across all customers.” That requires a separate aggregation layer (HIE, payer, or vendor-level service).
