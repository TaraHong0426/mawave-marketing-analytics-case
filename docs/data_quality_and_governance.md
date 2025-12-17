# Data Quality & Governance  
## Campaign Performance Dashboard – Business Layer

---

## 1. dbt Tests for Critical Data Quality

This section covers the dbt tests implemented across all layers:

- Cleansing Layer (CL)
- Operational Layer (OL)
- Business Layer (BL)
- Singular SQL tests
- YAML documentation
- Data lineage and governance

---

## 1.1 Cleansing Layer (CL) – Generic Tests

### cl_clients.ymls

```yaml
version: 2

models:

name: cl_clients
description: "Cleaned client master data."
columns:

name: client_id
description: "Unique client identifier."
tests:

not_null

unique

name: client_name
description: "Client name."
tests:

not_null
```

---

### cl_campaigns.yml
```yaml
version: 2

models:

name: cl_campaigns
description: "Cleaned campaign definitions."
columns:

name: campaign_id
description: "Unique campaign identifier."
tests:

not_null

unique

name: client_id
tests:

not_null

relationships:
to: ref('cl_clients')
field: client_id
```

---

## 1.2 Operational Layer (OL) – Generic Tests

### ol_unified_ad_metrics.yml

```yaml
version: 2

models:

name: ol_unified_ad_metrics
description: "Unified daily advertising metrics."
tests:

dbt_utils.unique_combination_of_columns:
combination_of_columns: ["campaign_id", "report_date"]

columns:

name: campaign_id
tests:

not_null

relationships:
to: ref('cl_campaigns')
field: campaign_id

name: report_date
tests:

not_null

name: ad_spend_eur
tests:

dbt_utils.expression_is_true:
expression: ">= 0"
```


---

### ol_campaign_profitability.yml

```yaml
version: 2

models:

name: ol_campaign_profitability
description: "Daily campaign profitability."
tests:

dbt_utils.unique_combination_of_columns:
combination_of_columns:
["client_id","campaign_id","platform","attribution_window","report_date"]

columns:

name: ad_spend_eur
tests:

dbt_utils.expression_is_true:
expression: ">= 0"

name: revenue_eur
tests:

dbt_utils.expression_is_true:
expression: ">= 0"
```

---

## 1.3 Business Layer (BL) – Generic Tests

### bl_client_performance_dashboard.yml

```yaml
version: 2

models:

name: bl_client_performance_dashboard
description: "Client KPIs across platforms and attribution windows."
tests:

dbt_utils.unique_combination_of_columns:
combination_of_columns: ["client_id","platform","attribution_window"]

columns:

name: total_ad_spend_eur
tests:

dbt_utils.expression_is_true:
expression: ">= 0"

name: total_revenue_eur
tests:

dbt_utils.expression_is_true:
expression: ">= 0"
```

---

### bl_resource_utilization.yml

```yaml
version: 2

models:

name: bl_resource_utilization
description: "Employee × client × project utilization model."
tests:

dbt_utils.unique_combination_of_columns:
combination_of_columns: ["employee_id","client_id","project_id"]

columns:

name: total_hours
tests:

dbt_utils.expression_is_true:
expression: ">= 0"
```

---

### bl_monthly_campaign_cohorts.yml

```yaml
version: 2

models:

name: bl_monthly_campaign_cohorts
description: "Monthly cohort performance + internal resource cost."
tests:

dbt_utils.unique_combination_of_columns:
combination_of_columns:
["client_id","platform","attribution_window","cohort_month"]

columns:

name: monthly_ad_spend_eur
tests:

dbt_utils.expression_is_true:
expression: ">= 0"
```

---

## 1.4 Singular Tests (Custom SQL)

### No negative values
```sql
select *
from {{ ref('bl_monthly_campaign_cohorts') }}
where monthly_ad_spend_eur < 0
or monthly_revenue_eur < 0;


Expected: **0 rows**
```
---

### Profit consistency check
```sql
with calc as (
select
media_only_profit_eur,
(revenue_eur - ad_spend_eur) as expected
from {{ ref('ol_campaign_profitability') }}
)
select *
from calc
where round(media_only_profit_eur - expected, 2) <> 0;
```

Expected: **0 rows**

---

## 2. YAML Documentation for Key Models

### bl_client_performance_dashboard.yml (documentation)
```yaml
version: 2

models:
  - name: bl_client_performance_dashboard
    description: >
      Business-layer model providing a unified client-level performance view
      across platforms and attribution windows.
    meta:
      grain: "1 row per client_id × platform × attribution_window"
      data_product: "Campaign Performance Dashboard"

    columns:
      - name: client_id
        description: "Unique client identifier."

      - name: platform
        description: "Advertising platform (e.g. Meta Ads, TikTok Ads)."
```


---

## 3. Data Lineage Overview

Layered architecture:

Integration Layer (IL)
→ Cleansing Layer (CL)
→ Operational Layer (OL)
→ Business Layer (BL)
→ BI Layer (out of scope)


### Lineage per BL Model

#### bl_client_performance_dashboard

cl_clients
cl_campaigns
ol_campaign_profitability
→ bl_client_performance_dashboard


#### bl_resource_utilization

cl_employees
cl_time_tracking
cl_projects
ol_client_projects_with_time
→ bl_resource_utilization


#### bl_monthly_campaign_cohorts

ol_campaign_profitability
ol_client_projects_with_time
→ bl_monthly_campaign_cohorts


---

## 4. Governance Implications

- Full transparency via dbt lineage  
- Data contracts enforced through tests  
- Documentation in YAML files  
- Stable CL / OL / BL boundaries  
- Strong foundations for scalable BI dashboards  

---

## 5. Summary

This document fulfills **Teil 3: Datenqualität & Governance** by providing:

- A complete dbt test strategy  
- Model documentation (YAML)  
- Data lineage for every BL model  
- Business logic validation via singular SQL tests  
- Governance structure for long-term maintainability  

---
