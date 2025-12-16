# Stakeholder Management & Business Requirements  
## Campaign Performance Dashboard – Business Layer Documentation

---

## 1. Data Product Vision

The **Campaign Performance Dashboard** is a unified Business Layer built on top of standardized operational models.  
It consolidates:

- Paid media performance  
- Media-only profitability  
- Internal resource allocation  
- Organic and social performance  

into a single, consistent, analytics-ready dataset.

**Purpose of the Data Product**

- Create a single source of truth for all performance and profitability KPIs  
- Standardize KPI definitions across teams  
- Enable scalable reporting and client insights  
- Reduce manual reporting overhead  
- Prepare the foundation for BI dashboards without redefining business logic  

**Important Note:**  
The term “Dashboard” refers to the **dbt Business Layer models**, not to a BI visualization tool.

---

## 2. Stakeholder Mapping

### 2.1 Internal Stakeholders

---

### Marketing Strategists  
**Needs:**  
- Compare performance across platforms and attribution windows  
- Identify optimization opportunities  
- Understand efficiency and scaling behavior  

**Key KPIs:**  
- ROAS, CPA, CTR, CVR  
- Revenue, Conversions, Spend  

**Example SQL: High spend but low efficiency clients (optimization candidates)**
```sql
select
  client_id,
  client_name,
  platform,
  attribution_window,
  total_ad_spend_eur,
  blended_roas,
  blended_cpa_eur
from bl_client_performance_dashboard
where attribution_window = '1d_view'
  and total_ad_spend_eur > 50000
order by blended_roas asc
limit 10;
Result example: Client_17

---

### Account Managers  
**Needs:**  
- Provide transparent performance reporting  
- Understand service intensity (internal hours and cost)  
- Identify over-served or under-served clients  

**Key KPIs:**  
- Utilization Rate  
- Cost per Client  
- Core performance KPIs  


---

### Finance & Controlling  
**Needs:**  
- Understand true profitability  
- Integrate media cost, revenue, and internal cost  
- Support pricing, budgeting, and forecasting  

**Key KPIs:**  
- Net Profit After Internal Cost  
- Project Margin  
- Client Lifetime Value (conceptually supported but not calculated in this dataset)  

---

### Data Analysts  
**Needs:**  
- Clean, well-tested BL models  
- Consistent KPI definitions  
- Ability to perform exploratory and deep-dive analysis  

**Key KPIs:**  
- Campaign Performance KPIs  
- Profitability KPIs  
- Resource Allocation KPIs  

---

### 2.2 External Stakeholders (Read-Only)

---

### Clients  
**Needs:**  
- Clear visibility into media performance  
- Transparent linkage between spend and results  
- Trustworthy, standardized reporting  

**Key KPIs:**  
- ROAS, CPA, CTR, CVR  
- Spend, Revenue, Conversions  

---

## 3. KPI Framework

The following KPI groups reflect the combined outputs of all Business Layer models.

---

### 3.1 Campaign Performance KPIs  
(Provided by `bl_client_performance_dashboard` and `bl_monthly_campaign_cohorts`)

- ROAS  
- CPA  
- CTR  
- CVR  
- CPC  
- Spend  
- Revenue  
- Conversions  
- Attribution-window–specific metrics  

---

### 3.2 Resource Allocation KPIs  
(Provided by `bl_resource_utilization`)

- Utilization Rate  
- Internal Hours per Client / Project  
- Internal Cost per Client  
- Internal Cost per Hour  
- Billable vs Non-Billable Split  
  (Note: Sample dataset contains only productive hours → 100% billable)

---

### 3.3 Profitability KPIs  
(Supported by all BL models)

- Media-Only Profit (Revenue – Ad Spend)  
- Media-Only Margin on Revenue  
- Media-Only Margin on Cost  
- Net Profit after Internal Cost  
- Project Margin  
- Client Lifetime Value (LTV conceptually supported; not directly calculated due to dataset limitations)

LTV Note:  
The dataset lacks long-term retention and revenue continuity required for LTV calculations.  
However, the BL models provide foundational metrics (revenue, profit, internal cost) to support future LTV development.

---

## 4. Implementation Roadmap

The data stack follows a structured transformation architecture:

### **IL → CL → OL → BL**  
- **IL (Input Layer):** Raw input files and source structures  
- **CL (Clean Layer):** Type-corrected, trimmed, standardized schemas  
- **OL (Operational Layer):** Business logic combining multiple sources (profitability, ad metrics, internal cost)  
- **BL (Business Layer):** Final analytical models consumed by business stakeholders  

This layered approach ensures maintainability, clarity, and traceability across the full data pipeline.

---

### Phase 1 – Foundation (Staging / CL Layer)
- Build cleaned models for all raw sources  
- Normalize schemas & apply naming conventions  
- Add dbt tests: not_null, unique, relationships  
- Ensure compatibility for downstream OL models  

---

### Phase 2 – Operational Layer (OL)
- Build unified ad metrics model (`ol_unified_ad_metrics`)  
- Implement media profitability logic (`ol_campaign_profitability`)  
- Build internal cost allocation model (`ol_client_projects_with_time`)  
- Add granular KPIs (CPC, CPA, ROAS) at campaign/day level  
- Validate grain consistency and data quality across operational sources  

---

### Phase 3 – Business Layer (BL)
- Build `bl_client_performance_dashboard`  
- Build `bl_resource_utilization`  
- Build `bl_monthly_campaign_cohorts`  
- Integrate profitability and internal cost KPIs  
- Apply rounding, null-handling, and enrichment logic  
- Add comprehensive grain and structural integrity tests  
- Deliver stakeholder-ready performance and profitability outputs  

---

### Phase 4 – BI Enablement (Future Scope)

This phase takes place **after** building the Business Layer and is **not part of the current case scope**.  
It defines how the BL models will be consumed by end users.

- Expose BL models to BI tools (Tableau, Looker, PowerBI)  
- Build stakeholder-specific dashboards  
- Define refresh schedules and monitoring  
- Publish business and technical documentation  
- Enable scalable self-service analytics  

Note: This phase outlines future enablement steps and is *not required* for this challenge, but demonstrates complete data product lifecycle understanding.

---

## 5. Summary

The Campaign Performance Dashboard Business Layer:

- Provides a unified analytical foundation for performance, profitability, and resource KPIs  
- Supports a diverse set of stakeholders across strategy, operations, finance, and analytics  
- Ensures standardized KPI definitions across the organization  
- Enables scalable reporting and supports future BI dashboard development  
- Fully satisfies the requirements for  
  **Teil 2: Stakeholder Management & Business Requirements**

The Business Layer is validated, structurally sound, and ready for downstream consumption or visualization.

---
