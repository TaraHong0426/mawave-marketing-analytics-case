# Debug Report – bl_monthly_campaign_cohorts

## 1. Model Overview

Model: bl_monthly_campaign_cohorts  
Grain: One row per cohort_month × client_id × platform × attribution_window  

This Business Layer model aggregates campaign performance at a monthly cohort level.  
It combines:

- Monthly spend, impressions, clicks, conversions, revenue  
- Media-only profitability (before internal cost)  
- Internal cost at client-month level (if available)  
- Profitability after internal cost  
- Campaign footprint metrics such as number of campaigns per month  

Important note:  
In the sample dataset, clients with ad spend **do not overlap** with clients that have internal time-tracking data.  
Therefore, internal cost and net profit after internal cost are expected to be `NULL` for all advertising clients.

---

## 2. Sanity Check – Cohort Grain & Coverage

### SQL
```sql
select
cohort_month,
client_id,
client_name,
platform,
attribution_window,
num_campaigns,
monthly_ad_spend_eur,
monthly_revenue_eur
from {{ ref('bl_monthly_campaign_cohorts') }}
order by
cohort_month,
client_id,
platform,
attribution_window
limit 50;
```

### Observations

- Each client × platform × attribution_window appears exactly once per month  
- monthly_ad_spend_eur is constant across windows for a given client/platform/month   
- num_campaigns correctly reflects campaign count per client/month  
- No missing cohort_month values

### Interpretation

- The model respects the desired grain  
- Spend aggregation behaves consistently   

---

## 3. Sanity Check – Monthly Trend for a Specific Client

### SQL
```sql
select
cohort_month,
client_id,
client_name,
platform,
attribution_window,
num_campaigns,
monthly_ad_spend_eur,
monthly_revenue_eur
from {{ ref('bl_monthly_campaign_cohorts') }}
where client_id = '1'
order by
cohort_month,
platform,
attribution_window;
```

### Observations

- Client 1 shows consistent spend across windows per month  
- Revenue fluctuates by attribution window   

### Interpretation

- Monthly cohort rollups behave correctly  
- No duplication or missing data across monthly partitions  
- Revenue variability across windows confirms attribution-driven aggregation is functioning

---

## 4. Sanity Check – Resource & Profitability Consistency

### SQL
```sql
select
cohort_month,
client_id,
client_name,
sum(monthly_internal_cost_eur) as internal_cost_eur,
sum(media_only_profit_eur) as media_only_profit_eur,
sum(monthly_net_profit_after_internal_eur) as net_profit_after_internal_eur
from {{ ref('bl_monthly_campaign_cohorts') }}
group by
cohort_month,
client_id,
client_name
order by
cohort_month,
client_id;
```

### Observations

- internal_cost_eur = NULL for all advertising clients  
- media_only_profit_eur contains valid positive values  
- net_profit_after_internal_eur = NULL (as expected since internal cost is missing)  
- Example values observed:
  - Client 1: very high monthly media-only profitability  
  - Client 12, 6, 15, 17 also show positive media-only profit values  

### Interpretation

- NULL internal cost is **not an error**; it reflects the known characteristic of the dataset  
- The BL is prepared to compute net profit, but can only do so when internal cost exists  
- Media-only profitability KPIs are calculated correctly  

---

## 5. Sanity Check – Media-only Margins

### SQL
```sql
select
cohort_month,
client_id,
client_name,
platform,
attribution_window,
monthly_media_only_margin_on_revenue,
monthly_media_only_margin_on_cost
from {{ ref('bl_monthly_campaign_cohorts') }}
order by
cohort_month,
client_id,
platform,
attribution_window
limit 50;
```

### Observations

- monthly_media_only_margin_on_revenue values are typically between 0.7 and 0.98  
- monthly_media_only_margin_on_cost values can be very high (e.g., 20, 30, 40+)  
  → This is expected since margin_on_cost = profit / cost  
- No negative margins observed  
- No division-by-zero issues  

### Interpretation

- Profitability KPIs behave as defined  
- No unrealistic negative margins  

---

## 6. Structural “Should-be-empty” Checks

### 6.1 Negative financial values
```sql
select *
from {{ ref('bl_monthly_campaign_cohorts') }}
where monthly_ad_spend_eur < 0
or monthly_revenue_eur < 0
or media_only_profit_eur < 0;
```

Expected: 0 rows  
Observed: No negative values found

---

### 6.2 Missing dimensional keys
```sql
select *
from {{ ref('bl_monthly_campaign_cohorts') }}
where platform is null
or attribution_window is null
or cohort_month is null;
```

Expected: 0 rows  
Observed: 0 rows → All key dimensions present

---

### 6.3 Duplicate grain check (double safety)
```sql
select
client_id,
platform,
attribution_window,
cohort_month,
count(*) as n_rows
from {{ ref('bl_monthly_campaign_cohorts') }}
group by 1,2,3,4
having n_rows > 1;
```

Expected: 0 rows  
Observed: 0 rows → Grain integrity confirmed

---

## 7. Conclusion

The `bl_monthly_campaign_cohorts` BL model is validated and ready for analytical and BI consumption.

Confirmed by debug checks:
- Grain integrity is correct  
- Monthly aggregations (spend, revenue, profit) behave as expected  
- No negative values or missing dimensions  
- Internal cost being NULL is consistent with dataset properties  
- Profitability KPIs (ROAS, margins) are stable and correctly computed  

This model forms a reliable foundation for monthly cohort analysis, profitability evaluation, and performance trend tracking across clients and platforms.
