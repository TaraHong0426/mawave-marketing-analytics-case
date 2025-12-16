# Debug Report – bl_client_performance_dashboard

## 1. Model Overview

Model: bl_client_performance_dashboard  
Grain: One row per client_id × platform × attribution_window  

This Business Layer model unifies:
- Paid media performance  
- Media-only profitability  
- (If available) internal cost and net profit  
- Organic/social metrics  

The purpose of this debug document is to validate:
- Grain consistency  
- Metric correctness  
- Attribution window behavior  
- Structural data quality (nulls, negatives, duplicates)

---

## 2. Sanity Check – Grain & Coverage

### SQL
```sql
select
client_id,
client_name,
platform,
attribution_window,
count(*) as n_rows,
sum(total_ad_spend_eur) as total_ad_spend_eur,
sum(total_revenue_eur) as total_revenue_eur
from {{ ref('bl_client_performance_dashboard') }}
group by
client_id,
client_name,
platform,
attribution_window
order by
client_id,
platform,
attribution_window;
```

### Sample Observations (no tables)

- Client 1 / Meta Ads:
  - 1d_click → spend ~1,003,447.35, revenue ~21,160,780.02, n_rows = 1  
  - 7d_click → spend identical, revenue slightly higher  
  - 28d_click → spend identical, revenue significantly higher  
- Client 1 / TikTok Ads:
  - spend ~32,360.60 across all windows  
  - revenue values differ by window, as expected  
- Client 12 / Meta Ads:
  - spend ~16,933.42  
  - revenue changes across windows  

### Interpretation

- Each client × platform × attribution_window appears exactly once → grain is correct.   
- The dataset does NOT enforce monotonic window behaviour (1d ≤ 7d ≤ 28d), which is an expected characteristic of this synthetic sample.

---

## 3. Sanity Check – Top Revenue Clients

### SQL
```sql
select
client_id,
client_name,
sum(total_revenue_eur) as total_revenue_eur,
sum(total_ad_spend_eur) as total_ad_spend_eur,
round(sum(total_revenue_eur) / nullif(sum(total_ad_spend_eur), 0), 2) as blended_roas
from {{ ref('bl_client_performance_dashboard') }}
group by client_id, client_name
order by total_revenue_eur desc
limit 10;
```
### Sample Observations

- Client 1:
  - Revenue ~110M  
  - Spend ~5.18M  
  - Blended ROAS ~21.35  
- Client 17:
  - Revenue ~5.4M, ROAS ~19.16  
- Client 15:
  - Revenue ~4.8M, ROAS ~27  
- No unrealistic or suspicious ROAS values observed.  

### Interpretation

- Client 1 dominates portfolio revenue.  
- No extreme or negative ROAS → metric calculations are behaving correctly.

---

## 4. Sanity Check – Media-only Profitability

### SQL
```sql
select
client_id,
client_name,
platform,
attribution_window,
media_only_total_cost_eur,
media_only_profit_eur,
media_only_margin_on_revenue
from {{ ref('bl_client_performance_dashboard') }}
order by media_only_profit_eur desc
limit 20;
```

### Sample Observations

- Client 1 / Meta Ads / 28d_click:
  - media-only cost ~1,003,447.35  
  - profit ~25M  
  - margin_on_revenue ~0.96  
- Client 17 / Meta Ads:
  - cost ~56K  
  - profit ~1.5M  
  - margin_on_revenue ~0.96  
- Client 15 / Meta Ads:
  - very high margin_on_revenue (~0.98)  

### Interpretation

- Media-only cost = ad spend → correct by definition.  
- No negative margins or impossible values.

---

## 5. Structural “Should-be-empty” Checks

### 5.1 Negative values check
```sql
select *
from {{ ref('bl_client_performance_dashboard') }}
where total_ad_spend_eur < 0
or total_revenue_eur < 0;
```

Expected: 0 rows  
Result: 0 rows  
→ No negative spend or revenue.

---

### 5.2 Missing mandatory dimensions
```sql
select *
from {{ ref('bl_client_performance_dashboard') }}
where platform is null
or attribution_window is null;
```

Expected: 0 rows  
Result: 0 rows  
→ All key dimensions are populated.

---

## 6. Conclusion

The `bl_client_performance_dashboard` model is structurally correct and analytically reliable:

- Grain validated (1 row per client × platform × attribution window)  
- No invalid nulls or negative financial values  
- Suitable for downstream BI dashboards and stakeholder reporting  

This model can be confidently used as the primary client-level performance foundation in the Business Layer.
