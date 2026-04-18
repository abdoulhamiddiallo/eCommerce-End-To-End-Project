# DAX Measures — Ecommerce Fabric End-to-End Analytics

> This file centralizes all DAX measures used in the semantic model and Power BI executive dashboard.
> It is structured by business domain to improve readability, maintainability, and analytical transparency.

---

## 1. Sales & Revenue Measures

### Gross Sales
```DAX
Gross Sales =
SUM(gold_fact_orders[Gross Sales])
```

### Net Sales
```DAX
Net Sales =
SUM(gold_fact_orders[Net Sales])
```

### Discount Amount
```DAX
Discount Amount =
SUM(gold_fact_orders[Discount Amount])
```

### Shipping Cost
```DAX
Shipping Cost =
SUM(gold_fact_orders[Shipping Cost])
```

### Tax Amount
```DAX
Tax Amount =
SUM(gold_fact_orders[Tax Amount])
```

### Refund Amount
```DAX
Refund Amount =
SUM(gold_fact_orders[Refund Amount])
```

### Chargeback Amount
```DAX
Chargeback Amount =
SUM(gold_fact_orders[Chargeback Amount])
```

### Net Revenue
```DAX
Net Revenue =
[Net Sales] - [Refund Amount] - [Chargeback Amount]
```

### Net Revenue Previous Period
```DAX
Net Revenue Previous Period =
CALCULATE(
    [Net Revenue],
    DATEADD(gold_dim_date[Date], -1, YEAR)
)
```

### Net Revenue Variance %
```DAX
Net Revenue Variance % =
DIVIDE(
    [Net Revenue] - [Net Revenue Previous Period],
    [Net Revenue Previous Period]
)
```

### Net Revenue Variance Color
```DAX
Net Revenue Variance Color =
IF(
    [Net Revenue Variance %] >= 0,
    "#107C41",
    "#D13438"
)
```

### Net Revenue Variance Display
```DAX
Net Revenue Variance Display =
VAR v = [Net Revenue Variance %]
RETURN
IF(
    ISBLANK(v),
    BLANK(),
    IF(v >= 0, "+", "") & FORMAT(v, "0.0%")
)
```

### Net Revenue Variance Label
```DAX
Net Revenue Variance Label =
"vs Previous Period"
```

---

## 2. Order Measures

### Total Orders
```DAX
Total Orders =
DISTINCTCOUNT(gold_fact_orders[Order ID])
```

### Average Order Value
```DAX
Average Order Value =
DIVIDE([Net Sales], [Total Orders])
```

### Average Order Value Previous Period
```DAX
Average Order Value Previous Period =
CALCULATE(
    [Average Order Value],
    DATEADD(gold_dim_date[Date], -1, YEAR)
)
```

### Average Order Value Variance
```DAX
Average Order Value Variance =
[Average Order Value] - [Average Order Value Previous Period]
```

### Average Order Value Variance %
```DAX
Average Order Value Variance % =
DIVIDE(
    [Average Order Value] - [Average Order Value Previous Period],
    [Average Order Value Previous Period]
)
```

### Average Order Value Variance Color
```DAX
Average Order Value Variance Color =
IF(
    [Average Order Value Variance %] >= 0,
    "#107C41",
    "#D13438"
)
```

### Average Order Value Variance Display
```DAX
Average Order Value Variance Display =
VAR v = [Average Order Value Variance %]
RETURN
IF(
    ISBLANK(v),
    BLANK(),
    IF(v >= 0, "+", "") & FORMAT(v, "0.0%")
)
```

### Average Order Value Variance Label
```DAX
Average Order Value Variance Label =
"vs Previous Period"
```

### Payment Attempts
```DAX
Payment Attempts =
SUM(gold_fact_orders[Payment Attempts])
```

### Orders Fully Paid
```DAX
Orders Fully Paid =
CALCULATE(
    DISTINCTCOUNT(gold_fact_orders[Order ID]),
    gold_fact_orders[Fully Paid] = TRUE()
)
```

### Fully Paid Rate
```DAX
Fully Paid Rate =
DIVIDE([Orders Fully Paid], [Total Orders])
```

---

## 3. Customer Measures

### Total Customers
```DAX
Total Customers =
DISTINCTCOUNT(gold_fact_orders[Customer ID])
```

### Total Customers Previous Period
```DAX
Total Customers Previous Period =
CALCULATE(
    [Total Customers],
    DATEADD(gold_dim_date[Date], -1, YEAR)
)
```

### Total Customers Variance
```DAX
Total Customers Variance =
[Total Customers] - [Total Customers Previous Period]
```

### Total Customers Variance %
```DAX
Total Customers Variance % =
DIVIDE(
    [Total Customers] - [Total Customers Previous Period],
    [Total Customers Previous Period]
)
```

### Total Customers Variance Arrow
```DAX
Total Customers Variance Arrow =
VAR v = [Total Customers Variance %]
RETURN
IF(
    ISBLANK(v),
    BLANK(),
    IF(v >= 0, "▲", "▼")
)
```

### Total Customers Variance Color
```DAX
Total Customers Variance Color =
IF(
    [Total Customers Variance %] >= 0,
    "#107C41",
    "#D13438"
)
```

### Total Customers Variance Display
```DAX
Total Customers Variance Display =
VAR v = [Total Customers Variance %]
RETURN
IF(
    ISBLANK(v),
    BLANK(),
    IF(v >= 0, "+", "") & FORMAT(v, "0.0%")
)
```

### Total Customers Variance Display With Arrow
```DAX
Total Customers Variance Display With Arrow =
VAR v = [Total Customers Variance %]
RETURN
IF(
    ISBLANK(v),
    BLANK(),
    IF(v >= 0, "▲ ", "▼ ") & FORMAT(v, "0.0%")
)
```

### Total Customers Variance Label
```DAX
Total Customers Variance Label =
"vs Previous Period"
```

---

## 4. Support & Service Measures

### Total Tickets
```DAX
Total Tickets =
DISTINCTCOUNT(gold_fact_support_tickets[Ticket ID])
```

### Average CSAT
```DAX
Average CSAT =
AVERAGE(gold_fact_support_tickets[CSAT Score])
```

### Average CSAT Display
```DAX
Average CSAT Display =
IF(
    ISBLANK([Average CSAT]),
    BLANK(),
    FORMAT([Average CSAT], "0.00") & " / 5"
)
```

### Average CSAT Previous Period
```DAX
Average CSAT Previous Period =
CALCULATE(
    [Average CSAT],
    DATEADD(gold_dim_date[Date], -1, YEAR)
)
```

### Average CSAT Variance
```DAX
Average CSAT Variance =
[Average CSAT] - [Average CSAT Previous Period]
```

### Average CSAT Variance %
```DAX
Average CSAT Variance % =
DIVIDE(
    [Average CSAT] - [Average CSAT Previous Period],
    [Average CSAT Previous Period]
)
```

### Average CSAT Variance Color
```DAX
Average CSAT Variance Color =
IF(
    [Average CSAT Variance] >= 0,
    "#107C41",
    "#D13438"
)
```

### Average CSAT Variance Display
```DAX
Average CSAT Variance Display =
VAR v = [Average CSAT Variance]
RETURN
IF(
    ISBLANK(v),
    BLANK(),
    IF(v >= 0, "+", "") & FORMAT(v, "0.00")
)
```

### Average CSAT Variance Label
```DAX
Average CSAT Variance Label =
"vs Previous Period"
```

### Average First Response (Minutes)
```DAX
Average First Response (Minutes) =
AVERAGE(gold_fact_support_tickets[First Response (Minutes)])
```

### Average Resolution Time (Minutes)
```DAX
Average Resolution Time (Minutes) =
AVERAGE(gold_fact_support_tickets[Resolution Time (Minutes)])
```

### Escalated Tickets
```DAX
Escalated Tickets =
CALCULATE(
    DISTINCTCOUNT(gold_fact_support_tickets[Ticket ID]),
    gold_fact_support_tickets[Escalated] = TRUE()
)
```

### Escalation Rate
```DAX
Escalation Rate =
DIVIDE([Escalated Tickets], [Total Tickets])
```

### SLA Breach Tickets
```DAX
SLA Breach Tickets =
CALCULATE(
    DISTINCTCOUNT(gold_fact_support_tickets[Ticket ID]),
    gold_fact_support_tickets[SLA Breach] = TRUE()
)
```

### SLA Breach Rate
```DAX
SLA Breach Rate =
DIVIDE([SLA Breach Tickets], [Total Tickets])
```

---

## 5. Notes

### Time Intelligence Logic
All previous-period comparisons use:
```DAX
DATEADD(gold_dim_date[Date], -1, YEAR)
```

### Color Convention
- Positive variance → `#107C41`
- Negative variance → `#D13438`

### KPI Display Logic
- Revenue, Customers, and Average Order Value use percentage variance displays
- CSAT uses absolute score delta display
- Customer KPI also includes optional arrow-based display
