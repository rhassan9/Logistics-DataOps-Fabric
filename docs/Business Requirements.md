# Business and analytical requirements

This document defines what the gold layer must answer, and records which questions the
source data cannot honestly support. It sits between the silver layer and the dimensional
design: the bus matrix, fact grains and measures are derived from it rather than from
whatever the data happens to contain.

---

## Source of requirements

This project uses a published dataset rather than data supplied by a business stakeholder.
The analytical scope is therefore derived from the dataset publisher's documented use cases
and sample questions, supplemented by what the profiling evidence shows the data can support.

**These are not requirements from an operating logistics organisation.** They are a defensible
analytical baseline for a portfolio project, and they are treated the way real requirements
would be: assessed for feasibility, scoped to what the data supports, and explicitly rejected
where it does not.

Requirements are organised as **stable analytical domains** rather than as reports. A domain
outlives any particular dashboard, which is what makes the dimensional model reusable. A new
report that fits an existing grain becomes a measure; one that needs a new attribute extends
a dimension; only a genuinely new business process needs a new fact.

---

## Analytical domains

| Domain | Business process | Primary consumer |
|---|---|---|
| Fleet economics | loads booked and executed | finance, executive |
| Delivery performance | pickup and delivery events | operations |
| Fuel | fuel purchases | operations, finance |
| Maintenance | service events | fleet management |
| Safety | incidents | compliance, HR |
| Customer and revenue | loads by customer | commercial |

---

## Requirements

Each requirement states the question, the measures needed, and a feasibility verdict backed
by the profiling evidence.

### FE-1 Route contribution after fuel

*Which routes generate the highest margin after fuel costs?*

Revenue is on the load; fuel cost is on purchases linked through the trip. Both sides of the
calculation exist and join cleanly.

**Supported, with a naming constraint.** The source has no driver pay, no equipment
purchase price and therefore no depreciation, and no overhead. What can be computed is
**contribution after fuel**, not profit margin, and the measure must be named accordingly.
Calling it margin would imply costs the data does not contain.

Maintenance cost does exist in full (labour hours, labour cost, parts cost, total cost,
downtime), but it sits at truck-event grain rather than load grain. Attributing it to a route
requires an allocation rule such as cost per mile applied to load miles. That is a modelled
choice rather than a source fact, so it is available as an extended measure (contribution
after fuel and allocated maintenance) with the allocation basis stated wherever it appears.

Measures: revenue, fuel surcharge, accessorial charges, fuel cost, contribution after fuel,
revenue per mile, fuel cost per mile.

### FE-2 Revenue and cost per asset

*What is revenue per truck? What is cost per mile?*

Supported. Trips carry distance and link to trucks; loads carry revenue; fuel and maintenance
carry cost. All resolve to the same conformed truck and date dimensions.

Measures: revenue per truck, miles per truck, cost per mile (fuel), cost per mile
(maintenance), loads per truck.

### FU-1 Fuel spend and price variance

*Where is fuel money going, and is the fleet paying consistent prices?*

Supported. 196,442 purchases carry gallons, price per gallon and total cost, each linked to a
trip, truck and driver. Price paid ranges from 3.15 to 5.00 per gallon, which is genuine
variation rather than a constant, so price analysis has something to find.

Fuel efficiency is reported at fleet and truck level only. Per-driver comparison is excluded
for the reason given under Not supported by the source data.

Measures: total fuel spend, gallons purchased, average price paid, price variance by location
and by month, spend by truck, spend concentration by purchase location, gallons per mile at
fleet level.

### DP-1 On-time performance by lane

*Which city pairs have the most reliable on-time delivery performance?*

Supported, and one of the strongest requirements in the set. Routes give 58 origin and
destination pairs; delivery events give 170,820 measured arrivals with a source on-time flag,
a signed arrival variance and a detention figure.

Measures: on-time rate, early rate, late rate, average arrival variance, arrival variance
distribution.

### DP-2 Facility and detention performance

*Where is time being lost, and how much detention is billable?*

Supported, and the highest-value finding in the data. 86.8 percent of deliveries incur some
detention, 61,785 events exceed the two-hour free period, and 2,987,151 minutes are billable
across the three years.

Detention measures facility performance after arrival; arrival variance measures carrier
punctuality. Profiling confirmed they are independent (correlation 0.0255), so they must be
reported as separate measures rather than combined into a single service metric.

Measures: average detention, billable detention minutes, detention by facility, detention by
event type.

### MA-1 Maintenance spend reporting

*Where is maintenance money going, and on which assets?*

**Supported descriptively only.** Spend, downtime and event counts are all present and can be
reported by truck, type, urgency and period. Cost per mile is meaningful because mileage
varies genuinely across the fleet.

The analytical question originally posed, how truck age and service interval affect cost and
failure, is **not supported**. Three relationships were tested and none exists:

| Tested | Result |
|---|---|
| Emergency share against miles since last service | flat across 17 buckets, 18 to 49 percent with no direction, within two standard errors of the 35 percent mean throughout |
| Cost and downtime against service urgency | total cost varies 0.7 percent across Emergency, Routine and Scheduled; labour hours identical; scheduled work has the *highest* downtime |
| Emergency share and cost against odometer | flat from 0 to 700,000 miles |
| Cost and downtime against truck model year | flat from 2015 to 2021; where it moves at all the newest trucks look most expensive, on 32 events against 1,871 for 2015 |

Urgency was assigned as an even three-way split across 2,920 records and carries no
consequence. It remains a valid slicer, but no measure may be built on the assumption that it
differentiates anything.

Measures: maintenance spend by truck, by type and by period; downtime hours; maintenance cost
per mile; event counts by urgency.

### SA-1 Incident patterns by driver experience

*What safety incident patterns exist by driver experience level?*

**Supported descriptively only.** There are 170 incidents across 150 drivers over three years.
Any breakdown by experience band, category and severity produces cells in single digits.

Silver separated `incident_category` into Safety, Regulatory, Asset and Service, because the
source `incident_type` mixes four unrelated concerns and an unqualified count of "safety
incidents" would include customer complaints.

Measures: incident count by category, severity and cause; preventable rate; at-fault rate;
claim cost. All reported with an explicit sample size caveat.

### CR-1 Customer revenue and terms

*Which customers have the highest revenue per load and the best payment terms?*

Supported. `credit_terms_days` exists on the customer record with four distinct values, so
payment terms are available as a dimension attribute rather than something that needs
inventing.

Measures: revenue per load, total revenue, load count, revenue concentration by customer,
average credit terms.

### UT-1 Asset utilisation

*Which assets are underutilised? What is the driver-to-truck ratio?*

Supported at a descriptive level. Trips, miles and active days per truck and per driver are
all derivable from the same conformed dimensions.

"Ideal fleet size" asks how many trucks the business should own. Answering it needs a demand
forecast and the cost of owning a truck weighed against the cost of failing to cover a load.
Neither exists in the source, and neither is a warehouse's responsibility. Gold reports the
utilisation measures that such an analysis would take as input.

Measures: trips per truck, miles per truck, active days, idle hours, utilisation rate.

---

## Not supported by the source data

Recording these matters more than the requirements that work. Each was checked against
measured evidence rather than assumed.

### Driver fuel-efficiency ranking

`average_mpg` spans 5.5 to 7.5 across 85,410 trips, with a standard deviation of 0.58. In a
real fleet this figure moves substantially with load weight, terrain, weather, vehicle age and
driver behaviour. Here it does not.

A driver leaderboard built on this column would rank 150 drivers inside a two-MPG band and
present noise as performance. **Excluded from gold.** Fleet-level fuel consumption remains a
valid measure; per-driver comparison does not.

### Seasonal analysis

*How do seasonal patterns affect utilisation and revenue?*

A monthly load-count chart appears to show a pattern, with visible troughs each February.
Those troughs are month length: at 77.4 loads per day, a 28-day February yields about 2,167
loads against roughly 2,400 for a 31-day month.

Normalised to loads per day, the twelve months of the year range from 75.8 to 79.0, a spread
of 4 percent. Daily volume has a standard deviation near 8.5, and each month-of-year covers
about 93 days across the three years, giving a standard error near 0.9 on each monthly mean.
A 3.2 spread across twelve groups at that error is within ordinary random variation.

For scale, real freight seasonality moves 20 to 40 percent between peak and trough. This
dataset contains no seasonal signal worth planning around, and the finding is reported as
such rather than charted in a way that implies one exists.

### Driver turnover prediction

Only 26 of 150 drivers were ever terminated, all before May 2022, and no driver was hired
after December 2021. There are no turnover events inside the three-year fact window.

**No supporting data.** Gold carries tenure and employment status as dimension attributes;
turnover modelling is not possible.

### Equipment failure forecasting

Maintenance records carry type, cost and downtime but no failure labels, no component-level
detail and no sensor telemetry. There is nothing to forecast against.

Gold provides the maintenance history such a model would consume. The model itself is out of
scope and would require the telemetry source planned for a later milestone.

### Optimal preventive maintenance interval

A preventive maintenance interval is worth optimising only if stretching it raises the failure
rate. The empirical signature of that is the emergency share rising with miles since the last
service.

It does not rise. Across 17 mileage buckets the emergency share moves between 18 and 49
percent with no direction, and with roughly 35 events per bucket that entire range sits within
two standard errors of the mean.

Nor does failure carry a cost penalty: emergency, routine and scheduled work cost within 0.7
percent of each other, take the same labour hours, and scheduled work actually shows the
highest downtime.

With neither an interval effect nor a cost consequence, there is no optimum to find. The
monetary rate for downtime is also absent, so even a partial cost model is not possible.

---

## Out of scope by category

**Predictive modelling.** Gold's job is to provide trustworthy features and history. Models
built on those features belong outside the warehouse. Where a predictive use case appears in
the source documentation, the requirement here is the feature set, not the prediction.

**SQL technique exercises.** Window functions, CTEs and multi-table joins are implementation
techniques, not business requirements. They appear in gold where the requirement calls for
them, never as an end in themselves.

---

## Known data limitations affecting every domain

These surfaced during profiling and constrain how measures may be interpreted.

| Limitation | Effect |
|---|---|
| 18,105 fuel purchases exceed the truck's tank capacity | flagged in silver; excluded from tank utilisation measures |
| 7,450 trips report more idle time than elapsed time | flagged; idle as a share of duration is unusable, absolute idle hours are valid |
| 486 loads have a delivery timestamp before pickup | flagged; transit duration suppressed on those rows |
| `location_state` unreliable on fuel purchases and safety incidents | column dropped in silver; geography comes from facilities |
| Facility coordinates are city centroids | proximity and inter-facility distance are meaningless |
| Delivery events extend to 2025-01-03 while loads end 2024-12-31 | the date dimension needs a complete-period flag, or monthly trends show a false collapse |
| No dimension changes anywhere in the source | SCD Type 2 is implemented but has no history to capture until generated data is added |
| `service_urgency` is an even random split with no cost, labour or downtime consequence | valid as a slicer, but no measure may assume it differentiates |
| Monthly load volume varies only 4 percent once normalised to loads per day | no seasonal analysis; apparent monthly troughs are month length |

---

## Reconciliation requirement

The source ships two pre-computed monthly aggregates, `driver_monthly_metrics` and
`truck_utilization_metrics`. They are OLAP artifacts and do not enter the pipeline, but they
are loaded separately into the operations lakehouse as an expected baseline.

Gold rebuilds both from transactional data and reconciles against that baseline. Two metrics
have been verified as exactly reproducible: trip counts and total miles, across all 4,464
driver-months and 3,312 truck-months, with no rows on either side only. For those two
metrics, a mismatch in gold is a defect in gold rather than noise in the source. No such
claim is made for the remaining metrics until they have been tested the same way.

The remaining metrics on those tables (`average_mpg`, `total_fuel_gallons`,
`on_time_delivery_rate`, `average_idle_hours`, `utilization_rate`) have no documented formula.
They are reconciled after gold is built, and any that cannot be reproduced are recorded as
definition-unknown rather than reverse-engineered to force a match.

These aggregates exist for reconciliation, not performance. At 541,172 fact rows the
warehouse does not need pre-aggregation, and they are labelled as validation artifacts so
nobody mistakes them for an optimisation.

---

## What this document feeds

Requirements above drive, in order: the bus matrix mapping business processes to conformed
dimensions, the grain declaration for each fact, the dimension and measure design, and
finally the warehouse DDL.

Requirements are expected to change. The dimensional model is designed around the six stable
domains rather than around any current report, so a new question that fits an existing grain
becomes a measure, one needing a new attribute extends a dimension, and only a new business
process requires a new fact.