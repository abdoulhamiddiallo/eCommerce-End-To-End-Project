## Architecture: Medallion (Bronze / Silver / Gold)
Execution: PySpark Notebook (Fabric Lakehouse)


## Notebook Initialization — Environment Setup & Data Sources Configuration

This section initializes the notebook environment and defines the foundational configuration required for the entire data pipeline.

The objective of this step is to:
- standardize access to source datasets,
- centralize input paths for maintainability,
- ensure consistency across Bronze ingestion processes,
- prepare the notebook for scalable and production-ready execution within Microsoft Fabric.

### Key Components

**1. PySpark Imports**
We import core PySpark modules required throughout the pipeline:
- `functions` for transformations,
- `types` for schema definition,
- `window` for advanced analytical computations.

These imports establish the technical foundation for all subsequent data engineering operations.

**2. Source Paths Definition**
A centralized dictionary (`SOURCE_PATHS`) is defined to map each business domain to its corresponding raw data location.

Domains covered:
- Customers
- Orders
- Payments
- Support Tickets
- Web



```python

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

SOURCE_PATHS = {
    "customers": "Files/data_customers/customers.csv",
    "orders": "Files/data_orders/orders.csv",
    "payments": "Files/data_payments/payments.csv",
    "support_tickets": "Files/data_support_tickets/support_tickets.csv",
    "web_activities": "Files/data_web_activities/web_activities.csv",
}

print("Notebook initialized successfully.")
print("Using physical Delta paths in Tables/.")

```


## Core Utility Functions — Reusable Data Engineering Framework

This section defines a set of reusable utility functions that standardize data ingestion, transformation, and persistence across the entire pipeline.

The objective is to build a **modular, scalable, and production-ready foundation** that can be consistently applied across all business domains (customers, orders, payments, support, web activity).

These utilities are designed to:
- reduce code duplication,
- enforce consistent data standards,
- improve readability and maintainability,
- support enterprise-grade data engineering practices.

---

### 1. Data Ingestion — `read_csv`

A standardized function to ingest raw CSV files into Spark DataFrames.

Key design choices:
- header enabled for proper column naming,
- schema inference for flexibility during ingestion,
- multiline disabled to optimize performance and avoid parsing inconsistencies.

This abstraction ensures that all raw data ingestion follows a **consistent and controlled pattern**.

---

### 2. Column Standardization — `normalize_colnames`

This function enforces a consistent naming convention across all datasets.

Transformations applied:
- trimming spaces,
- converting to lowercase,
- replacing special characters with underscores,
- removing non-analytics-friendly symbols.

This step is critical to:
- avoid downstream errors,
- ensure compatibility with SQL / BI tools,
- standardize the semantic layer.

---

### 3. Ingestion Metadata — `add_ingestion_metadata`

Adds technical lineage and audit columns to each dataset.

Metadata captured:
- `_source_system` → origin of the data,
- `_source_entity` → business domain,
- `_source_file` → file path,
- `_ingestion_ts` → ingestion timestamp,
- `_load_date` → load date.

These fields enable:
- data traceability,
- lineage tracking,
- auditability,
- operational monitoring.

This is a key requirement for **production-grade data platforms**.

---

### 4. Persistence Layer — Delta Tables

#### `write_delta_to_tables`
Handles writing DataFrames into Delta format using physical paths (`Tables/`).

Design considerations:
- supports overwrite mode,
- enforces schema overwrite when needed,
- ensures compatibility with Fabric Lakehouse architecture.

#### `read_delta_from_tables`
Provides a standardized method to read previously stored Delta tables.

Together, these functions ensure:
- consistency in storage,
- reliability of data access,
- alignment with medallion architecture (Bronze / Silver / Gold).

---

### 5. Data Type Safety — Date & Timestamp Handling

#### `safe_to_timestamp`
#### `safe_to_date`

These functions safely convert columns into proper date/time formats by:
- attempting multiple parsing patterns,
- using fallback logic (`coalesce`) to prevent failures,
- preserving data when format inconsistencies exist.

This approach is essential in real-world scenarios where:
- source data is heterogeneous,
- formats are inconsistent,
- strict casting would otherwise break pipelines.

---

### 6. Data Observability — `show_shape`

A lightweight utility to quickly inspect dataset structure:
- number of rows,
- number of columns.

This function supports:
- rapid debugging,
- validation checkpoints,
- pipeline transparency during development.

---

## Why This Matters

This utility layer transforms the notebook from a simple script into a **structured data engineering framework**.

It ensures that:
- all transformations are standardized,
- the pipeline is maintainable and scalable,
- the data model is reliable for downstream analytics,
- the project aligns with best practices in modern data platforms (Fabric, Delta Lake, Medallion Architecture).

In a production context, this layer acts as the backbone of the entire data pipeline.



```python

def read_csv(path: str):
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", False)
        .load(path)
    )

def normalize_colnames(df):
    for c in df.columns:
        new_c = (
            c.strip()
             .lower()
             .replace(" ", "_")
             .replace("-", "_")
             .replace("/", "_")
             .replace("(", "")
             .replace(")", "")
             .replace("%", "pct")
        )
        df = df.withColumnRenamed(c, new_c)
    return df

def add_ingestion_metadata(df, entity_name: str, source_path: str):
    return (
        df.withColumn("_source_system", F.lit("synthetic_ecommerce"))
          .withColumn("_source_entity", F.lit(entity_name))
          .withColumn("_source_file", F.lit(source_path))
          .withColumn("_ingestion_ts", F.current_timestamp())
          .withColumn("_load_date", F.current_date())
    )

def write_delta_to_tables(df, table_name: str, mode: str = "overwrite"):
    path = f"Tables/{table_name}"
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )
    print(f"Written successfully to {path}")

def read_delta_from_tables(table_name: str):
    path = f"Tables/{table_name}"
    return spark.read.format("delta").load(path)

def safe_to_timestamp(df, col_name):
    if col_name in df.columns:
        return df.withColumn(
            col_name,
            F.coalesce(
                F.to_timestamp(F.col(col_name)),
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd")
            )
        )
    return df

def safe_to_date(df, col_name):
    if col_name in df.columns:
        return df.withColumn(
            col_name,
            F.coalesce(
                F.to_date(F.col(col_name)),
                F.to_date(F.col(col_name), "yyyy-MM-dd")
            )
        )
    return df

def show_shape(df, name):
    print(f"{name} -> rows: {df.count()}, cols: {len(df.columns)}")

```


## Bronze Layer — Raw Data Ingestion & Standardization

This step marks the beginning of the Bronze layer, where raw data is ingested from source files and prepared for initial processing.

The objective is to:
- load raw datasets from their respective source paths,
- apply immediate structural standardization,
- ensure consistency across all business domains,
- establish a reliable foundation for downstream transformations.

---

### Data Ingestion Strategy

Each dataset is loaded using the standardized `read_csv` function and immediately passed through the `normalize_colnames` utility.

This approach ensures that:
- ingestion logic is centralized and reusable,
- column naming conventions are enforced at the earliest stage,
- all datasets share a consistent structural format.

---

### Business Domains Covered

The following core entities are ingested:

- **Customers** → customer master data  
- **Orders** → transactional order data  
- **Payments** → financial transaction records  
- **Support Tickets** → customer service interactions  
- **Web Activities** → digital behavior and engagement data  

This multi-domain ingestion reflects a **holistic data model**, enabling end-to-end analytics across:
- customer lifecycle,
- revenue generation,
- operational performance,
- digital funnel analysis.

---

### Column Standardization

All datasets undergo column normalization, which includes:
- converting names to lowercase,
- replacing spaces and special characters,
- ensuring compatibility with Spark, SQL, and BI tools.

This step is critical to:
- avoid downstream transformation errors,
- simplify joins and aggregations,
- ensure a clean and consistent schema across the pipeline.

---

## Why This Matters

By combining ingestion and standardization in a single step, we ensure that the Bronze layer is not only a raw landing zone, but also a **controlled and structured entry point** into the data platform.

This design enables:
- faster development,
- improved data quality from the start,
- seamless transition into Silver transformations,
- alignment with modern data engineering best practices (Medallion Architecture in Microsoft Fabric).

This is the foundation upon which the entire analytical pipeline is built.



```python

customers_src = normalize_colnames(read_csv(SOURCE_PATHS["customers"]))
orders_src = normalize_colnames(read_csv(SOURCE_PATHS["orders"]))
payments_src = normalize_colnames(read_csv(SOURCE_PATHS["payments"]))
support_src = normalize_colnames(read_csv(SOURCE_PATHS["support_tickets"]))
web_src = normalize_colnames(read_csv(SOURCE_PATHS["web_activities"]))

```


## Bronze Layer — Initial Data Validation & Observability

At this stage, we perform a first-level validation of the ingested datasets to ensure that the data has been correctly loaded and structured.

The objective of this step is to quickly assess the integrity and usability of each dataset before proceeding to further transformations.

---

### Structural Validation

Using the `show_shape` utility, we inspect:
- the number of rows (data volume),
- the number of columns (schema completeness).

This provides immediate visibility into:
- potential ingestion issues (empty datasets, unexpected sizes),
- schema inconsistencies,
- missing or incomplete data.

---

### Datasets Reviewed

The following datasets are validated:

- Customers  
- Orders  
- Payments  
- Support Tickets  
- Web Activities  

This ensures that all core business domains are correctly ingested and aligned in terms of structure.

---

### Why This Matters

Early validation is a critical best practice in data engineering pipelines.

By validating dataset shapes at the Bronze stage, we:
- detect issues as early as possible,
- reduce debugging complexity in downstream layers,
- ensure reliability before applying transformations,
- build confidence in the data pipeline.

This step acts as a lightweight but effective **data quality checkpoint**, supporting robust and production-ready analytics workflows.



```python

show_shape(customers_src, "customers_src")
show_shape(orders_src, "orders_src")
show_shape(payments_src, "payments_src")
show_shape(support_src, "support_src")
show_shape(web_src, "web_src")

```


## Bronze Layer — Data Preview & Exploratory Profiling

Following structural validation, we perform an initial visual inspection of the datasets.

The purpose of this step is to:
- understand the nature of the data,
- validate content consistency,
- identify potential anomalies early,
- gain business context before transformation.

---

### Data Exploration Approach

A sample of each dataset is displayed using `.limit(10)` to provide a quick and representative view of the data.

This allows us to:
- inspect column values and formats,
- detect nulls, inconsistencies, or unexpected patterns,
- verify that normalization (column names) has been correctly applied,
- ensure alignment with expected business semantics.

---

### Datasets Reviewed

We preview the following core entities:

- **Customers** → demographic and identity attributes  
- **Orders** → transactional and lifecycle data  
- **Payments** → financial transactions and statuses  
- **Support Tickets** → customer service interactions  
- **Web Activities** → digital engagement and behavior  

This multi-domain inspection provides a **holistic understanding of the data landscape**.

---

### Key Observations to Look For

During this step, attention should be given to:

- data type inconsistencies (dates, timestamps, numeric fields),
- missing or malformed values,
- unexpected categorical values,
- formatting issues (e.g., casing, spacing),
- business logic anomalies (e.g., negative amounts, invalid statuses).

---

## Why This Matters

Data preview is a critical step in any professional data pipeline.

It enables:
- early detection of data quality issues,
- better-informed transformation logic in the Silver layer,
- improved collaboration with business stakeholders,
- stronger alignment between technical implementation and business reality.

In a production-grade environment, this step ensures that transformations are not applied blindly, but are grounded in a clear understanding of the underlying data.



```python

display(customers_src.limit(10))
display(orders_src.limit(10))
display(payments_src.limit(10))
display(support_src.limit(10))
display(web_src.limit(10))

```


## Bronze Layer — Schema Validation & Data Structure Control

In this step, we perform a detailed inspection of the schema for each ingested dataset.

The objective is to ensure that the structural definition of the data aligns with expectations before proceeding to transformation and enrichment steps.

---

### Schema Inspection Approach

Using the `.printSchema()` method, we analyze:

- column names (post-normalization),
- data types (string, integer, double, timestamp, etc.),
- nullable properties,
- nested or complex structures (if any).

This provides a clear view of how Spark interprets the raw data after ingestion.

---

### Why Schema Validation is Critical

Even with schema inference enabled, raw data sources can introduce inconsistencies such as:

- incorrect data types (e.g., numeric fields interpreted as strings),
- inconsistent date or timestamp formats,
- unexpected nullability,
- structural drift between files.

Validating the schema at this stage allows us to:

- identify type mismatches early,
- prepare safe casting strategies for the Silver layer,
- avoid runtime transformation errors,
- ensure compatibility with analytical and BI use cases.

---

### Datasets Reviewed

The schema is inspected for all core domains:

- Customers  
- Orders  
- Payments  
- Support Tickets  
- Web Activities  

This ensures consistency across the entire data model.

---

## Why This Matters

Schema validation is a foundational step in any production-grade data pipeline.

It ensures that:
- transformations are applied on correctly typed data,
- joins and aggregations behave as expected,
- downstream KPIs are reliable,
- the semantic model remains stable over time.

In a modern data platform such as Microsoft Fabric, this step contributes directly to **data reliability, governance, and analytical trust**.



```python

customers_src.printSchema()
orders_src.printSchema()
payments_src.printSchema()
support_src.printSchema()
web_src.printSchema()

```


# Bronze Layer — Raw Ingestion with Traceability

## Objective
This section builds the **Bronze layer**, the raw landing zone of the Medallion architecture.  
The goal is to ingest each source dataset **as-is**, while enriching it with **technical ingestion metadata** required for lineage, auditability, replayability, and downstream operational monitoring.

## Design principles
- Preserve source fidelity
- Add ingestion and lineage metadata
- Keep transformations minimal
- Prepare Delta persistence for reproducible downstream processing

## Expected output
Five raw Delta tables ready for controlled consumption in Silver:
- `bronze_customers_raw`
- `bronze_orders_raw`
- `bronze_payments_raw`
- `bronze_support_tickets_raw`
- `bronze_web_activities_raw`



```python

customers_bronze = add_ingestion_metadata(customers_src, "customers", SOURCE_PATHS["customers"])
orders_bronze = add_ingestion_metadata(orders_src, "orders", SOURCE_PATHS["orders"])
payments_bronze = add_ingestion_metadata(payments_src, "payments", SOURCE_PATHS["payments"])
support_bronze = add_ingestion_metadata(support_src, "support_tickets", SOURCE_PATHS["support_tickets"])
web_bronze = add_ingestion_metadata(web_src, "web_activities", SOURCE_PATHS["web_activities"])

```


### Bronze Write — Persist Raw Tables to Delta

This cell writes the raw Bronze DataFrames into Delta format in the Lakehouse **Tables** area.  
At this stage, the objective is to create a durable raw layer that is:
- queryable
- auditable
- versionable
- reusable for reprocessing


```python

write_delta_to_tables(customers_bronze, "bronze_customers_raw")
write_delta_to_tables(orders_bronze, "bronze_orders_raw")
write_delta_to_tables(payments_bronze, "bronze_payments_raw")
write_delta_to_tables(support_bronze, "bronze_support_tickets_raw")
write_delta_to_tables(web_bronze, "bronze_web_activities_raw")

```


### Bronze Read — Reload Persisted Raw Data

This cell reloads the Bronze Delta tables from storage.  
This validation step confirms that the write operation succeeded and that the Bronze layer is now accessible as a trusted raw source for Silver transformations.



```python

bronze_customers = read_delta_from_tables("bronze_customers_raw")
bronze_orders = read_delta_from_tables("bronze_orders_raw")
bronze_payments = read_delta_from_tables("bronze_payments_raw")
bronze_support = read_delta_from_tables("bronze_support_tickets_raw")
bronze_web = read_delta_from_tables("bronze_web_activities_raw")

```


### Bronze Data Audit — Structural Validation

This control cell validates the structural footprint of the Bronze layer by checking:
- row counts
- column counts
- dataset presence

It provides a first operational checkpoint before moving to data exploration.



```python

show_shape(bronze_customers, "bronze_customers")
show_shape(bronze_orders, "bronze_orders")
show_shape(bronze_payments, "bronze_payments")
show_shape(bronze_support, "bronze_support")
show_shape(bronze_web, "bronze_web")

```


### Bronze Preview — Sample Raw Records

This cell displays sample rows from each Bronze table in order to:
- validate ingestion visually
- confirm source fidelity
- inspect raw attribute patterns
- identify obvious anomalies before cleansing



```python

display(bronze_customers.limit(10))
display(bronze_orders.limit(10))
display(bronze_payments.limit(10))
display(bronze_support.limit(10))
display(bronze_web.limit(10))

```


### Bronze Exploratory Profiling — High-Level Distribution Checks

This cell performs lightweight profiling on selected operational attributes such as:
- order status
- payment status
- ticket category
- event type / device indicators

This quick exploratory view helps identify dominant patterns and potential data quality issues before Silver standardization.



```python

display(
    bronze_orders.groupBy("order_status").count().orderBy(F.desc("count"))
)

display(
    bronze_payments.groupBy("payment_status").count().orderBy(F.desc("count"))
)

display(
    bronze_support.groupBy("ticket_category").count().orderBy(F.desc("count"))
)

display(
    bronze_web.groupBy("event_type").count().orderBy(F.desc("count"))
)

```


```python

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

def read_delta_from_tables(table_name: str):
    path = f"Tables/{table_name}"
    return spark.read.format("delta").load(path)

def write_delta_to_tables(df, table_name: str, mode: str = "overwrite"):
    path = f"Tables/{table_name}"
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )
    print(f"Written successfully to {path}")

def safe_to_timestamp(df, col_name):
    if col_name in df.columns:
        return df.withColumn(
            col_name,
            F.coalesce(
                F.to_timestamp(F.col(col_name)),
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd")
            )
        )
    return df

def safe_to_date(df, col_name):
    if col_name in df.columns:
        return df.withColumn(
            col_name,
            F.coalesce(
                F.to_date(F.col(col_name)),
                F.to_date(F.col(col_name), "yyyy-MM-dd")
            )
        )
    return df

```


# Silver Layer — Standardization, Cleansing and Business Conformance

## Objective
This section transforms raw Bronze datasets into **trusted Silver tables** by applying:
- typing
- cleansing
- deduplication
- normalization
- business rule enrichment

## Design principles
- Enforce data quality at entity level
- Standardize naming and business values
- Preserve analytical usefulness without over-aggregating
- Prepare conformed datasets for downstream 360 views and Gold marts



```python

bronze_customers = read_delta_from_tables("bronze_customers_raw")
bronze_orders = read_delta_from_tables("bronze_orders_raw")
bronze_payments = read_delta_from_tables("bronze_payments_raw")
bronze_support = read_delta_from_tables("bronze_support_tickets_raw")
bronze_web = read_delta_from_tables("bronze_web_activities_raw")

```


### Silver Preparation — Customers Schema Inspection

Before building the Silver Customers table, this diagnostic cell inspects the available schema and previews sample rows.  
This is a good engineering practice when source structures may vary slightly across iterations.



```python

print(bronze_customers.columns)
display(bronze_customers.limit(5))

```


```python

silver_customers = (
    bronze_customers
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
    .transform(lambda df: safe_to_date(df, "birth_date"))
    .transform(lambda df: safe_to_timestamp(df, "created_at"))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .withColumn("gender", F.lower(F.trim(F.col("gender"))))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("customer_segment", F.initcap(F.trim(F.col("customer_segment"))))
    .withColumn("acquisition_channel", F.initcap(F.trim(F.col("acquisition_channel"))))
    .withColumn("age", F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12))
    .withColumn(
        "age_band",
        F.when(F.col("age") < 25, "18-24")
         .when(F.col("age") < 35, "25-34")
         .when(F.col("age") < 45, "35-44")
         .when(F.col("age") < 55, "45-54")
         .otherwise("55+")
    )
    .withColumn("dq_email_valid", F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
)

```


```python

def col_or_null(df, col_name, cast_type="string"):
    if col_name in df.columns:
        return F.col(col_name)
    return F.lit(None).cast(cast_type)

```


### Silver Orders — Transaction Standardization

This cell transforms raw order data into a conformed Silver transaction table by applying:
- timestamp harmonization
- entity-level deduplication
- business text normalization
- financial derivations (`gross_amount`, `net_amount`, `discount_rate`)
- logistics KPIs (`shipping_delay_days`, `delivery_delay_days`)
- behavioral flags such as returns and discounts

This table becomes the operational backbone of the analytical model.



```python

orders_tmp = bronze_orders

for c in ["order_datetime", "promised_delivery_date", "actual_delivery_date"]:
    orders_tmp = safe_to_timestamp(orders_tmp, c)

silver_orders = (
    orders_tmp
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .withColumn("order_channel", F.initcap(F.trim(F.col("order_channel"))))
    .withColumn("device_type", F.lower(F.trim(F.col("device_type"))))
    .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
    .withColumn("top_product_category", F.initcap(F.trim(F.col("top_product_category"))))
    .withColumn("shipping_country_code", F.upper(F.trim(F.col("shipping_country_code"))))
    .withColumn("shipping_country", F.upper(F.trim(F.col("shipping_country"))))
    .withColumn("shipping_city", F.initcap(F.trim(F.col("shipping_city"))))
    .withColumn("fulfillment_type", F.initcap(F.trim(F.col("fulfillment_type"))))
    .withColumn("warehouse_id", F.upper(F.trim(F.col("warehouse_id"))))
    .withColumn("coupon_code", F.upper(F.trim(F.col("coupon_code"))))
    .withColumn("order_ts", F.col("order_datetime"))
    .withColumn("order_dt", F.to_date(F.col("order_datetime")))
    .withColumn(
        "promised_delivery_days",
        F.datediff(F.to_date(F.col("promised_delivery_date")), F.to_date(F.col("order_datetime")))
    )
    .withColumn(
        "actual_delivery_days",
        F.datediff(F.to_date(F.col("actual_delivery_date")), F.to_date(F.col("order_datetime")))
    )
    .withColumn(
        "delivery_delay_days",
        F.datediff(F.to_date(F.col("actual_delivery_date")), F.to_date(F.col("promised_delivery_date")))
    )
    .withColumn("has_discount", F.coalesce(F.col("discount_amount"), F.lit(0.0)) > 0)
    .withColumn(
        "discount_rate",
        F.when(F.col("gross_amount") > 0, F.col("discount_amount") / F.col("gross_amount"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "dq_amount_valid",
        F.coalesce(F.col("net_amount"), F.lit(0.0)) >= 0
    )
)

```


### Silver Preparation — Payments Schema Inspection

This cell inspects the Bronze Payments schema before transformation in order to validate available fields and adapt the standardization logic safely.



```python

print(bronze_payments.columns)

```


### Silver Payments — Financial Transaction Cleansing

This cell standardizes payment transactions by applying:
- timestamp parsing
- payment method / provider normalization
- status standardization
- currency normalization
- date derivation for time analysis

The result is a clean payments layer ready for reconciliation, payment analytics, and order-level enrichment.



```python

payments_tmp = bronze_payments

for c in ["payment_date", "created_at", "processed_at"]:
    if c in payments_tmp.columns:
        payments_tmp = safe_to_timestamp(payments_tmp, c)

silver_payments = (
    payments_tmp
    .filter(F.col("payment_id").isNotNull())
    .dropDuplicates(["payment_id"])
    .withColumn("payment_method", F.initcap(F.trim(col_or_null(payments_tmp, "payment_method"))))
    .withColumn("payment_provider", F.initcap(F.trim(col_or_null(payments_tmp, "payment_provider"))))
    .withColumn("payment_status", F.lower(F.trim(col_or_null(payments_tmp, "payment_status"))))
    .withColumn(
        "currency_code",
        F.upper(
            F.trim(
                F.coalesce(
                    col_or_null(payments_tmp, "currency_code"),
                    col_or_null(payments_tmp, "currency")
                )
            )
        )
    )
    .withColumn(
        "transaction_amount",
        F.coalesce(
            col_or_null(payments_tmp, "transaction_amount", "double"),
            col_or_null(payments_tmp, "amount", "double"),
            F.lit(0.0)
        )
    )
    .withColumn(
        "payment_ts",
        F.coalesce(
            col_or_null(payments_tmp, "payment_date", "timestamp"),
            col_or_null(payments_tmp, "processed_at", "timestamp"),
            col_or_null(payments_tmp, "created_at", "timestamp")
        )
    )
    .withColumn("payment_dt", F.to_date(F.col("payment_ts")))
    .withColumn(
        "installment_number",
        F.coalesce(col_or_null(payments_tmp, "installment_number", "int"), F.lit(1))
    )
    .withColumn(
        "is_refund",
        F.when(F.lower(col_or_null(payments_tmp, "is_refund")).isin("true", "1", "yes", "y"), True).otherwise(False)
    )
    .withColumn(
        "is_chargeback",
        F.when(F.lower(col_or_null(payments_tmp, "is_chargeback")).isin("true", "1", "yes", "y"), True).otherwise(False)
    )
    .withColumn("failure_reason", col_or_null(payments_tmp, "failure_reason"))
)

```


### Silver Preparation — Support Schema Inspection

This diagnostic step validates the support ticket schema before transformation.  
It is useful for confirming field availability and reducing schema-related runtime issues.



```python

print(bronze_support.columns)

```


### Silver Support Tickets — Service Quality Standardization

This cell builds the standardized support ticket layer by applying:
- timestamp harmonization
- category / priority / status normalization
- ticket date derivation
- SLA breach logic based on response and resolution time thresholds

The output supports customer service analytics and operational performance monitoring.



```python

support_tmp = bronze_support

for c in ["created_at", "closed_at", "resolved_at"]:
    if c in support_tmp.columns:
        support_tmp = safe_to_timestamp(support_tmp, c)

silver_support_tickets = (
    support_tmp
    .filter(F.col("ticket_id").isNotNull())
    .dropDuplicates(["ticket_id"])
    .withColumn("ticket_channel", F.initcap(F.trim(col_or_null(support_tmp, "ticket_channel"))))
    .withColumn("ticket_category", F.initcap(F.trim(col_or_null(support_tmp, "ticket_category"))))
    .withColumn("ticket_priority", F.initcap(F.trim(col_or_null(support_tmp, "ticket_priority"))))
    .withColumn("ticket_status", F.lower(F.trim(col_or_null(support_tmp, "ticket_status"))))
    .withColumn("resolution_status", F.lower(F.trim(col_or_null(support_tmp, "resolution_status"))))
    .withColumn("assigned_team", F.initcap(F.trim(col_or_null(support_tmp, "assigned_team"))))
    .withColumn(
        "ticket_ts",
        F.coalesce(
            col_or_null(support_tmp, "created_at", "timestamp"),
            col_or_null(support_tmp, "opened_at", "timestamp")
        )
    )
    .withColumn("ticket_dt", F.to_date(F.col("ticket_ts")))
    .withColumn(
        "first_response_minutes",
        F.coalesce(col_or_null(support_tmp, "first_response_minutes", "int"), F.lit(0))
    )
    .withColumn(
        "resolution_minutes",
        F.coalesce(col_or_null(support_tmp, "resolution_minutes", "int"), F.lit(0))
    )
    .withColumn(
        "csat_score",
        col_or_null(support_tmp, "csat_score", "double")
    )
    .withColumn(
        "is_escalated",
        F.when(F.lower(col_or_null(support_tmp, "is_escalated")).isin("true", "1", "yes", "y"), True).otherwise(False)
    )
    .withColumn(
        "sla_breached",
        F.when(F.col("first_response_minutes") > 60, True)
         .when(F.col("resolution_minutes") > 24 * 60, True)
         .otherwise(False)
    )
)

```


### Silver Preparation — Web Activity Schema Inspection

This cell validates the web activity schema before standardization and confirms the availability of digital interaction fields.



```python

print(bronze_web.columns)

```


### Silver Web Activities — Digital Event Standardization

This cell transforms raw web activity into a conformed interaction table by applying:
- timestamp parsing
- event normalization
- device / browser / traffic source cleansing
- geo standardization
- event date derivation
- session sequencing through `session_event_rank`

This table supports digital funnel and engagement analytics.



```python

web_tmp = bronze_web

for c in ["event_timestamp", "created_at"]:
    if c in web_tmp.columns:
        web_tmp = safe_to_timestamp(web_tmp, c)

silver_web_activities = (
    web_tmp
    .filter(F.col("activity_id").isNotNull())
    .dropDuplicates(["activity_id"])
    .withColumn("event_type", F.lower(F.trim(col_or_null(web_tmp, "event_type"))))
    .withColumn("page_type", F.lower(F.trim(col_or_null(web_tmp, "page_type"))))
    .withColumn(
        "device_type",
        F.lower(
            F.trim(
                F.coalesce(
                    col_or_null(web_tmp, "device_type"),
                    col_or_null(web_tmp, "device")
                )
            )
        )
    )
    .withColumn("browser", F.initcap(F.trim(col_or_null(web_tmp, "browser"))))
    .withColumn("traffic_source", F.initcap(F.trim(col_or_null(web_tmp, "traffic_source"))))
    .withColumn("campaign_name", F.initcap(F.trim(col_or_null(web_tmp, "campaign_name"))))
    .withColumn(
        "country",
        F.upper(
            F.trim(
                F.coalesce(
                    col_or_null(web_tmp, "country"),
                    col_or_null(web_tmp, "country_code")
                )
            )
        )
    )
    .withColumn("city", F.initcap(F.trim(col_or_null(web_tmp, "city"))))
    .withColumn(
        "event_ts",
        F.coalesce(
            col_or_null(web_tmp, "event_timestamp", "timestamp"),
            col_or_null(web_tmp, "created_at", "timestamp")
        )
    )
    .withColumn("event_dt", F.to_date(F.col("event_ts")))
    .withColumn(
        "session_event_rank",
        F.row_number().over(Window.partitionBy("session_id").orderBy(F.col("event_ts")))
    )
)

```


### Silver Persistence — Write Customers

This cell writes the standardized customer dataset to Delta as `silver_customers`.



```python

write_delta_to_tables(silver_customers, "silver_customers")
print("silver_customers written successfully")

```


### Silver Validation — Read and Preview Customers

This cell reloads the persisted `silver_customers` table and displays sample rows for validation.



```python

silver_customers = read_delta_from_tables("silver_customers")
display(silver_customers.limit(10))

```


### Silver Persistence — Write Orders

This cell writes the standardized orders dataset to Delta as `silver_orders`.



```python

write_delta_to_tables(silver_orders, "silver_orders")
print("silver_orders written successfully")

```


### Silver Validation — Read and Preview Orders

This cell reloads the persisted `silver_orders` table and displays sample rows for validation.



```python

silver_orders = read_delta_from_tables("silver_orders")
display(silver_orders.limit(10))

```


### Silver Persistence — Write Payments

This cell writes the standardized payments dataset to Delta as `silver_payments`.



```python

write_delta_to_tables(silver_payments, "silver_payments")
print("silver_payments written successfully")

```


### Silver Persistence — Write Support Tickets

This cell writes the standardized support dataset to Delta as `silver_support_tickets`.



```python

write_delta_to_tables(silver_support_tickets, "silver_support_tickets")
print("silver_support_tickets written successfully")

```


### Silver Validation — Read and Preview Support Tickets

This cell reloads the persisted `silver_support_tickets` table and displays sample rows for validation.



```python

silver_support_tickets = read_delta_from_tables("silver_support_tickets")
display(silver_support_tickets.limit(10))

```


### Silver Persistence — Write Web Activities

This cell writes the standardized digital activity dataset to Delta as `silver_web_activities`.



```python

write_delta_to_tables(silver_web_activities, "silver_web_activities")
print("silver_web_activities written successfully")

```


### Silver Validation — Read and Preview Web Activities

This cell reloads the persisted `silver_web_activities` table and displays sample rows for validation.



```python

silver_web_activities = read_delta_from_tables("silver_web_activities")
display(silver_web_activities.limit(10))

```


### Silver Profiling — Orders Status Distribution

This analytical checkpoint reviews the distribution of order statuses to validate business consistency after standardization.



```python

display(
    silver_orders.groupBy("order_status").count().orderBy(F.desc("count"))
)

```


### Silver Profiling — Customer Segment Distribution

This cell reviews the distribution of customer segments, helping validate segmentation values and business balance.



```python

display(
    silver_customers.groupBy("customer_segment").count().orderBy(F.desc("count"))
)

```


### Silver Profiling — Payment Method Distribution

This cell explores payment method usage in the standardized payments layer.



```python

display(
    silver_payments.groupBy("payment_method").count().orderBy(F.desc("count"))
)

```


### Silver Profiling — Device Distribution in Web Activity

This cell profiles the device mix captured in digital activity, supporting digital behavior analysis.



```python

display(
    silver_web_activities.groupBy("device_type").count().orderBy(F.desc("count"))
)

```


# Silver Enrichment — Order 360 Preparation

## Objective
This subsection creates **aggregated enrichment datasets** at order level by combining:
- payment behavior
- support interactions
- digital signals

These datasets are used to assemble a consolidated **Order 360** analytical view.



```python

payments_by_order = (
    silver_payments.groupBy("order_id")
    .agg(
        F.sum("transaction_amount").alias("payments_total_amount"),
        F.sum(F.when(F.col("payment_status") == "paid", F.col("transaction_amount")).otherwise(F.lit(0.0))).alias("paid_amount"),
        F.sum(F.when(F.col("is_refund") == True, F.col("transaction_amount")).otherwise(F.lit(0.0))).alias("refund_amount"),
        F.sum(F.when(F.col("is_chargeback") == True, F.col("transaction_amount")).otherwise(F.lit(0.0))).alias("chargeback_amount"),
        F.countDistinct("payment_id").alias("payment_attempt_count"),
        F.max("payment_ts").alias("last_payment_ts")
    )
)

support_by_order = (
    silver_support_tickets.groupBy("order_id")
    .agg(
        F.countDistinct("ticket_id").alias("ticket_count"),
        F.avg("csat_score").alias("avg_csat_score"),
        F.sum(F.when(F.col("sla_breached") == True, 1).otherwise(0)).alias("sla_breach_count"),
        F.max(F.col("is_escalated").cast("int")).alias("has_escalation")
    )
)

web_by_order = (
    silver_web_activities.filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.count("activity_id").alias("web_event_count"),
        F.countDistinct("session_id").alias("session_count"),
        F.max("event_ts").alias("last_web_event_ts")
    )
)

```


### Silver Order 360 — Consolidated Order View

This cell builds `silver_order_360`, a unified analytical order entity combining:
- transactional order data
- customer attributes
- payment aggregates
- support indicators
- web engagement signals

This view is ideal for customer journey analysis, operational review, and commercial performance interpretation.



```python

silver_order_360 = (
    silver_orders.alias("o")
    .join(silver_customers.alias("c"), "customer_id", "left")
    .join(payments_by_order.alias("p"), "order_id", "left")
    .join(support_by_order.alias("s"), "order_id", "left")
    .join(web_by_order.alias("w"), "order_id", "left")
    .select(
        "order_id",
        "customer_id",
        "order_number",
        "order_ts",
        "order_dt",
        "order_status",
        "order_channel",
        "device_type",
        "currency_code",
        "top_product_category",
        "items_count",
        "gross_amount",
        "discount_amount",
        "shipping_amount",
        "tax_amount",
        "net_amount",
        "coupon_code",
        "is_first_order",
        "shipping_country_code",
        "shipping_country",
        "shipping_city",
        "fulfillment_type",
        "warehouse_id",
        "promised_delivery_date",
        "actual_delivery_date",
        "promised_delivery_days",
        "actual_delivery_days",
        "delivery_delay_days",
        "has_discount",
        "discount_rate",
        "full_name",
        "gender",
        "age",
        "age_band",
        F.col("c.country").alias("customer_country"),
        F.col("c.city").alias("customer_city"),
        "customer_segment",
        "acquisition_channel",
        F.coalesce(F.col("payments_total_amount"), F.lit(0.0)).alias("payments_total_amount"),
        F.coalesce(F.col("paid_amount"), F.lit(0.0)).alias("paid_amount"),
        F.coalesce(F.col("refund_amount"), F.lit(0.0)).alias("refund_amount"),
        F.coalesce(F.col("chargeback_amount"), F.lit(0.0)).alias("chargeback_amount"),
        F.coalesce(F.col("payment_attempt_count"), F.lit(0)).alias("payment_attempt_count"),
        F.coalesce(F.col("ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("sla_breach_count"), F.lit(0)).alias("sla_breach_count"),
        "avg_csat_score",
        F.coalesce(F.col("has_escalation"), F.lit(0)).alias("has_escalation"),
        F.coalesce(F.col("web_event_count"), F.lit(0)).alias("web_event_count"),
        F.coalesce(F.col("session_count"), F.lit(0)).alias("session_count"),
        "last_payment_ts",
        "last_web_event_ts"
    )
    .withColumn("is_fully_paid", F.col("paid_amount") >= F.col("net_amount"))
    .withColumn("margin_proxy", F.col("net_amount") - F.col("refund_amount") - F.col("chargeback_amount"))
)

```


### Silver Persistence — Write Order 360

This cell persists the enriched order-centric analytical view as `silver_order_360`.



```python

write_delta_to_tables(silver_order_360, "silver_order_360")
print("silver_order_360 written successfully")

```


### Silver Validation — Read and Preview Order 360

This cell reloads and previews the persisted `silver_order_360` table.



```python

silver_order_360 = read_delta_from_tables("silver_order_360")
display(silver_order_360.limit(10))

```


# Silver Enrichment — Customer 360 Preparation

## Objective
This subsection prepares customer-level enrichments by aggregating:
- order history
- support interactions
- web engagement

These inputs enable a consolidated **Customer 360** view.



```python

orders_by_customer = (
    silver_orders.groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("net_amount").alias("gross_revenue"),
        F.sum(F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)).alias("delayed_orders"),
        F.min("order_ts").alias("first_order_ts"),
        F.max("order_ts").alias("last_order_ts")
    )
)

support_by_customer = (
    silver_support_tickets.groupBy("customer_id")
    .agg(
        F.countDistinct("ticket_id").alias("total_tickets"),
        F.avg("csat_score").alias("customer_avg_csat")
    )
)

web_by_customer = (
    silver_web_activities.groupBy("customer_id")
    .agg(
        F.countDistinct("session_id").alias("total_sessions"),
        F.count("activity_id").alias("total_web_events"),
        F.max("event_ts").alias("last_activity_ts")
    )
)

```


### Silver Customer 360 — Unified Customer View

This cell assembles `silver_customer_360`, a customer-centric analytical table combining:
- core customer profile
- commercial behavior
- support activity
- digital engagement
- derived metrics such as AOV, return rate, recency, and value segment

This becomes the primary customer insight layer before Gold modeling.



```python

silver_customer_360 = (
    silver_customers.alias("c")
    .join(orders_by_customer.alias("o"), "customer_id", "left")
    .join(support_by_customer.alias("s"), "customer_id", "left")
    .join(web_by_customer.alias("w"), "customer_id", "left")
    .select(
        "c.*",
        F.coalesce(F.col("total_orders"), F.lit(0)).alias("total_orders"),
        F.coalesce(F.col("gross_revenue"), F.lit(0.0)).alias("gross_revenue"),
        F.coalesce(F.col("delayed_orders"), F.lit(0)).alias("delayed_orders"),
        "first_order_ts",
        "last_order_ts",
        F.coalesce(F.col("total_tickets"), F.lit(0)).alias("total_tickets"),
        "customer_avg_csat",
        F.coalesce(F.col("total_sessions"), F.lit(0)).alias("total_sessions"),
        F.coalesce(F.col("total_web_events"), F.lit(0)).alias("total_web_events"),
        "last_activity_ts"
    )
    .withColumn("aov", F.when(F.col("total_orders") > 0, F.col("gross_revenue") / F.col("total_orders")).otherwise(F.lit(0.0)))
    .withColumn("delay_rate", F.when(F.col("total_orders") > 0, F.col("delayed_orders") / F.col("total_orders")).otherwise(F.lit(0.0)))
    .withColumn("recency_days", F.datediff(F.current_date(), F.to_date(F.col("last_order_ts"))))
    .withColumn(
        "customer_value_segment",
        F.when((F.col("gross_revenue") >= 1000) & (F.col("total_orders") >= 5), "High Value")
         .when((F.col("gross_revenue") >= 300) & (F.col("total_orders") >= 2), "Mid Value")
         .otherwise("Low Value")
    )
)

```


### Silver Persistence — Write Customer 360

This cell persists the enriched customer-centric analytical view as `silver_customer_360`.



```python

write_delta_to_tables(silver_customer_360, "silver_customer_360")
print("silver_customer_360 written successfully")

```


### Silver Validation — Read and Preview Customer 360

This cell reloads and previews the persisted `silver_customer_360` table.



```python

silver_customer_360 = read_delta_from_tables("silver_customer_360")
display(silver_customer_360.limit(10))

```


### Silver Data Quality Controls

This cell computes a first set of **data quality checks** on critical Silver tables, covering:
- null key checks
- invalid email detection
- missing timestamps
- negative amount checks

This is an important engineering control before promoting data into Gold.



```python

silver_dq_results = spark.createDataFrame(
    [
        ("silver_customers", "null_customer_id", silver_customers.filter(F.col("customer_id").isNull()).count()),
        ("silver_customers", "invalid_email", silver_customers.filter(~F.col("dq_email_valid")).count()),
        ("silver_orders", "null_order_id", silver_orders.filter(F.col("order_id").isNull()).count()),
        ("silver_orders", "negative_net_amount", silver_orders.filter(F.col("net_amount") < 0).count()),
        ("silver_payments", "null_payment_id", silver_payments.filter(F.col("payment_id").isNull()).count()),
        ("silver_support_tickets", "null_ticket_id", silver_support_tickets.filter(F.col("ticket_id").isNull()).count()),
        ("silver_web_activities", "null_activity_id", silver_web_activities.filter(F.col("activity_id").isNull()).count())
    ],
    ["table_name", "rule_name", "failed_count"]
)

display(silver_dq_results)

```


### Silver Final Spot Check — Key Table Preview

This validation step provides a final visual spot check of the core Silver datasets before Gold modeling.



```python

display(read_delta_from_tables("silver_customers").limit(5))
display(read_delta_from_tables("silver_orders").limit(5))
display(read_delta_from_tables("silver_order_360").limit(5))
display(read_delta_from_tables("silver_customer_360").limit(5))

```


### Silver Final Volume Check

This cell validates the row counts of all key Silver datasets, including both conformed tables and 360 enrichments.



```python

print("silver_customers:", read_delta_from_tables("silver_customers").count())
print("silver_orders:", read_delta_from_tables("silver_orders").count())
print("silver_payments:", read_delta_from_tables("silver_payments").count())
print("silver_support_tickets:", read_delta_from_tables("silver_support_tickets").count())
print("silver_web_activities:", read_delta_from_tables("silver_web_activities").count())
print("silver_order_360:", read_delta_from_tables("silver_order_360").count())
print("silver_customer_360:", read_delta_from_tables("silver_customer_360").count())

```


```python

def write_delta_and_reload(df, table_name: str):
    write_delta_to_tables(df, table_name)
    print(f"{table_name} written successfully")

```


# Gold Layer — Dimensional Modeling and Consumption-Ready Data Products

## Objective
This section builds the **Gold layer**, the consumption layer of the platform.  
It transforms trusted Silver data into:
- dimensions
- facts
- marts
- KPI-oriented analytical assets

## Design principles
- Design for business consumption
- Support semantic modeling and Power BI
- Separate reusable dimensions from measurable facts
- Expose curated marts for executive analytics



```python

min_max = silver_orders.select(
    F.min("order_dt").alias("min_dt"),
    F.max("order_dt").alias("max_dt")
).collect()[0]

min_dt = min_max["min_dt"]
max_dt = min_max["max_dt"]

gold_dim_date = spark.sql(f"""
SELECT explode(sequence(to_date('{min_dt}'), to_date('{max_dt}'), interval 1 day)) AS date_day
""")

gold_dim_date = (
    gold_dim_date
    .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date_day"))
    .withColumn("quarter", F.quarter("date_day"))
    .withColumn("month", F.month("date_day"))
    .withColumn("month_name", F.date_format("date_day", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("date_day"))
    .withColumn("day_of_month", F.dayofmonth("date_day"))
    .withColumn("day_name", F.date_format("date_day", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date_day").isin([1, 7]))
)

```


### Gold Dimension — Persist Date Dimension

This cell writes the conformed calendar dimension to Delta as `gold_dim_date`.



```python

write_delta_and_reload(gold_dim_date, "gold_dim_date")

```


### Gold Validation — Read and Preview Date Dimension

This cell reloads and previews the calendar dimension, which will support all time-based analytics.



```python

gold_dim_date = read_delta_from_tables("gold_dim_date")
display(gold_dim_date.limit(10))

```


### Gold Dimension — Customer Dimension

This cell projects the curated customer 360 view into a reusable Gold customer dimension optimized for semantic modeling and reporting.



```python

gold_dim_customer = silver_customer_360.select(
    "customer_id",
    F.col("external_customer_key").alias("external_customer_id"),
    "full_name",
    "first_name",
    "last_name",
    "gender",
    "birth_date",
    "age",
    "age_band",
    "email",
    "phone",
    "country",
    "city",
    "postal_code",
    "customer_segment",
    "customer_value_segment",
    "acquisition_channel",
    F.col("is_active_customer").alias("is_active"),
    F.col("signup_datetime").alias("created_at"),
    "total_orders",
    "gross_revenue",
    "aov",
    "delay_rate",
    "total_tickets",
    "customer_avg_csat",
    "total_sessions",
    "total_web_events",
    "recency_days"
)

```


### Gold Validation — Preview Customer Dimension

This cell previews the customer dimension before persistence.



```python

display(gold_dim_customer.limit(10))

```


### Gold Dimension — Persist Customer Dimension

This cell writes `gold_dim_customer` to Delta.



```python

write_delta_and_reload(gold_dim_customer, "gold_dim_customer")

```


### Gold Validation — Read and Preview Customer Dimension

This cell reloads and previews the persisted customer dimension.



```python

gold_dim_customer = read_delta_from_tables("gold_dim_customer")
display(gold_dim_customer.limit(10))

```


### Gold Dimension — Geography Dimension

This cell creates a conformed geography dimension by unioning geographic signals from customers, orders, and web activity, then deduplicating the result.



```python

gold_dim_geography = (
    silver_customers.select(
        F.col("country").alias("country"),
        F.col("city").alias("city"),
        F.col("postal_code").alias("postal_code")
    )
    .unionByName(
        silver_orders.select(
            F.col("shipping_country").alias("country"),
            F.col("shipping_city").alias("city"),
            F.lit(None).cast("string").alias("postal_code")
        )
    )
    .unionByName(
        silver_web_activities.select(
            F.col("country").alias("country"),
            F.col("city").alias("city"),
            F.lit(None).cast("string").alias("postal_code")
        )
    )
    .dropDuplicates()
    .withColumn(
        "geography_key",
        F.row_number().over(Window.orderBy("country", "city", "postal_code"))
    )
    .select("geography_key", "country", "city", "postal_code")
)

```


### Gold Dimension — Persist Geography Dimension

This cell writes `gold_dim_geography` to Delta.



```python

write_delta_and_reload(gold_dim_geography, "gold_dim_geography")

```


### Gold Validation — Read and Preview Geography Dimension

This cell reloads and previews the geography dimension.



```python

gold_dim_geography = read_delta_from_tables("gold_dim_geography")
display(gold_dim_geography.limit(10))

```


### Gold Dimension — Payment Method Dimension

This cell builds a compact payment method reference dimension combining method, provider, and currency.



```python

gold_dim_payment_method = (
    silver_payments
    .select("payment_method", "payment_provider", "currency_code")
    .dropDuplicates()
    .withColumn(
        "payment_method_key",
        F.row_number().over(Window.orderBy("payment_method", "payment_provider", "currency_code"))
    )
)

```


### Gold Dimension — Persist Payment Method Dimension

This cell writes `gold_dim_payment_method` to Delta.



```python

write_delta_and_reload(gold_dim_payment_method, "gold_dim_payment_method")

```


### Gold Validation — Read and Preview Payment Method Dimension

This cell reloads and previews the payment method dimension.



```python

gold_dim_payment_method = read_delta_from_tables("gold_dim_payment_method")
display(gold_dim_payment_method.limit(10))

```


### Gold Fact Preparation — Shipping Geography Bridge

This helper cell prepares a shipping geography mapping used to enrich the Orders fact with a reusable geography key.



```python

geo_shipping = gold_dim_geography.select(
    F.col("geography_key").alias("shipping_geography_key"),
    F.col("country").alias("shipping_country"),
    F.col("city").alias("shipping_city")
)

```


### Gold Fact — Orders

This cell builds `gold_fact_orders`, the central commercial fact table of the analytical model.  
It integrates:
- order metrics
- financial metrics
- customer grain linkage
- geography linkage
- operational and customer service indicators



```python

gold_fact_orders = (
    silver_order_360.alias("o")
    .join(gold_dim_date.alias("d"), F.col("o.order_dt") == F.col("d.date_day"), "left")
    .join(geo_shipping.alias("g"), ["shipping_country", "shipping_city"], "left")
    .select(
        "order_id",
        "order_number",
        F.col("d.date_key").alias("order_date_key"),
        "customer_id",
        "shipping_geography_key",
        "order_status",
        "order_channel",
        "device_type",
        "currency_code",
        "top_product_category",
        "items_count",
        "gross_amount",
        "discount_amount",
        "shipping_amount",
        "tax_amount",
        "net_amount",
        "discount_rate",
        "payment_attempt_count",
        "payments_total_amount",
        "paid_amount",
        "refund_amount",
        "chargeback_amount",
        "ticket_count",
        "sla_breach_count",
        "avg_csat_score",
        "web_event_count",
        "session_count",
        "is_first_order",
        "is_fully_paid",
        "margin_proxy",
        "delivery_delay_days"
    )
)

```


### Gold Fact — Persist Orders Fact

This cell writes `gold_fact_orders` to Delta.



```python

write_delta_and_reload(gold_fact_orders, "gold_fact_orders")

```


### Gold Validation — Read and Preview Orders Fact

This cell reloads and previews the persisted Orders fact table.



```python

gold_fact_orders = read_delta_from_tables("gold_fact_orders")
display(gold_fact_orders.limit(10))

```


### Gold Fact — Payments

This cell builds `gold_fact_payments`, the payment-centric fact table used for payment analytics, reconciliation, and revenue integrity analysis.



```python

gold_fact_payments = (
    silver_payments.alias("p")
    .join(gold_dim_date.alias("d"), F.col("p.payment_dt") == F.col("d.date_day"), "left")
    .join(
        gold_dim_payment_method.alias("pm"),
        [
            silver_payments["payment_method"] == gold_dim_payment_method["payment_method"],
            silver_payments["payment_provider"] == gold_dim_payment_method["payment_provider"],
            silver_payments["currency_code"] == gold_dim_payment_method["currency_code"]
        ],
        "left"
    )
    .select(
        F.col("p.payment_id").alias("payment_id"),
        F.col("d.date_key").alias("payment_date_key"),
        F.col("p.order_id").alias("order_id"),
        F.col("p.customer_id").alias("customer_id"),
        F.col("pm.payment_method_key").alias("payment_method_key"),
        F.col("p.payment_status").alias("payment_status"),
        F.col("p.transaction_amount").alias("transaction_amount"),
        F.col("p.currency_code").alias("currency_code"),
        F.col("p.installment_number").alias("installment_number"),
        F.col("p.is_refund").alias("is_refund"),
        F.col("p.is_chargeback").alias("is_chargeback"),
        F.col("p.failure_reason").alias("failure_reason")
    )
)

```


### Gold Fact — Persist Payments Fact

This cell writes `gold_fact_payments` to Delta.



```python

write_delta_and_reload(gold_fact_payments, "gold_fact_payments")

```


### Gold Validation — Read and Preview Payments Fact

This cell reloads and previews the persisted Payments fact table.



```python

gold_fact_payments = read_delta_from_tables("gold_fact_payments")
display(gold_fact_payments.limit(10))

```


### Gold Fact — Support Tickets

This cell builds `gold_fact_support_tickets`, the service operations fact table used for support volume, SLA, escalation, and CSAT analysis.



```python

gold_fact_support_tickets = (
    silver_support_tickets.alias("s")
    .join(gold_dim_date.alias("d"), F.col("s.ticket_dt") == F.col("d.date_day"), "left")
    .select(
        "ticket_id",
        F.col("d.date_key").alias("ticket_date_key"),
        "customer_id",
        "order_id",
        "ticket_channel",
        "ticket_category",
        "ticket_priority",
        "ticket_status",
        "resolution_status",
        "assigned_team",
        "csat_score",
        "first_response_minutes",
        "resolution_minutes",
        "is_escalated",
        "sla_breached"
    )
)

```


### Gold Fact — Persist Support Tickets Fact

This cell writes `gold_fact_support_tickets` to Delta.



```python

write_delta_and_reload(gold_fact_support_tickets, "gold_fact_support_tickets")

```


### Gold Validation — Read and Preview Support Tickets Fact

This cell reloads and previews the persisted Support Tickets fact table.



```python

gold_fact_support_tickets = read_delta_from_tables("gold_fact_support_tickets")
display(gold_fact_support_tickets.limit(10))

```


### Gold Fact Preparation — Session Aggregation

This helper cell aggregates raw web events at session grain in order to derive reusable session-level behavioral metrics.



```python

session_agg = (
    silver_web_activities.groupBy("session_id", "customer_id")
    .agg(
        F.min("event_ts").alias("session_start_ts"),
        F.max("event_ts").alias("session_end_ts"),
        F.count("activity_id").alias("event_count"),
        F.max("device_type").alias("device_type"),
        F.max("browser").alias("browser"),
        F.max("traffic_source").alias("traffic_source"),
        F.max("campaign_name").alias("campaign_name"),
        F.max("country").alias("country"),
        F.max("city").alias("city"),
        F.max(F.when(F.col("event_type").isin("purchase", "order_confirmed", "checkout_complete"), 1).otherwise(0)).alias("is_conversion")
    )
    .withColumn("session_dt", F.to_date("session_start_ts"))
    .withColumn("session_duration_seconds", F.unix_timestamp("session_end_ts") - F.unix_timestamp("session_start_ts"))
)

```


### Gold Fact — Web Sessions

This cell builds `gold_fact_web_sessions`, the digital behavior fact table used for engagement and conversion analysis.



```python

gold_fact_web_sessions = (
    session_agg.alias("s")
    .join(gold_dim_date.alias("d"), F.col("s.session_dt") == F.col("d.date_day"), "left")
    .join(gold_dim_geography.alias("g"), ["country", "city"], "left")
    .select(
        "session_id",
        F.col("d.date_key").alias("session_date_key"),
        "customer_id",
        F.col("g.geography_key").alias("geography_key"),
        "device_type",
        "browser",
        "traffic_source",
        "campaign_name",
        "event_count",
        "session_duration_seconds",
        "is_conversion"
    )
)

```


### Gold Fact — Persist Web Sessions Fact

This cell writes `gold_fact_web_sessions` to Delta.



```python

write_delta_and_reload(gold_fact_web_sessions, "gold_fact_web_sessions")

```


### Gold Validation — Read and Preview Web Sessions Fact

This cell reloads and previews the persisted Web Sessions fact table.



```python

gold_fact_web_sessions = read_delta_from_tables("gold_fact_web_sessions")
display(gold_fact_web_sessions.limit(10))

```


### Gold Mart — Daily Sales

This cell builds an executive-ready daily sales mart with key revenue KPIs aggregated by calendar day.



```python

gold_mart_daily_sales = (
    gold_fact_orders.alias("f")
    .join(gold_dim_date.alias("d"), F.col("f.order_date_key") == F.col("d.date_key"), "left")
    .groupBy("f.order_date_key", "d.date_day", "d.year", "d.month", "d.month_name")
    .agg(
        F.countDistinct("f.order_id").alias("orders_count"),
        F.countDistinct("f.customer_id").alias("customers_count"),
        F.sum("f.net_amount").alias("gross_sales"),
        F.sum("f.discount_amount").alias("discounts"),
        F.sum("f.refund_amount").alias("refunds"),
        F.sum("f.chargeback_amount").alias("chargebacks"),
        F.sum("f.margin_proxy").alias("net_revenue_proxy"),
        F.avg("f.net_amount").alias("aov")
    )
)

```


### Gold Mart — Persist Daily Sales Mart

This cell writes `gold_mart_daily_sales` to Delta.



```python

write_delta_and_reload(gold_mart_daily_sales, "gold_mart_daily_sales")

```


### Gold Validation — Read and Preview Daily Sales Mart

This cell reloads and previews the daily sales mart.



```python

gold_mart_daily_sales = read_delta_from_tables("gold_mart_daily_sales")
display(gold_mart_daily_sales.limit(10))

```


### Gold Mart — Customer Segments

This cell builds a curated mart for customer segmentation analysis, combining customer segment, value segment, and geography.



```python

gold_mart_customer_segments = (
    gold_dim_customer.groupBy("customer_segment", "customer_value_segment", "country")
    .agg(
        F.countDistinct("customer_id").alias("customers_count"),
        F.sum("gross_revenue").alias("gross_revenue"),
        F.avg("aov").alias("avg_aov"),
        F.avg("delay_rate").alias("avg_delay_rate"),
        F.avg("customer_avg_csat").alias("avg_csat")
    )
)

```


### Gold Mart — Persist Customer Segments Mart

This cell writes `gold_mart_customer_segments` to Delta.



```python

write_delta_and_reload(gold_mart_customer_segments, "gold_mart_customer_segments")

```


### Gold Validation — Read and Preview Customer Segments Mart

This cell reloads and previews the customer segments mart.



```python

gold_mart_customer_segments = read_delta_from_tables("gold_mart_customer_segments")
display(gold_mart_customer_segments.limit(10))

```


### Gold Mart — Digital Funnel

This cell builds a digital funnel mart that tracks sessions, page views, add-to-cart, checkout, and conversion metrics by date, source, and device.



```python

gold_mart_funnel = (
    silver_web_activities.groupBy("event_dt", "traffic_source", "device_type")
    .agg(
        F.countDistinct("session_id").alias("sessions"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
        F.sum(F.when(F.col("event_type").isin("checkout_start", "checkout"), 1).otherwise(0)).alias("checkout_starts"),
        F.sum(F.when(F.col("event_type").isin("purchase", "order_confirmed", "checkout_complete"), 1).otherwise(0)).alias("conversions")
    )
    .withColumn("cart_rate", F.when(F.col("sessions") > 0, F.col("add_to_carts") / F.col("sessions")).otherwise(F.lit(0.0)))
    .withColumn("checkout_rate", F.when(F.col("add_to_carts") > 0, F.col("checkout_starts") / F.col("add_to_carts")).otherwise(F.lit(0.0)))
    .withColumn("conversion_rate", F.when(F.col("sessions") > 0, F.col("conversions") / F.col("sessions")).otherwise(F.lit(0.0)))
)

```


### Gold Mart — Persist Digital Funnel Mart

This cell writes `gold_mart_funnel` to Delta.



```python

write_delta_and_reload(gold_mart_funnel, "gold_mart_funnel")

```


### Gold Validation — Read and Preview Digital Funnel Mart

This cell reloads and previews the digital funnel mart.



```python

gold_mart_funnel = read_delta_from_tables("gold_mart_funnel")
display(gold_mart_funnel.limit(10))

```


### Gold Data Quality Controls

This cell executes core Gold quality checks to ensure that critical dimensions and facts are fit for downstream BI and semantic modeling.



```python

gold_dq_results = spark.createDataFrame(
    [
        ("gold_dim_customer", "null_customer_id", gold_dim_customer.filter(F.col("customer_id").isNull()).count()),
        ("gold_fact_orders", "null_order_id", gold_fact_orders.filter(F.col("order_id").isNull()).count()),
        ("gold_fact_payments", "null_payment_id", gold_fact_payments.filter(F.col("payment_id").isNull()).count()),
        ("gold_fact_support_tickets", "null_ticket_id", gold_fact_support_tickets.filter(F.col("ticket_id").isNull()).count()),
        ("gold_fact_web_sessions", "null_session_id", gold_fact_web_sessions.filter(F.col("session_id").isNull()).count())
    ],
    ["table_name", "rule_name", "failed_count"]
)

display(gold_dq_results)

```


### Gold Mart Visualization — Daily Sales

This visual checkpoint explores the daily sales mart to confirm business usability and analytical consistency.



```python

display(
    gold_mart_daily_sales.orderBy(F.desc("date_day"))
)

```


### Gold Mart Visualization — Customer Segments

This visual checkpoint explores the customer segment mart.



```python

display(
    gold_mart_customer_segments.orderBy(F.desc("gross_revenue"))
)

```


### Gold Mart Visualization — Digital Funnel

This visual checkpoint explores the digital funnel mart.



```python

display(
    gold_mart_funnel.orderBy(F.desc("event_dt"))
)

```


# Gold Final Validation and Consumption Readiness

## Objective
This final subsection validates that all Gold assets are persisted and ready for downstream usage in:
- semantic models
- Power BI
- SQL views
- KPI reporting



```python

gold_dim_date = read_delta_from_tables("gold_dim_date")
gold_dim_customer = read_delta_from_tables("gold_dim_customer")
gold_dim_geography = read_delta_from_tables("gold_dim_geography")
gold_dim_payment_method = read_delta_from_tables("gold_dim_payment_method")
gold_fact_orders = read_delta_from_tables("gold_fact_orders")
gold_fact_payments = read_delta_from_tables("gold_fact_payments")
gold_fact_support_tickets = read_delta_from_tables("gold_fact_support_tickets")
gold_fact_web_sessions = read_delta_from_tables("gold_fact_web_sessions")
gold_mart_daily_sales = read_delta_from_tables("gold_mart_daily_sales")
gold_mart_customer_segments = read_delta_from_tables("gold_mart_customer_segments")
gold_mart_funnel = read_delta_from_tables("gold_mart_funnel")

print("All Gold tables loaded successfully.")

```


### Gold Inventory — Table Volume Check

This cell reviews row counts across all Gold dimensions, facts, and marts.



```python

gold_counts = [
    ("gold_dim_date", gold_dim_date.count()),
    ("gold_dim_customer", gold_dim_customer.count()),
    ("gold_dim_geography", gold_dim_geography.count()),
    ("gold_dim_payment_method", gold_dim_payment_method.count()),
    ("gold_fact_orders", gold_fact_orders.count()),
    ("gold_fact_payments", gold_fact_payments.count()),
    ("gold_fact_support_tickets", gold_fact_support_tickets.count()),
    ("gold_fact_web_sessions", gold_fact_web_sessions.count()),
    ("gold_mart_daily_sales", gold_mart_daily_sales.count()),
    ("gold_mart_customer_segments", gold_mart_customer_segments.count()),
    ("gold_mart_funnel", gold_mart_funnel.count())
]

display(spark.createDataFrame(gold_counts, ["table_name", "row_count"]))

```


### Gold Semantic Readiness — Temporary Views

This cell exposes Gold datasets as temporary views to facilitate SQL-style exploration and semantic layer prototyping.



```python

gold_dim_date.createOrReplaceTempView("vw_gold_dim_date")
gold_dim_customer.createOrReplaceTempView("vw_gold_dim_customer")
gold_dim_geography.createOrReplaceTempView("vw_gold_dim_geography")
gold_dim_payment_method.createOrReplaceTempView("vw_gold_dim_payment_method")
gold_fact_orders.createOrReplaceTempView("vw_gold_fact_orders")
gold_fact_payments.createOrReplaceTempView("vw_gold_fact_payments")
gold_fact_support_tickets.createOrReplaceTempView("vw_gold_fact_support_tickets")
gold_fact_web_sessions.createOrReplaceTempView("vw_gold_fact_web_sessions")
gold_mart_daily_sales.createOrReplaceTempView("vw_gold_mart_daily_sales")
gold_mart_customer_segments.createOrReplaceTempView("vw_gold_mart_customer_segments")
gold_mart_funnel.createOrReplaceTempView("vw_gold_mart_funnel")

print("Temporary views created successfully.")

```


### Executive KPI Snapshot — Commercial View

This cell computes a compact executive KPI view from the Orders fact, focused on revenue, customer base, and monetization.



```python

exec_kpis = gold_fact_orders.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.countDistinct("customer_id").alias("total_customers"),
    F.sum("net_amount").alias("gross_sales"),
    F.sum("refund_amount").alias("total_refunds"),
    F.sum("chargeback_amount").alias("total_chargebacks"),
    F.avg("net_amount").alias("avg_order_value")
).withColumn(
    "net_revenue",
    F.col("gross_sales") - F.col("total_refunds") - F.col("total_chargebacks")
)

display(exec_kpis)

```


### Executive KPI Snapshot — Support View

This cell computes support service KPIs, including CSAT, first response time, resolution time, and SLA breach rate.



```python

support_kpis = gold_fact_support_tickets.agg(
    F.countDistinct("ticket_id").alias("total_tickets"),
    F.avg("csat_score").alias("avg_csat"),
    F.avg("first_response_minutes").alias("avg_first_response_minutes"),
    F.avg("resolution_minutes").alias("avg_resolution_minutes"),
    F.avg(F.col("sla_breached").cast("double")).alias("sla_breach_rate")
)

display(support_kpis)

```


### Executive KPI Snapshot — Funnel View

This cell computes funnel KPIs from session-level data, including sessions, conversions, average duration, and conversion rate.



```python

funnel_kpis = gold_fact_web_sessions.agg(
    F.countDistinct("session_id").alias("total_sessions"),
    F.sum("is_conversion").alias("total_conversions"),
    F.avg("session_duration_seconds").alias("avg_session_duration_seconds")
).withColumn(
    "conversion_rate",
    F.when(F.col("total_sessions") > 0, F.col("total_conversions") / F.col("total_sessions")).otherwise(F.lit(0.0))
)

display(funnel_kpis)

```


### Gold Documentation — Model Inventory

This cell creates a concise inventory of Gold analytical assets with table type and business description.  
It is useful for handover, governance, and semantic model documentation.



```python

model_inventory = spark.createDataFrame(
    [
        ("gold_dim_date", "Dimension", "Calendrier analytique"),
        ("gold_dim_customer", "Dimension", "Vue client analytique"),
        ("gold_dim_geography", "Dimension", "Référentiel géographique"),
        ("gold_dim_payment_method", "Dimension", "Référentiel des méthodes de paiement"),
        ("gold_fact_orders", "Fact", "Faits de commandes"),
        ("gold_fact_payments", "Fact", "Faits de paiements"),
        ("gold_fact_support_tickets", "Fact", "Faits de tickets support"),
        ("gold_fact_web_sessions", "Fact", "Faits de sessions web"),
        ("gold_mart_daily_sales", "Mart", "KPI journaliers de ventes"),
        ("gold_mart_customer_segments", "Mart", "Segmentation client analytique"),
        ("gold_mart_funnel", "Mart", "Tunnel de conversion web")
    ],
    ["table_name", "table_type", "business_description"]
)

display(model_inventory)

```

