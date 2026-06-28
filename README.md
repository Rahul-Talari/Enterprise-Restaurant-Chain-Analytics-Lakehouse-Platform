# Enterprise Restaurant Chain Analytics Lakehouse Platform

---

# Business Rules

Restaurant Chain
```
    └── Operates multiple Restaurant Branches.
```
Restaurant Branch
```
    ├── Offers multiple Menu Items          : Each Menu Item belongs to one Menu Category.
    ├── Receives multiple Customer Orders.  : After an order is placed, the restaurant branch prepares and fulfills it.
    └── Receives multiple Customer Reviews.
```
Customer
```
    ├── Registers with the restaurant chain.
    ├── Places multiple Orders from any restaurant branch.
    └── Writes multiple Customer Reviews.    
```
Customer Order
```
    ├── Contains one or more Menu Items.                  Order Type   : Dine-in, Takeaway, Delivery
    ├── Progresses through Order Statuses.                Ex           : Received, Preparing, Ready, Delivered, Completed
    ├── Is associated with one Payment.                   Payment Types: Cash, Card, Digital Wallet
    └── Can receive one Customer Review after completion.
```
## Conceptual Data Model

<p align="center">
    <img src="diagrams/Conceptual_Data_Model.png" width="1000"/>
</p>


# Databricks Database Ingestion Methods

I have used **Lakeflow Connect (CDC-enabled ingestion)** in my pipeline.

---

## 1. Lakeflow (CDC-enabled)
- Captures real-time changes (insert, update, delete) from source systems.
- Bronze stores raw CDC events as-is.
- Silver layer processes changes using CDF/streaming, including:
  - Data cleaning
  - Deduplication
  - SCD Type 1 / Type 2 modeling
- Best suited for modern near real-time lakehouse architectures.

---

## 2. Query-based (Watermark / Incremental)
- Used when CDC is not available in the source system.
- Uses incremental extraction based on `updated_at` (watermarking).
- Captures only new and updated records between runs.
- Deletes are handled via **soft deletes** (e.g., `is_deleted` flag).
- Best for batch-oriented incremental ingestion pipelines.

---

## 3. JDBC ingestion
- Direct ingestion from databases using JDBC connectors.
- Supports full load and incremental extraction (if supported by source).
- Common for legacy systems and simple ingestion needs.
- Less scalable and requires higher operational maintenance.

---

## 4. Federated querying (Lakehouse Federation)
- No data ingestion into Databricks.
- Data remains in the source system and is queried directly.
- Best for TB/PB-scale datasets or ad-hoc exploration.
- Performance depends on source system latency and capacity.

---

## Summary
- Lakeflow CDC      → Real-time ingestion (recommended modern approach)
- Watermarking      → Incremental batch ingestion
- JDBC              → Legacy ingestion approach
- Federation        → Query data without ingestion


# Lakeflow Connect - CDC Based

**Tables ingested via Lakeflow Connect into Bronze:** `historical_orders`, `reviews`
**Tables ingested via Lakeflow Connect into Silver:** `Customers`, `menu_items`, `restaurants`

---

## Step 1: Create a Source Connection

- Configure the connection to the source system.
- Example: Azure SQL Database, SQL Server, Salesforce, etc.
- Provide host, port, database, username, password, and authentication.

---

## Step 2: Create an Ingestion Pipeline

- Define pipeline name and Event Log location (stores pipeline execution logs and metrics).
- Choose the ingestion mode:

### CDC (Change Data Capture)

- Reads DML changes directly from the database transaction log.
- **Use when:** The source database supports CDC and you need near real-time synchronization.
- **Why:** Lowest source impact, captures all data changes (including deletes), and provides continuous ingestion.

### Query-based Capture

- Uses a cursor (watermark) column to query new or updated records on a schedule.
- **Use when:** CDC is unavailable, unsupported, or cannot be enabled on the source database.
- **Why:** Simple to configure, works with many databases, but relies on a watermark column and captures only the latest state of changed rows between scheduled runs.

---

## Step 3: Select Source Tables

For each source table, configure:

- **Cursor Column:** Watermark column used to detect newly inserted or updated records. Enables incremental ingestion.
- **Primary Key(s):** Unique identifier for each row. Used to identify records during incremental loads, merges, and updates.
- **History Tracking**
  - **OFF** → SCD Type 1 (keeps only the latest version of a record).
  - **ON** → SCD Type 2 (maintains historical versions of records, maintains all inserts, updates, and deletes).

---

## Step 4: Configure Destination

- Select the Unity Catalog catalog, schema, and destination Delta table.

---

## Step 5: Schedule & Notifications
- Configure pipeline schedule, alerts, and notifications.

