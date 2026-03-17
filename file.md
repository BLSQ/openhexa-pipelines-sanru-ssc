
````markdown
# 📊 IASO to DHIS2 Data Pipeline

## 🧩 Overview

This pipeline orchestrates the end-to-end extraction, transformation, and loading (ETL) of health supervision form data from **IASO** into **DHIS2**. It is designed to ensure data consistency, efficiency, and reliability across reporting systems.

The pipeline is modular, configurable, and optimized for both **incremental processing** and **selective reprocessing**.

---

## ⚙️ Pipeline Stages

### 1. 📥 Data Extraction (IASO → Raw Layer)
- Connects to IASO API and retrieves form submissions
- Supports:
  - Incremental extraction via `last_updated`
  - Period-based filtering (`period_to_run`)
- Converts coded values to human-readable labels
- Cleans column names and removes duplicates
- Stores data as period-based files (CSV/Parquet)

---

### 2. 🔄 Data Transformation (Raw → DHIS2 Format)
- Applies business rules and domain-specific transformations:
  - Aggregations and derived indicators
  - Yes/No → numeric conversions
- Reshapes data into DHIS2-compatible long format
- Maps fields using a configurable mapping file
- Filters invalid or incomplete records
- Outputs standardized datasets

---

### 3. 🚀 Data Loading (Transformed → DHIS2)
- Pushes data into DHIS2 using API
- Supports:
  - Dry-run mode (for safe testing)
  - Configurable import strategies
- Ensures required fields are present
- Logs results and errors

---

## 🧠 Key Features

- ✅ Incremental and selective processing
- ✅ SQLite-based caching (track extracted/transformed/pushed)
- ✅ Modular execution (run extract/transform/load independently)
- ✅ Efficient reprocessing of failed stages
- ✅ File-based architecture (raw vs transformed layers)
- ✅ Robust logging and error handling

---

## 🔧 Example Configuration

```json
{
    "dry_run": false,
    "extract": true,
    "transform": true,
    "post_to_dhis2": true,
    "period_to_run": 202602,
    "clear_previous_runs": false,
    "scheduled_run": false
}
````

---

## 🚀 Use Cases

### 1. 🎯 Targeted Monthly Reprocessing

**Scenario:** Fix errors in a specific month (e.g., February 2026)

* `period_to_run: 202602`
* Reprocess only that period
* Other periods remain untouched

**Outcome:**
Efficient correction without reprocessing all data

---

### 2. 🔁 Full End-to-End Pipeline Run

**Scenario:** Run the pipeline after data collection is complete

* All stages enabled (extract → transform → load)
* `dry_run: false` ensures real data is pushed

**Outcome:**
Data is fully processed and available in DHIS2

---

### 3. ⚡ Incremental Processing with Cache

**Scenario:** Avoid reprocessing already handled data

* `clear_previous_runs: false`
* Pipeline skips completed stages automatically

**Outcome:**
Faster execution and reduced compute usage

---

### 4. 🛠️ Recovery from Partial Failure

**Scenario:** Pipeline failed during transform or load

* Cache ensures completed stages are skipped
* Only failed steps are rerun

**Outcome:**
Quick recovery without restarting everything

---

### 5. 🏭 Production Data Load

**Scenario:** Run pipeline in production environment

* `dry_run: false`
* Data is written to DHIS2

**Outcome:**
Live reporting data is updated

---

## 🔄 Alternative Configurations

### 🧪 A. Testing Mode (Dry Run)

```json
{
    "dry_run": true,
    "extract": true,
    "transform": true,
    "post_to_dhis2": true
}
```

**Use Case:** Validate pipeline without writing to DHIS2
**Outcome:** Safe testing

---

### 🔄 B. Transformation Only

```json
{
    "extract": false,
    "transform": true,
    "post_to_dhis2": false
}
```

**Use Case:** Iterate on transformation logic
**Outcome:** Faster development cycles

---

### 🚀 C. Push Only

```json
{
    "extract": false,
    "transform": false,
    "post_to_dhis2": true
}
```

**Use Case:** Re-send already transformed data
**Outcome:** Useful for fixing DHIS2-side issues

---

### 🔥 D. Full Pipeline Reset

```json
{
    "clear_previous_runs": true
}
```

**Use Case:** Major logic or mapping changes
**Outcome:** Entire pipeline reprocesses all data

---

### ⏱️ E. Scheduled Runs (Planned)

```json
{
    "scheduled_run": true
}
```

**Use Case:** Automatically process recent periods (e.g., last 3 months)
**Outcome:** Continuous data updates

---

## 🧠 Key Insight

This is a **state-aware ETL pipeline**:

* Tracks progress per period (extract → transform → push)
* Enables selective and efficient reprocessing
* Supports safe experimentation (dry runs)
* Reduces redundant computation through caching

---

## 📌 Ideal Use Case

This pipeline is ideal for organizations that:

* Collect field data using IASO
* Need to integrate data into DHIS2
* Require reliable, scalable, and repeatable data workflows

---

## 🏁 Conclusion

The IASO → DHIS2 pipeline provides a robust and flexible foundation for health data integration, enabling teams to:

* Maintain high data quality
* Reduce operational overhead
* Deliver timely insights for decision-making

```