# Reference Architecture: Financial Data Ingestion Pipeline

> **Note:** This repository is a sanitized reference architecture demonstrating the ETL principles I utilized for high-frequency financial data operations. It does not contain proprietary code.

### 🎯 The Objective
In high-frequency financial environments, "Data Physics" is paramount. A pipeline cannot just move data; it must ensure:
1.  **Strict Schema Validation:** Preventing data type drift.
2.  **Latency Tracking:** Monitoring the "freshness" of the data.
3.  **Auditability:** Structured logging for every batch drop.

### 🛠️ The Stack
* **Python 3.9+**: Core transformation logic.
* **Pandas**: Vectorized data manipulation.
* **YAML**: Configuration management (Env/Prod separation).
* **SQL**: Strict schema definition (see `schema/market_data.sql`).

### 🏗️ Design Decisions
* **Class-Based Structure:** Modular design allows for easier unit testing and extension.
* **Validation Layer:** The `validate_batch` method acts as a gatekeeper. In my production experience, catching a `null` here saves hours of debugging downstream in the Data Warehouse.
