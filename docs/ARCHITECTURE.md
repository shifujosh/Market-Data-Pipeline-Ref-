# Architecture Overview

This document provides a high-level view of the Market Data Pipeline reference architecture.

## System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MARKET DATA PIPELINE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐     ┌─────────────────────┐     ┌───────────────────────┐  │
│  │   Sources   │────▶│   Ingestion Layer   │────▶│   Validation Layer    │  │
│  │             │     │                     │     │                       │  │
│  │ • Exchange  │     │ • Kafka Consumer    │     │ • Hard Rules (Reject) │  │
│  │ • Vendor    │     │ • Rate Limiting     │     │ • Soft Rules (Flag)   │  │
│  │ • Internal  │     │ • Deduplication     │     │ • Anomaly Detection   │  │
│  └─────────────┘     └─────────────────────┘     └───────────────────────┘  │
│                                                            │                 │
│                              ┌─────────────────────────────┼─────────┐       │
│                              │                             │         │       │
│                              ▼                             ▼         ▼       │
│                    ┌─────────────────┐          ┌─────────────┐ ┌────────┐  │
│                    │  Dead Letter Q  │          │  Warehouse  │ │ Alerts │  │
│                    │  (Quarantine)   │          │ TimescaleDB │ │Grafana │  │
│                    └─────────────────┘          └─────────────┘ └────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Ingestion Layer (`ts/src/validators/ingestion-engine.ts`)

The ingestion layer handles:
- Receiving raw tick data from exchange feeds
- Initial parsing and type coercion
- Sequence tracking for gap detection

### 2. Validation Layer

Implements "Data Physics" through tiered validation:

| Tier | Severity | Action | Example |
|------|----------|--------|---------|
| Hard | `REJECTED` | Drop + Dead Letter | Negative price |
| Soft | `SUSPECT` | Flag + Continue | Stale data, sequence gap |
| Contextual | `SUSPECT` | Flag + Continue | Price exceeds volatility |

### 3. Dead Letter Queue

Rejected records are quarantined for:
- Manual review
- Retry after source investigation
- Audit compliance

### 4. Storage Layer

- **TimescaleDB**: Time-series optimized PostgreSQL for tick data
- **S3/Parquet**: Long-term archival with columnar compression

## Data Flow

1. **Ingest**: Raw JSON from Kafka topic
2. **Parse**: Convert to `RawTickData` type
3. **Validate**: Run through rule chain
4. **Enrich**: Add ingestion metadata
5. **Route**: Verified → Warehouse, Rejected → DLQ
6. **Monitor**: Prometheus metrics + Grafana dashboards

## Key Decisions

See [ADR Directory](./adr/) for architectural decision records:

- [ADR-001: Tiered Validation](./adr/001-tiered-validation.md)
- [ADR-002: Dead Letter Strategy](./adr/002-dead-letter-strategy.md)
