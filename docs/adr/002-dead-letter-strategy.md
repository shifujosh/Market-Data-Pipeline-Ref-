# 2. Dead Letter Queue Strategy

**Date:** 2025-09-22

**Status:** Accepted

---

## Context

In high-throughput data pipelines, some records will inevitably fail validation. The question is: what happens to them?

Options:
1. **Drop silently** - Fast, but data loss
2. **Crash the pipeline** - No data loss, but blocks everything
3. **Dead Letter Queue** - Preserve for later processing

---

## Decision

Implement a **Dead Letter Queue (DLQ)** for all rejected records.

### DLQ Record Structure

Every rejected record is preserved with full context:

```typescript
interface DeadLetterRecord {
  id: string;              // UUID for tracking
  originalData: unknown;   // Exact input received
  errors: ValidationError[]; // All failures
  timestamp: string;       // When rejected
  source: string;          // Which validator
  retryCount: number;      // Retry attempts
  lastRetryAt?: string;    // Last retry time
}
```

### Retention Policy

| Environment | Retention | Storage |
|------------|-----------|---------|
| Development | 24 hours | In-memory |
| Staging | 7 days | Redis |
| Production | 30 days | S3 + DynamoDB |

---

## Benefits

### 1. Nothing Disappears Silently

Every rejected record is traceable:
- What was the original data?
- Why was it rejected?
- When did this happen?

### 2. Enables Recovery

Bad data sources can be identified and fixed, then records can be replayed:

```typescript
// Replay DLQ after fixing data source
for (const record of dlq) {
  const fixed = applyFix(record.originalData);
  pipeline.ingest(fixed);
}
```

### 3. Compliance

Regulated environments require audit trails. DLQ provides:
- Complete rejection history
- Error categorization
- Timing data

---

## Monitoring

### Alerts

| Metric | Threshold | Action |
|--------|-----------|--------|
| DLQ Size | >1000 records/hour | Warning |
| DLQ Growth Rate | >5% of ingest rate | Critical |
| Same Error Type | >100 in 10 min | Investigate source |

### Dashboard

```
┌─────────────────────────────────────────┐
│  Dead Letter Queue Status               │
│                                         │
│  Records: 47       Last Hour: 12        │
│  Top Errors:                            │
│    - positive_price: 23                 │
│    - valid_timestamp: 15                │
│    - staleness: 9                       │
│                                         │
│  Sources:                               │
│    - exchange_feed_a: 30                │
│    - exchange_feed_b: 17                │
└─────────────────────────────────────────┘
```

---

## Consequences

### Positive

- **Zero silent data loss**
- **Full audit trail**
- **Enables bulk replay**

### Negative

- **Storage costs** for rejected data
- **Requires monitoring** to avoid unbounded growth
