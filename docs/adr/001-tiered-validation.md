# 1. Tiered Validation Strategy

**Date:** 2025-09-20

**Status:** Accepted

---

## Context

Financial data pipelines must balance strictness with throughput. Rejecting every slightly anomalous record would grind the system to a halt. Accepting everything would let garbage pollute downstream systems.

We needed a validation strategy that:
- Catches catastrophic errors (hard rejection)
- Flags suspicious data for review (soft flagging)
- Maintains high throughput

---

## Decision

Implement a **three-tier validation strategy**:

### Tier 1: Hard Rules (Rejection)

Violations that make the record unusable:
- Price is negative or zero
- Price exceeds maximum ceiling
- Timestamp is invalid
- Timestamp is in the future

**Outcome:** Record is REJECTED and sent to Dead Letter Queue.

### Tier 2: Soft Rules (Suspect)

Violations that are concerning but not fatal:
- Data is stale (>60s old)
- Sequence gap detected
- Price changed more than threshold

**Outcome:** Record is marked SUSPECT and processed with flag.

### Tier 3: Warnings (Logged)

Anomalies worth noting but not affecting processing:
- Unusual volume
- First tick for symbol (no context)

**Outcome:** Record is VERIFIED with warning logged.

---

## Architecture

```
Raw Data ──▶ Hard Rules ──▶ Soft Rules ──▶ Output
              │                │
              ▼                ▼
           REJECTED         SUSPECT
              │
              ▼
         Dead Letter
            Queue
```

---

## Consequences

### Positive

- **High Throughput:** Only catastrophic errors cause rejection
- **Visibility:** Suspect data is flagged for monitoring
- **Recoverability:** Rejected records are preserved for analysis

### Negative

- **Complexity:** Three-tier system requires careful rule classification
- **Monitoring Overhead:** Suspect data needs attention

---

## Implementation

```typescript
// Determine quality based on error severity
const hasHard = errors.some(e => e.severity === 'hard');
const hasSoft = errors.some(e => e.severity === 'soft');

let quality: DataQuality;
if (hasHard) {
  quality = 'REJECTED';
} else if (hasSoft) {
  quality = 'SUSPECT';
} else {
  quality = 'VERIFIED';
}
```
