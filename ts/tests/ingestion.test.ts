/**
 * Ingestion Engine Tests
 * 
 * Tests for Data Physics validation rules:
 * - Hard rules (rejection)
 * - Soft rules (suspect)
 * - Contextual rules (anomaly detection)
 * - Dead letter queue handling
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  IngestionEngine,
  RawTickData,
  DataQuality,
} from '../src';

// ============================================================================
// Test Fixtures
// ============================================================================

function createValidTick(overrides: Partial<RawTickData> = {}): RawTickData {
  return {
    symbol: 'AAPL',
    price: '150.00',
    volume: 100,
    timestamp: new Date().toISOString(),
    exchange: 'NASDAQ',
    ...overrides,
  };
}

// ============================================================================
// Hard Rule Tests
// ============================================================================

describe('Ingestion Engine - Hard Rules', () => {
  let engine: IngestionEngine;

  beforeEach(() => {
    engine = new IngestionEngine();
  });

  describe('Positive Price Rule', () => {
    it('should VERIFY valid positive price', () => {
      const tick = createValidTick({ price: '150.00' });
      const { quality, errors } = engine.validate(tick);
      
      expect(quality).toBe('VERIFIED');
      expect(errors).toHaveLength(0);
    });

    it('should REJECT zero price', () => {
      const tick = createValidTick({ price: '0' });
      const { quality, errors } = engine.validate(tick);
      
      expect(quality).toBe('REJECTED');
      expect(errors.some(e => e.rule === 'positive_price')).toBe(true);
    });

    it('should REJECT negative price', () => {
      const tick = createValidTick({ price: '-5.00' });
      const { quality, errors } = engine.validate(tick);
      
      expect(quality).toBe('REJECTED');
      expect(errors.some(e => e.rule === 'positive_price')).toBe(true);
    });

    it('should REJECT invalid price format', () => {
      const tick = createValidTick({ price: 'not-a-number' });
      const { quality, errors } = engine.validate(tick);
      
      expect(quality).toBe('REJECTED');
    });
  });

  describe('Price Ceiling Rule', () => {
    it('should VERIFY price below ceiling', () => {
      const tick = createValidTick({ price: '500000.00' });
      const { quality } = engine.validate(tick);
      
      expect(quality).toBe('VERIFIED');
    });

    it('should REJECT price above ceiling', () => {
      const tick = createValidTick({ price: '1500000.00' });
      const { quality, errors } = engine.validate(tick);
      
      expect(quality).toBe('REJECTED');
      expect(errors.some(e => e.rule === 'price_ceiling')).toBe(true);
    });
  });

  describe('Valid Timestamp Rule', () => {
    it('should VERIFY valid ISO timestamp', () => {
      const tick = createValidTick({ timestamp: new Date().toISOString() });
      const { quality } = engine.validate(tick);
      
      expect(quality).toBe('VERIFIED');
    });

    it('should REJECT invalid timestamp', () => {
      const tick = createValidTick({ timestamp: 'not-a-timestamp' });
      const { quality, errors } = engine.validate(tick);
      
      expect(quality).toBe('REJECTED');
      expect(errors.some(e => e.rule === 'valid_timestamp')).toBe(true);
    });
  });

  describe('No Future Timestamp Rule', () => {
    it('should VERIFY current timestamp', () => {
      const tick = createValidTick({ timestamp: new Date().toISOString() });
      const { quality } = engine.validate(tick);
      
      expect(quality).toBe('VERIFIED');
    });

    it('should REJECT future timestamp', () => {
      const future = new Date(Date.now() + 60000).toISOString(); // 1 minute ahead
      const tick = createValidTick({ timestamp: future });
      const { quality, errors } = engine.validate(tick);
      
      expect(quality).toBe('REJECTED');
      expect(errors.some(e => e.rule === 'no_future_timestamp')).toBe(true);
    });
  });
});

// ============================================================================
// Soft Rule Tests
// ============================================================================

describe('Ingestion Engine - Soft Rules', () => {
  let engine: IngestionEngine;

  beforeEach(() => {
    engine = new IngestionEngine({ stalenessThresholdMs: 60000 });
  });

  describe('Staleness Rule', () => {
    it('should VERIFY fresh data', () => {
      const tick = createValidTick({ timestamp: new Date().toISOString() });
      const { quality } = engine.validate(tick);
      
      expect(quality).toBe('VERIFIED');
    });

    it('should mark SUSPECT for stale data', () => {
      const old = new Date(Date.now() - 120000).toISOString(); // 2 minutes ago
      const tick = createValidTick({ timestamp: old });
      const { quality, errors } = engine.validate(tick);
      
      expect(quality).toBe('SUSPECT');
      expect(errors.some(e => e.rule === 'staleness')).toBe(true);
    });
  });

  describe('Sequence Gap Rule', () => {
    it('should detect sequence gaps', () => {
      // First tick with seq 1
      engine.validate(createValidTick({ sequenceId: 1 }));
      
      // Skip to seq 5 (gap of 3)
      const { quality, errors } = engine.validate(createValidTick({ sequenceId: 5 }));
      
      expect(quality).toBe('SUSPECT');
      expect(errors.some(e => e.rule === 'sequence_gap')).toBe(true);
    });

    it('should not flag correct sequence', () => {
      engine.validate(createValidTick({ sequenceId: 1 }));
      const { quality } = engine.validate(createValidTick({ sequenceId: 2 }));
      
      expect(quality).toBe('VERIFIED');
    });
  });
});

// ============================================================================
// Contextual Rule Tests
// ============================================================================

describe('Ingestion Engine - Contextual Rules', () => {
  let engine: IngestionEngine;

  beforeEach(() => {
    engine = new IngestionEngine({ priceChangeThreshold: 0.10 });
  });

  describe('Price Inertia Rule', () => {
    it('should allow small price changes', () => {
      engine.validate(createValidTick({ price: '100.00' }));
      const { quality } = engine.validate(createValidTick({ price: '105.00' })); // 5% change
      
      expect(quality).toBe('VERIFIED');
    });

    it('should flag large price changes', () => {
      engine.validate(createValidTick({ price: '100.00' }));
      const { quality, errors } = engine.validate(createValidTick({ price: '120.00' })); // 20% change
      
      expect(quality).toBe('SUSPECT');
      expect(errors.some(e => e.rule === 'price_inertia')).toBe(true);
    });
  });
});

// ============================================================================
// Dead Letter Queue Tests
// ============================================================================

describe('Ingestion Engine - Dead Letter Queue', () => {
  let engine: IngestionEngine;

  beforeEach(() => {
    engine = new IngestionEngine({ enableDeadLetterQueue: true });
  });

  it('should add rejected records to DLQ', () => {
    const badTick = createValidTick({ price: '-5.00' });
    engine.validate(badTick);
    
    const dlq = engine.getDeadLetterQueue();
    
    expect(dlq).toHaveLength(1);
    expect(dlq[0].errors.length).toBeGreaterThan(0);
  });

  it('should not add valid records to DLQ', () => {
    const goodTick = createValidTick();
    engine.validate(goodTick);
    
    const dlq = engine.getDeadLetterQueue();
    
    expect(dlq).toHaveLength(0);
  });

  it('should track retry count', () => {
    const badTick = createValidTick({ price: '-5.00' });
    engine.validate(badTick);
    
    const dlq = engine.getDeadLetterQueue();
    
    expect(dlq[0].retryCount).toBe(0);
  });

  it('should clear DLQ after processing', () => {
    engine.validate(createValidTick({ price: '-5.00' }));
    engine.clearDeadLetterQueue();
    
    expect(engine.getDeadLetterQueue()).toHaveLength(0);
  });
});

// ============================================================================
// Context Tracking Tests
// ============================================================================

describe('Ingestion Engine - Context Tracking', () => {
  let engine: IngestionEngine;

  beforeEach(() => {
    engine = new IngestionEngine();
  });

  it('should track symbols after validation', () => {
    engine.validate(createValidTick({ symbol: 'AAPL' }));
    engine.validate(createValidTick({ symbol: 'GOOGL' }));
    
    const symbols = engine.getTrackedSymbols();
    
    expect(symbols).toContain('AAPL');
    expect(symbols).toContain('GOOGL');
  });

  it('should maintain price history', () => {
    engine.validate(createValidTick({ symbol: 'AAPL', price: '100.00' }));
    engine.validate(createValidTick({ symbol: 'AAPL', price: '101.00' }));
    engine.validate(createValidTick({ symbol: 'AAPL', price: '102.00' }));
    
    const context = engine.getContext('AAPL');
    
    expect(context).toBeDefined();
    expect(context!.priceHistory.length).toBe(3);
    expect(context!.tickCount).toBe(3);
  });
});
