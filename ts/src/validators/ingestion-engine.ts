/**
 * Validation Engine - Data Physics Implementation
 * 
 * Implements tiered validation with:
 * - Hard rules (rejection)
 * - Soft rules (flagging)
 * - Contextual rules (anomaly detection)
 */

import Decimal from 'decimal.js';
import { v4 as uuidv4 } from 'uuid';
import {
  RawTickData,
  TickData,
  EnrichedTick,
  ValidationError,
  ValidationSeverity,
  DataQuality,
  SymbolContext,
  DeadLetterRecord,
  PipelineConfig,
  ExchangeCode,
} from '../types';

// ============================================================================
// Configuration Defaults
// ============================================================================

const DEFAULT_CONFIG: PipelineConfig = {
  environment: 'development',
  batchSize: 1000,
  maxLatencyMs: 5000,
  priceChangeThreshold: 0.10, // 10% max change
  stalenessThresholdMs: 60000, // 1 minute
  enableDeadLetterQueue: true,
  sampleRate: 1.0,
};

// ============================================================================
// Validation Rule Interface
// ============================================================================

interface ValidationRule {
  name: string;
  severity: ValidationSeverity;
  validate(data: RawTickData, context?: SymbolContext): ValidationError | null;
}

// ============================================================================
// Hard Rules (Rejection)
// ============================================================================

/**
 * Price must be positive.
 */
const PositivePriceRule: ValidationRule = {
  name: 'positive_price',
  severity: 'hard',
  validate(data) {
    try {
      const price = new Decimal(data.price);
      if (price.lte(0)) {
        return {
          rule: this.name,
          severity: this.severity,
          message: 'Price must be positive',
          field: 'price',
          actual: data.price,
        };
      }
    } catch {
      return {
        rule: this.name,
        severity: this.severity,
        message: 'Invalid price format',
        field: 'price',
        actual: data.price,
      };
    }
    return null;
  },
};

/**
 * Price cannot be unrealistically high.
 */
const PriceCeilingRule: ValidationRule = {
  name: 'price_ceiling',
  severity: 'hard',
  validate(data) {
    try {
      const price = new Decimal(data.price);
      // Berkshire Hathaway Class A is ~$600k, use $1M as ceiling
      if (price.gt(1000000)) {
        return {
          rule: this.name,
          severity: this.severity,
          message: 'Price exceeds maximum ceiling',
          field: 'price',
          expected: '<= 1000000',
          actual: data.price,
        };
      }
    } catch {
      // Already caught by PositivePriceRule
    }
    return null;
  },
};

/**
 * Timestamp must be valid ISO 8601.
 */
const ValidTimestampRule: ValidationRule = {
  name: 'valid_timestamp',
  severity: 'hard',
  validate(data) {
    const date = new Date(data.timestamp);
    if (isNaN(date.getTime())) {
      return {
        rule: this.name,
        severity: this.severity,
        message: 'Invalid timestamp format',
        field: 'timestamp',
        actual: data.timestamp,
      };
    }
    return null;
  },
};

/**
 * Timestamp cannot be in the future.
 */
const NoFutureTimestampRule: ValidationRule = {
  name: 'no_future_timestamp',
  severity: 'hard',
  validate(data) {
    const dataTime = new Date(data.timestamp).getTime();
    const now = Date.now();
    const tolerance = 5000; // 5 second tolerance for clock skew
    
    if (dataTime > now + tolerance) {
      return {
        rule: this.name,
        severity: this.severity,
        message: 'Timestamp is in the future',
        field: 'timestamp',
        expected: '<= now',
        actual: data.timestamp,
      };
    }
    return null;
  },
};

// ============================================================================
// Soft Rules (Suspect)
// ============================================================================

/**
 * Data must not be stale.
 */
class StalenessRule implements ValidationRule {
  name = 'staleness';
  severity: ValidationSeverity = 'soft';
  thresholdMs: number;

  constructor(thresholdMs: number) {
    this.thresholdMs = thresholdMs;
  }

  validate(data: RawTickData): ValidationError | null {
    const dataTime = new Date(data.timestamp).getTime();
    const age = Date.now() - dataTime;
    
    if (age > this.thresholdMs) {
      return {
        rule: this.name,
        severity: this.severity,
        message: `Data is stale (${Math.round(age / 1000)}s old)`,
        field: 'timestamp',
        expected: `< ${this.thresholdMs}ms`,
        actual: `${age}ms`,
      };
    }
    return null;
  }
}

/**
 * Detect sequence gaps (missing messages).
 */
const SequenceGapRule: ValidationRule = {
  name: 'sequence_gap',
  severity: 'soft',
  validate(data, context) {
    if (!context || data.sequenceId === undefined || context.lastSequenceId === undefined) {
      return null;
    }
    
    const expectedSeq = context.lastSequenceId + 1;
    if (data.sequenceId > expectedSeq) {
      const gap = data.sequenceId - expectedSeq;
      return {
        rule: this.name,
        severity: this.severity,
        message: `Sequence gap detected (${gap} messages missing)`,
        field: 'sequenceId',
        expected: expectedSeq,
        actual: data.sequenceId,
      };
    }
    return null;
  },
};

// ============================================================================
// Contextual Rules (Anomaly Detection)
// ============================================================================

/**
 * Price change exceeds volatility threshold (Data Inertia).
 */
class PriceInertiaRule implements ValidationRule {
  name = 'price_inertia';
  severity: ValidationSeverity = 'soft';
  threshold: number;

  constructor(threshold: number) {
    this.threshold = threshold;
  }

  validate(data: RawTickData, context?: SymbolContext): ValidationError | null {
    if (!context) return null;
    
    try {
      const newPrice = new Decimal(data.price);
      const lastPrice = context.lastPrice;
      const change = newPrice.minus(lastPrice).abs();
      const pctChange = change.div(lastPrice).toNumber();
      
      if (pctChange > this.threshold) {
        return {
          rule: this.name,
          severity: this.severity,
          message: `Price changed ${(pctChange * 100).toFixed(2)}% (exceeds ${this.threshold * 100}% threshold)`,
          field: 'price',
          expected: `within ${this.threshold * 100}%`,
          actual: `${(pctChange * 100).toFixed(2)}%`,
        };
      }
    } catch {
      // Price parsing error handled elsewhere
    }
    return null;
  }
}

// ============================================================================
// Ingestion Engine
// ============================================================================

export class IngestionEngine {
  private config: PipelineConfig;
  private rules: ValidationRule[];
  private contextMap: Map<string, SymbolContext>;
  private deadLetterQueue: DeadLetterRecord[];

  constructor(config: Partial<PipelineConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.contextMap = new Map();
    this.deadLetterQueue = [];
    
    // Initialize rules
    this.rules = [
      // Hard rules
      PositivePriceRule,
      PriceCeilingRule,
      ValidTimestampRule,
      NoFutureTimestampRule,
      // Soft rules
      new StalenessRule(this.config.stalenessThresholdMs),
      SequenceGapRule,
      // Contextual rules
      new PriceInertiaRule(this.config.priceChangeThreshold),
    ];
  }

  /**
   * Get or create symbol context for tracking.
   */
  getOrCreateContext(symbol: string): SymbolContext | undefined {
    return this.contextMap.get(symbol);
  }

  /**
   * Update context after successful validation.
   */
  private updateContext(data: RawTickData): void {
    const existing = this.contextMap.get(data.symbol);
    const price = new Decimal(data.price);
    
    if (existing) {
      existing.priceHistory.push(price);
      if (existing.priceHistory.length > 100) {
        existing.priceHistory.shift();
      }
      existing.volumeHistory.push(data.volume);
      if (existing.volumeHistory.length > 100) {
        existing.volumeHistory.shift();
      }
      existing.lastPrice = price;
      existing.lastVolume = data.volume;
      existing.lastTimestamp = data.timestamp;
      existing.lastSequenceId = data.sequenceId;
      existing.tickCount++;
    } else {
      this.contextMap.set(data.symbol, {
        symbol: data.symbol,
        lastPrice: price,
        lastVolume: data.volume,
        lastTimestamp: data.timestamp,
        priceHistory: [price],
        volumeHistory: [data.volume],
        tickCount: 1,
        lastSequenceId: data.sequenceId,
      });
    }
  }

  /**
   * Validate a single tick and return quality assessment.
   */
  validate(data: RawTickData): {
    tick: TickData | null;
    quality: DataQuality;
    errors: ValidationError[];
  } {
    const errors: ValidationError[] = [];
    const context = this.contextMap.get(data.symbol);
    
    // Run all validation rules
    for (const rule of this.rules) {
      const error = rule.validate(data, context);
      if (error) {
        errors.push(error);
      }
    }
    
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
    
    // Build validated tick if not rejected
    let tick: TickData | null = null;
    if (quality !== 'REJECTED') {
      tick = {
        symbol: data.symbol.toUpperCase(),
        price: new Decimal(data.price),
        volume: data.volume,
        timestamp: new Date(data.timestamp).toISOString(),
        exchange: data.exchange ? ExchangeCode.parse(data.exchange) : undefined,
        sequenceId: data.sequenceId,
      };
      this.updateContext(data);
    } else if (this.config.enableDeadLetterQueue) {
      this.addToDeadLetterQueue(data, errors);
    }
    
    return { tick, quality, errors };
  }

  /**
   * Add rejected record to dead letter queue.
   */
  private addToDeadLetterQueue(data: RawTickData, errors: ValidationError[]): void {
    const record: DeadLetterRecord = {
      id: uuidv4(),
      originalData: data,
      errors,
      timestamp: new Date().toISOString(),
      source: 'ingestion_engine',
      retryCount: 0,
    };
    this.deadLetterQueue.push(record);
  }

  /**
   * Get dead letter queue contents.
   */
  getDeadLetterQueue(): DeadLetterRecord[] {
    return [...this.deadLetterQueue];
  }

  /**
   * Clear dead letter queue after processing.
   */
  clearDeadLetterQueue(): void {
    this.deadLetterQueue = [];
  }

  /**
   * Get current context for a symbol.
   */
  getContext(symbol: string): SymbolContext | undefined {
    return this.contextMap.get(symbol);
  }

  /**
   * Get all tracked symbols.
   */
  getTrackedSymbols(): string[] {
    return Array.from(this.contextMap.keys());
  }
}

// ============================================================================
// Exports
// ============================================================================

export {
  ValidationRule,
  PositivePriceRule,
  PriceCeilingRule,
  ValidTimestampRule,
  NoFutureTimestampRule,
  StalenessRule,
  SequenceGapRule,
  PriceInertiaRule,
};
