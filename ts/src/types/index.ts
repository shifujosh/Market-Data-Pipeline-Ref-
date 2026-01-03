/**
 * Market Data Types - Zod Schemas for Financial Data
 * 
 * Implements "Data Physics" principles:
 * - Explicit types with no implicit defaults
 * - Financial-grade decimal precision
 * - Temporal validation
 */

import { z } from 'zod';
import Decimal from 'decimal.js';

// ============================================================================
// Decimal Handling
// ============================================================================

/**
 * Convert string to Decimal for financial precision.
 * JavaScript numbers are IEEE 754 floats - unacceptable for finance.
 */
export function toDecimal(value: string): Decimal {
  return new Decimal(value);
}

/**
 * Zod transformer for decimal strings.
 */
export const DecimalString = z.string().transform((val, ctx) => {
  try {
    const decimal = new Decimal(val);
    if (decimal.isNaN()) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Invalid decimal value',
      });
      return z.NEVER;
    }
    return decimal;
  } catch {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Failed to parse decimal',
    });
    return z.NEVER;
  }
});

// ============================================================================
// Core Types
// ============================================================================

export const ExchangeCode = z.enum([
  'NYSE', 'NASDAQ', 'AMEX', 'ARCA', 'BATS', 
  'IEX', 'CME', 'CBOE', 'LSE', 'TSE'
]);
export type ExchangeCode = z.infer<typeof ExchangeCode>;

export const DataQuality = z.enum(['VERIFIED', 'SUSPECT', 'REJECTED']);
export type DataQuality = z.infer<typeof DataQuality>;

export const MarketState = z.enum([
  'PRE_MARKET', 'OPEN', 'CLOSED', 'HALTED', 'AUCTION'
]);
export type MarketState = z.infer<typeof MarketState>;

// ============================================================================
// Tick Data Schema
// ============================================================================

/**
 * Raw tick data as received from exchange feed.
 */
export const RawTickDataSchema = z.object({
  symbol: z.string().min(1).max(10).toUpperCase(),
  price: z.string(),
  volume: z.number().int().min(0),
  timestamp: z.string(),
  exchange: z.string().optional(),
  sequenceId: z.number().int().optional(),
  bidPrice: z.string().optional(),
  askPrice: z.string().optional(),
  bidSize: z.number().int().optional(),
  askSize: z.number().int().optional(),
});
export type RawTickData = z.infer<typeof RawTickDataSchema>;

/**
 * Validated tick data with parsed decimals.
 */
export const TickDataSchema = z.object({
  symbol: z.string().min(1).max(10).toUpperCase(),
  price: DecimalString.refine(
    (d) => d.gt(0),
    { message: 'Price must be positive' }
  ),
  volume: z.number().int().min(0),
  timestamp: z.string().datetime({ offset: true }),
  exchange: ExchangeCode.optional(),
  sequenceId: z.number().int().optional(),
  bidPrice: DecimalString.optional(),
  askPrice: DecimalString.optional(),
  bidSize: z.number().int().optional(),
  askSize: z.number().int().optional(),
});
export type TickData = z.infer<typeof TickDataSchema>;

// ============================================================================
// Enriched Data
// ============================================================================

/**
 * Tick data enriched with ingestion metadata.
 */
export const EnrichedTickSchema = TickDataSchema.extend({
  ingestionTimestamp: z.string().datetime({ offset: true }),
  ingestionLatencyMs: z.number().int().min(0),
  quality: DataQuality,
  sourceId: z.string().uuid(),
  batchId: z.string().uuid().optional(),
});
export type EnrichedTick = z.infer<typeof EnrichedTickSchema>;

// ============================================================================
// Validation Error Types
// ============================================================================

export const ValidationSeverity = z.enum(['hard', 'soft', 'warning']);
export type ValidationSeverity = z.infer<typeof ValidationSeverity>;

export const ValidationErrorSchema = z.object({
  rule: z.string(),
  severity: ValidationSeverity,
  message: z.string(),
  field: z.string().optional(),
  expected: z.unknown().optional(),
  actual: z.unknown().optional(),
});
export type ValidationError = z.infer<typeof ValidationErrorSchema>;

// ============================================================================
// Dead Letter Record
// ============================================================================

export const DeadLetterRecordSchema = z.object({
  id: z.string().uuid(),
  originalData: z.unknown(),
  errors: z.array(ValidationErrorSchema),
  timestamp: z.string().datetime({ offset: true }),
  source: z.string(),
  retryCount: z.number().int().min(0).default(0),
  lastRetryAt: z.string().datetime({ offset: true }).optional(),
});
export type DeadLetterRecord = z.infer<typeof DeadLetterRecordSchema>;

// ============================================================================
// Context Types
// ============================================================================

export const SymbolContextSchema = z.object({
  symbol: z.string(),
  lastPrice: DecimalString,
  lastVolume: z.number().int(),
  lastTimestamp: z.string().datetime({ offset: true }),
  priceHistory: z.array(DecimalString),
  volumeHistory: z.array(z.number().int()),
  tickCount: z.number().int(),
  avgPrice: DecimalString.optional(),
  volatility: z.number().optional(),
  lastSequenceId: z.number().int().optional(),
});
export type SymbolContext = z.infer<typeof SymbolContextSchema>;

// ============================================================================
// Batch Types
// ============================================================================

export const BatchResultSchema = z.object({
  batchId: z.string().uuid(),
  totalRecords: z.number().int(),
  verified: z.number().int(),
  suspect: z.number().int(),
  rejected: z.number().int(),
  processingTimeMs: z.number().int(),
  startTimestamp: z.string().datetime({ offset: true }),
  endTimestamp: z.string().datetime({ offset: true }),
});
export type BatchResult = z.infer<typeof BatchResultSchema>;

// ============================================================================
// Configuration Types
// ============================================================================

export const PipelineConfigSchema = z.object({
  environment: z.enum(['development', 'staging', 'production']),
  batchSize: z.number().int().min(1).max(10000).default(1000),
  maxLatencyMs: z.number().int().min(0).default(5000),
  priceChangeThreshold: z.number().min(0).max(1).default(0.1),
  stalenessThresholdMs: z.number().int().min(0).default(60000),
  enableDeadLetterQueue: z.boolean().default(true),
  sampleRate: z.number().min(0).max(1).default(1.0),
});
export type PipelineConfig = z.infer<typeof PipelineConfigSchema>;
