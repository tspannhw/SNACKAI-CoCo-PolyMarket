import { z } from 'zod';

export const MarketSchema = z.object({
  ID: z.string().nullable().optional(),
  QUESTION: z.string().nullable().optional(),
  CATEGORY: z.string().nullable().optional(),
  OUTCOME_PRICES: z.string().nullable().optional(),
  OUTCOMES: z.string().nullable().optional(),
  VOLUME: z.number().nullable().optional(),
  VOLUME_NUM: z.number().nullable().optional(),
  VOLUME_24HR: z.number().nullable().optional(),
  VOLUME_1WK: z.number().nullable().optional(),
  LIQUIDITY: z.number().nullable().optional(),
  LIQUIDITY_NUM: z.number().nullable().optional(),
  SPREAD: z.number().nullable().optional(),
  ACTIVE: z.boolean().nullable().optional(),
  CLOSED: z.boolean().nullable().optional(),
  END_DATE: z.string().nullable().optional(),
  MARKET_TYPE: z.string().nullable().optional(),
  IMAGE: z.string().nullable().optional(),
  INGESTED_AT: z.string().nullable().optional(),
  BATCH_ID: z.string().nullable().optional(),
  SCORE: z.number().nullable().optional(),
});

export type Market = z.infer<typeof MarketSchema>;

export const VolumeSummarySchema = z.object({
  CATEGORY: z.string().nullable().optional(),
  MARKET_COUNT: z.number().nullable().optional(),
  TOTAL_VOLUME: z.number().nullable().optional(),
  TOTAL_24HR_VOLUME: z.number().nullable().optional(),
  TOTAL_LIQUIDITY: z.number().nullable().optional(),
  AVG_SPREAD: z.number().nullable().optional(),
});

export type VolumeSummary = z.infer<typeof VolumeSummarySchema>;

export const IngestionMetricSchema = z.object({
  MINUTE_BUCKET: z.string().nullable().optional(),
  BATCHES: z.number().nullable().optional(),
  TOTAL_MARKETS: z.number().nullable().optional(),
  AVG_DURATION_MS: z.number().nullable().optional(),
  MAX_DURATION_MS: z.number().nullable().optional(),
  ERROR_COUNT: z.number().nullable().optional(),
});

export type IngestionMetric = z.infer<typeof IngestionMetricSchema>;

export const ExportRequestSchema = z.object({
  data: z.array(z.record(z.unknown())),
  filename: z.string(),
  format: z.enum(['csv', 'json']),
});

export type ExportRequest = z.infer<typeof ExportRequestSchema>;

export function parseOutcomes(outcomes: string | null | undefined): string[] {
  if (!outcomes) return [];
  try {
    const parsed = JSON.parse(outcomes);
    return Array.isArray(parsed) ? parsed : [outcomes];
  } catch {
    return outcomes.split(',').map((s: string) => s.trim().replace(/"/g, ''));
  }
}

export function parseOutcomePrices(prices: string | null | undefined): number[] {
  if (!prices) return [];
  try {
    const parsed = JSON.parse(prices);
    return Array.isArray(parsed) ? parsed.map(Number) : [Number(prices)];
  } catch {
    return prices.split(',').map((s: string) => Number(s.trim().replace(/"/g, '')));
  }
}

export function formatVolume(vol: number | null | undefined): string {
  if (vol == null) return '$0';
  if (vol >= 1_000_000) return `$${(vol / 1_000_000).toFixed(1)}M`;
  if (vol >= 1_000) return `$${(vol / 1_000).toFixed(1)}K`;
  return `$${vol.toFixed(0)}`;
}

export function formatPrice(price: number | null | undefined): string {
  if (price == null) return '—';
  return `${(price * 100).toFixed(1)}%`;
}
