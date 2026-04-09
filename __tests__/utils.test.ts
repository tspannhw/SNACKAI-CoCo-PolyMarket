import { describe, expect, test } from '@jest/globals';
import { cn, formatNumber, formatCurrency, timeAgo } from '../lib/utils';
import {
  parseOutcomes,
  parseOutcomePrices,
  formatVolume,
  formatPrice,
  ExportRequestSchema,
} from '../lib/validations';

describe('cn utility', () => {
  test('joins class names', () => {
    expect(cn('a', 'b', 'c')).toBe('a b c');
  });

  test('filters falsy values', () => {
    expect(cn('a', undefined, null, false, 'b')).toBe('a b');
  });

  test('handles empty input', () => {
    expect(cn()).toBe('');
  });
});

describe('formatNumber', () => {
  test('formats numbers with commas', () => {
    expect(formatNumber(1000)).toBe('1,000');
    expect(formatNumber(1000000)).toBe('1,000,000');
  });

  test('handles null/undefined', () => {
    expect(formatNumber(null)).toBe('0');
    expect(formatNumber(undefined)).toBe('0');
  });
});

describe('formatCurrency', () => {
  test('formats millions', () => {
    expect(formatCurrency(5000000)).toBe('$5.00M');
  });

  test('formats thousands', () => {
    expect(formatCurrency(50000)).toBe('$50.0K');
  });

  test('formats small values', () => {
    expect(formatCurrency(42.5)).toBe('$42.50');
  });

  test('handles null', () => {
    expect(formatCurrency(null)).toBe('$0');
  });
});

describe('timeAgo', () => {
  test('handles null', () => {
    expect(timeAgo(null)).toBe('N/A');
    expect(timeAgo(undefined)).toBe('N/A');
  });

  test('handles recent time', () => {
    const now = new Date().toISOString();
    expect(timeAgo(now)).toBe('just now');
  });

  test('handles minutes ago', () => {
    const fiveMinAgo = new Date(Date.now() - 5 * 60000).toISOString();
    expect(timeAgo(fiveMinAgo)).toBe('5m ago');
  });

  test('handles hours ago', () => {
    const threeHoursAgo = new Date(Date.now() - 3 * 3600000).toISOString();
    expect(timeAgo(threeHoursAgo)).toBe('3h ago');
  });

  test('handles days ago', () => {
    const twoDaysAgo = new Date(Date.now() - 2 * 86400000).toISOString();
    expect(timeAgo(twoDaysAgo)).toBe('2d ago');
  });
});

describe('parseOutcomes', () => {
  test('parses JSON array', () => {
    expect(parseOutcomes('["Yes","No"]')).toEqual(['Yes', 'No']);
  });

  test('parses comma-separated string', () => {
    expect(parseOutcomes('Yes,No')).toEqual(['Yes', 'No']);
  });

  test('handles null/undefined/empty', () => {
    expect(parseOutcomes(null)).toEqual([]);
    expect(parseOutcomes(undefined)).toEqual([]);
    expect(parseOutcomes('')).toEqual([]);
  });

  test('handles single value JSON', () => {
    expect(parseOutcomes('["Yes"]')).toEqual(['Yes']);
  });

  test('strips quotes from comma-separated', () => {
    expect(parseOutcomes('"Yes","No"')).toEqual(['Yes', 'No']);
  });
});

describe('parseOutcomePrices', () => {
  test('parses JSON array of numbers', () => {
    expect(parseOutcomePrices('["0.72","0.28"]')).toEqual([0.72, 0.28]);
  });

  test('parses JSON array of raw numbers', () => {
    expect(parseOutcomePrices('[0.65, 0.35]')).toEqual([0.65, 0.35]);
  });

  test('parses comma-separated string', () => {
    expect(parseOutcomePrices('0.5,0.5')).toEqual([0.5, 0.5]);
  });

  test('handles null/undefined/empty', () => {
    expect(parseOutcomePrices(null)).toEqual([]);
    expect(parseOutcomePrices(undefined)).toEqual([]);
    expect(parseOutcomePrices('')).toEqual([]);
  });

  test('handles single value', () => {
    expect(parseOutcomePrices('0.99')).toEqual([0.99]);
  });
});

describe('formatVolume', () => {
  test('formats millions', () => {
    expect(formatVolume(5000000)).toBe('$5.0M');
  });

  test('formats thousands', () => {
    expect(formatVolume(50000)).toBe('$50.0K');
  });

  test('formats small values', () => {
    expect(formatVolume(42)).toBe('$42');
  });

  test('handles null/undefined', () => {
    expect(formatVolume(null)).toBe('$0');
    expect(formatVolume(undefined)).toBe('$0');
  });

  test('handles zero', () => {
    expect(formatVolume(0)).toBe('$0');
  });
});

describe('formatPrice', () => {
  test('formats as percentage', () => {
    expect(formatPrice(0.72)).toBe('72.0%');
    expect(formatPrice(0.5)).toBe('50.0%');
    expect(formatPrice(1)).toBe('100.0%');
    expect(formatPrice(0)).toBe('0.0%');
  });

  test('handles null/undefined', () => {
    expect(formatPrice(null)).toBe('—');
    expect(formatPrice(undefined)).toBe('—');
  });
});

describe('ExportRequestSchema', () => {
  test('validates correct CSV request', () => {
    const result = ExportRequestSchema.safeParse({
      data: [{ id: '1', question: 'Test' }],
      filename: 'export_2025',
      format: 'csv',
    });
    expect(result.success).toBe(true);
  });

  test('validates correct JSON request', () => {
    const result = ExportRequestSchema.safeParse({
      data: [{ id: '1' }],
      filename: 'export',
      format: 'json',
    });
    expect(result.success).toBe(true);
  });

  test('rejects invalid format', () => {
    const result = ExportRequestSchema.safeParse({
      data: [],
      filename: 'test',
      format: 'xml',
    });
    expect(result.success).toBe(false);
  });

  test('rejects missing data', () => {
    const result = ExportRequestSchema.safeParse({
      filename: 'test',
      format: 'csv',
    });
    expect(result.success).toBe(false);
  });

  test('accepts empty data array', () => {
    const result = ExportRequestSchema.safeParse({
      data: [],
      filename: 'empty',
      format: 'csv',
    });
    expect(result.success).toBe(true);
  });
});
