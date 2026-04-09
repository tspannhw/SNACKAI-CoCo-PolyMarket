import { describe, expect, test } from '@jest/globals';
import { cn, formatNumber, formatCurrency, timeAgo } from '../lib/utils';

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
