import { describe, it, expect } from 'vitest';
import { formatCurrency, formatNumber, formatPercentage } from '@/utils/formatting';

/**
 * Utility function tests for data formatting
 */

describe('Data Formatting Utilities', () => {
  describe('formatCurrency', () => {
    it('formats numbers as USD currency', () => {
      expect(formatCurrency(1234567)).toBe('$1,234,567');
      expect(formatCurrency(0)).toBe('$0');
      expect(formatCurrency(99.99)).toBe('$100');
      expect(formatCurrency(null)).toBe('$0');
      expect(formatCurrency(undefined)).toBe('$0');
    });
  });

  describe('formatPercentage', () => {
    it('formats numbers as percentages', () => {
      expect(formatPercentage(12.5)).toBe('12.5%');
      expect(formatPercentage(0)).toBe('0.0%');
      expect(formatPercentage(100)).toBe('100.0%');
      expect(formatPercentage(null)).toBe('0.0%');
      expect(formatPercentage(undefined)).toBe('0.0%');
    });
  });

  describe('formatNumber', () => {
    it('formats numbers with thousands separators', () => {
      expect(formatNumber(1234567)).toBe('1,234,567');
      expect(formatNumber(0)).toBe('0');
      expect(formatNumber(42)).toBe('42');
      expect(formatNumber('1234')).toBe('1,234');
      expect(formatNumber(null)).toBe('0');
      expect(formatNumber(undefined)).toBe('0');
    });
  });
});
