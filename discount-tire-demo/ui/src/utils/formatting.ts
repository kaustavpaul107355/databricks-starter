/**
 * Shared utility functions for formatting data
 */

/**
 * Format a number as USD currency
 * @param value - Number to format
 * @returns Formatted currency string (e.g., "$1,234,567")
 */
export function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "$0";
  }
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

/**
 * Format a number with thousands separators
 * @param value - Number to format
 * @returns Formatted number string (e.g., "1,234,567")
 */
export function formatNumber(value: number | string | null | undefined): string {
  if (value === null || value === undefined) {
    return "0";
  }
  const numValue = typeof value === 'string' ? parseFloat(value) : value;
  if (isNaN(numValue)) {
    return "0";
  }
  return new Intl.NumberFormat('en-US').format(numValue);
}

/**
 * Format a number as a percentage
 * @param value - Number to format
 * @param decimals - Number of decimal places (default: 1)
 * @returns Formatted percentage string (e.g., "12.5%")
 */
export function formatPercentage(value: number | null | undefined, decimals: number = 1): string {
  if (value === null || value === undefined) {
    return "0.0%";
  }
  return `${value.toFixed(decimals)}%`;
}

/**
 * Truncate text to a maximum length with ellipsis
 * @param text - Text to truncate
 * @param maxLength - Maximum length
 * @returns Truncated text
 */
export function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  return text.slice(0, maxLength - 3) + '...';
}

/**
 * Format a date string to a readable format
 * @param dateString - ISO date string
 * @returns Formatted date (e.g., "Jan 21, 2026")
 */
export function formatDate(dateString: string | null | undefined): string {
  if (!dateString) {
    return "N/A";
  }
  try {
    const date = new Date(dateString);
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    }).format(date);
  } catch {
    return "Invalid Date";
  }
}
