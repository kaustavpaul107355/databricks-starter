import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Header } from '@/app/components/Header';

// Mock fetch for user info
global.fetch = vi.fn();

describe('Header Component', () => {
  it('renders the Discount Tire logo', () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ name: 'Test User', email: 'test@test.com', role: 'Executive' }),
    });

    render(<Header />);
    
    const logo = screen.getByAltText('Discount Tire logo');
    expect(logo).toBeInTheDocument();
  });

  it('renders the application title', () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ name: 'Test User', email: 'test@test.com', role: 'Executive' }),
    });

    render(<Header />);
    
    expect(screen.getByText('DISCOUNT TIRE')).toBeInTheDocument();
    expect(screen.getByText('Executive Business Brief')).toBeInTheDocument();
  });

  it('displays the Built on Databricks badge', () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ name: 'Test User', email: 'test@test.com', role: 'Executive' }),
    });

    render(<Header />);
    
    expect(screen.getByText('Built on Databricks Data & AI Platform')).toBeInTheDocument();
  });
});
