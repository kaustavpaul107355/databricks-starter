import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { TabNavigation } from '@/app/components/TabNavigation';

describe('TabNavigation Component', () => {
  const mockOnTabChange = vi.fn();

  it('renders all tabs', () => {
    render(<TabNavigation activeTab="home" onTabChange={mockOnTabChange} />);
    
    expect(screen.getByText('Executive Summary')).toBeInTheDocument();
    expect(screen.getByText('Revenue Analytics')).toBeInTheDocument();
    expect(screen.getByText('Operations')).toBeInTheDocument();
    expect(screen.getByText('Customer Insights')).toBeInTheDocument();
    expect(screen.getByText('Store Map')).toBeInTheDocument();
  });

  it('highlights the active tab', () => {
    render(<TabNavigation activeTab="revenue" onTabChange={mockOnTabChange} />);
    
    const revenueTab = screen.getByText('Revenue Analytics').closest('button');
    expect(revenueTab).toHaveClass('bg-gradient-to-br');
  });

  it('calls onTabChange when tab is clicked', () => {
    render(<TabNavigation activeTab="home" onTabChange={mockOnTabChange} />);
    
    const operationsTab = screen.getByText('Operations');
    fireEvent.click(operationsTab);
    
    expect(mockOnTabChange).toHaveBeenCalledWith('operations');
  });

  it('applies correct styling to inactive tabs', () => {
    render(<TabNavigation activeTab="home" onTabChange={mockOnTabChange} />);
    
    const mapTab = screen.getByText('Store Map').closest('button');
    expect(mapTab).toHaveClass('bg-white/70');
  });
});
