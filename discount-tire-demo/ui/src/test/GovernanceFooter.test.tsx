import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { GovernanceFooter } from '@/app/components/GovernanceFooter';

describe('GovernanceFooter Component', () => {
  it('renders the Databricks logo', () => {
    render(<GovernanceFooter />);
    
    const logo = screen.getByAltText('Databricks logo');
    expect(logo).toBeInTheDocument();
  });

  it('displays Unity Catalog information', () => {
    render(<GovernanceFooter />);
    
    expect(screen.getByText(/Unity Catalog/i)).toBeInTheDocument();
  });

  it('displays databricks platform features', () => {
    render(<GovernanceFooter />);
    
    const databricksElements = screen.getAllByText(/Databricks/i);
    expect(databricksElements.length).toBeGreaterThan(0);
  });

  it('displays data lineage information', () => {
    render(<GovernanceFooter />);
    
    expect(screen.getByText(/Lineage/i)).toBeInTheDocument();
  });
});
