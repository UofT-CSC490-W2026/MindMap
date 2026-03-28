import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import Sidebar from '../../../../react/src/components/layout/SideBar'

describe('Sidebar', () => {
  it('renders navigation options', () => {
    render(<Sidebar />)
    expect(screen.getByText('Navigation')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Citation Network' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Topic Clusters' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Authors' })).toBeInTheDocument()
  })
})

