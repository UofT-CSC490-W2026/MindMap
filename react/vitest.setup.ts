import '@testing-library/jest-dom/vitest'

if (!HTMLElement.prototype.scrollIntoView) {
  HTMLElement.prototype.scrollIntoView = () => {}
}

// Provide two distinct API base URLs so sematicSearch fallback logic has two entries to try
import.meta.env.VITE_SEARCH_API_URL = 'https://search-api.test'
import.meta.env.VITE_API_URL = 'https://api.test'
