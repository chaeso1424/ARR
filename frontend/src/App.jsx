// src/App.jsx
import React from 'react';
import renderRoutes, { routes } from './routes';
export default function App() {
  return renderRoutes(routes);
}
