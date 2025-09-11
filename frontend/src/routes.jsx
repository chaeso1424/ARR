// src/routes.jsx
import React, { Suspense, Fragment, lazy } from 'react';
import { Routes, Navigate, Route } from 'react-router-dom';
import AdminLayout from './layouts/AdminLayout';

const isAuthed = () => !!localStorage.getItem('auth_token');
function RequireAuth({ children }) { return isAuthed() ? children : <Navigate to="/auth/signin-1" replace />; }
function LoginOnly({ children }) { return isAuthed() ? <Navigate to="/app/dashboard/analytics" replace /> : children; }
const Fallback = () => <div style={{ padding: 24, fontWeight: 700 }}>Loading…</div>;

const renderRoutes = (routes = []) => (
  <Suspense fallback={<Fallback />}>
    <Routes>
      {routes.map((route, i) => {
        const Guard = route.guard || Fragment;
        const Layout = route.layout || Fragment;
        const Element = route.element;
        return (
          <Route
            key={i}
            path={route.path}
            element={
              <Guard>
                <Layout>{route.routes ? renderRoutes(route.routes) : <Element props={true} />}</Layout>
              </Guard>
            }
          />
        );
      })}
    </Routes>
  </Suspense>
);

export const routes = [
  { path: '/', element: () => (<Navigate to={isAuthed() ? '/app/dashboard/analytics' : '/auth/signin-1'} replace />) },
  { path: '/auth/signin-1', element: lazy(() => import('./views/auth/signin/SignIn1')), guard: LoginOnly },
  {
    path: '*',
    layout: AdminLayout,
    routes: [
      { path: '/app/dashboard/analytics', element: lazy(() => import('./views/dashboard')), guard: RequireAuth },
      { path: '*', element: () => (<Navigate to={isAuthed() ? '/app/dashboard/analytics' : '/auth/signin-1'} replace />) },
      { path: '/app/logs', element: lazy(() => import('./views/logs')), guard: RequireAuth }
    ]
  },
];

export default renderRoutes;
