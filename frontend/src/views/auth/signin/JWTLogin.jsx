import React from 'react';
import { useNavigate, Navigate } from 'react-router-dom';
import { Row, Col, Alert, Button } from 'react-bootstrap';
import * as Yup from 'yup';
import { useFormik } from 'formik';

const isAuthed = () => !!localStorage.getItem('auth_token');

const JWTLogin = () => {
  const navigate = useNavigate();

  // 이미 로그인된 상태면 대시보드로
  if (isAuthed()) return <Navigate to="/app/dashboard/analytics" replace />;

  const formik = useFormik({
    initialValues: {
      email: 'info@codedthemes.com',
      password: '123456'
    },
    validationSchema: Yup.object({
      email: Yup.string().email('Must be a valid email').max(255).required('Email is required'),
      password: Yup.string().max(255).required('Password is required')
    }),
    onSubmit: async (values, helpers) => {
      helpers.setStatus(undefined);
      helpers.setErrors({});
      try {
        // ⚠️ 백엔드가 email 키를 받도록 구현돼 있어야 합니다.
        // 만약 서버가 id를 기대한다면 아래 payload를 { id: values.email, password: values.password }로 바꾸세요.
        const payload = { email: values.email, password: values.password };

        const res = await fetch('/api/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        if (!res.ok) {
          const t = await res.text();
          throw new Error(t || `HTTP ${res.status}`);
        }

        const data = await res.json();
        if (!data?.token) throw new Error('Token missing in response');

        localStorage.setItem('auth_token', data.token);
        navigate('/app/dashboard/analytics', { replace: true });
      } catch (err) {
        helpers.setErrors({ submit: err.message || 'Login failed' });
      } finally {
        helpers.setSubmitting(false);
      }
    }
  });

  return (
    <>
      <form noValidate onSubmit={formik.handleSubmit}>
        <div className="form-group mb-3">
          <input
            className="form-control"
            label="Email Address / Username"
            name="email"
            type="email"
            onBlur={formik.handleBlur}
            onChange={formik.handleChange}
            value={formik.values.email ?? ''}   // 안전 처리
          />
          {formik.touched.email && formik.errors.email && (
            <small className="text-danger form-text">{formik.errors.email}</small>
          )}
        </div>

        <div className="form-group mb-4">
          <input
            className="form-control"
            label="Password"
            name="password"
            type="password"
            onBlur={formik.handleBlur}
            onChange={formik.handleChange}
            value={formik.values.password ?? ''} // 안전 처리
          />
          {formik.touched.password && formik.errors.password && (
            <small className="text-danger form-text">{formik.errors.password}</small>
          )}
        </div>

        <div className="custom-control custom-checkbox text-start mb-4 mt-2">
          <input type="checkbox" className="custom-control-input mx-2" id="customCheck1" />
          <label className="custom-control-label" htmlFor="customCheck1">Save credentials.</label>
        </div>

        {formik.errors.submit && (
          <Col sm={12}>
            <Alert variant="danger">{formik.errors.submit}</Alert>
          </Col>
        )}

        <Row>
          <Col mt={2}>
            <Button
              className="btn-block mb-4"
              color="primary"
              size="large"
              type="submit"
              variant="primary"
              disabled={formik.isSubmitting}
            >
              Signin
            </Button>
          </Col>
        </Row>
      </form>
      <hr />
    </>
  );
};

export default JWTLogin;
