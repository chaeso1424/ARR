// src/hooks/useBaseline.js
import { useEffect, useState } from 'react';

export function useBaseline(initial = 10000) {
  const [baseline, setBaseline] = useState(() => {
    const saved = localStorage.getItem('baseline_usdt');
    return saved ? Number(saved) : initial;
  });

  useEffect(() => {
    localStorage.setItem('baseline_usdt', String(baseline));
  }, [baseline]);

  return [baseline, setBaseline];
}
