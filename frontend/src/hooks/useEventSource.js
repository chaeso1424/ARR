// src/hooks/useEventSource.js
import { useEffect, useState } from 'react';

export default function useEventSource(url) {
  const [data, setData] = useState(null);
  useEffect(() => {
    const es = new EventSource(url);
    es.onmessage = (e) => setData(JSON.parse(e.data));
    es.onerror = () => {/* 필요시 재연결 로직 */};
    return () => es.close();
  }, [url]);
  return data;
}
