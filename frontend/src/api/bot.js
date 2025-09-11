import client from './client';
export const getAccountSummary = () => client.get('/api/account/summary');
