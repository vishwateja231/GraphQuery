import axios from 'axios';

const api = axios.create({
    baseURL: import.meta.env.VITE_API_BASE_URL || '/',
    headers: { 'Content-Type': 'application/json' },
});

export const fetchOrderFlow = (orderId) =>
    api.get(`/orders/${orderId}/flow`).then((r) => r.data);

export const sendQuery = (question) =>
    api.post('/query/', { question }).then((r) => r.data);

export const fetchQueryPage = async (queryId, page) => {
    try {
        const response = await api.get(`query/${queryId}?page=${page}`);
        return response.data;
    } catch (error) {
        return { error: 'Failed to retrieve additional rows.' };
    }
};

export const fetchPipelineSummary = () =>
    api.get('/analytics/pipeline-summary').then((r) => r.data);

export const fetchTopProducts = (limit = 5) =>
    api.get(`/analytics/top-products?limit=${limit}`).then((r) => r.data);

export const fetchOverdueAR = () =>
    api.get('/analytics/overdue-ar').then((r) => r.data);

export default api;
