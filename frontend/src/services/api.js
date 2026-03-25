import axios from 'axios';

const api = axios.create({
    baseURL: import.meta.env.VITE_API_BASE_URL || '/',
    headers: { 'Content-Type': 'application/json' },
    timeout: 60000,
});

export const fetchOrderFlow = (orderId) =>
    api.get(`/orders/${orderId}/flow/`).then((r) => r.data);

export const sendQuery = (question) =>
    api.post('/query/', { question }).then((r) => r.data);

export const sendQueryStream = async (question, onStatus) => {
    const base = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '');
    const url = `${base}/query/stream/`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 60000);
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({ question }),
    }).finally(() => clearTimeout(timeout));
    if (!res.ok || !res.body) {
        throw new Error('Streaming request failed.');
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let finalPayload = null;

    while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const events = buffer.split('\n\n');
        buffer = events.pop() || '';

        for (const block of events) {
            const lines = block.split('\n');
            const eventLine = lines.find((l) => l.startsWith('event:'));
            const dataLine = lines.find((l) => l.startsWith('data:'));
            const eventName = eventLine ? eventLine.replace('event:', '').trim() : '';
            const data = dataLine ? dataLine.replace('data:', '').trim() : '';

            if (eventName === 'status' && typeof onStatus === 'function') {
                onStatus(data);
            }
            if (eventName === 'result' && data) {
                finalPayload = JSON.parse(data);
            }
        }
    }

    return finalPayload || { type: 'error', message: 'No response from stream endpoint.' };
};

export const fetchPipelineSummary = () =>
    api.get('/analytics/pipeline-summary').then((r) => r.data);

export const fetchTopProducts = (limit = 5) =>
    api.get(`/analytics/top-products?limit=${limit}`).then((r) => r.data);

export const fetchOverdueAR = () =>
    api.get('/analytics/overdue-ar').then((r) => r.data);

export default api;
