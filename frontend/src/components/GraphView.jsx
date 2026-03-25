import React, { useState, useEffect, useCallback } from 'react';
import {
    ReactFlow,
    Controls,
    Background,
    useNodesState,
    useEdgesState,
    MarkerType,
    Panel,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Search, Loader2 } from 'lucide-react';
import { fetchOrderFlow } from '../services/api';
import NodeDetailsCard from './NodeDetailsCard';
import * as d3 from 'd3-force';

// Node colors — same hues, visible on both dark and light backgrounds
const NODE_COLORS = {
    customer: '#7c3aed', // purple
    order: '#2563eb', // blue
    delivery: '#06b6d4', // cyan
    invoice: '#f59e0b', // amber
    payment: '#10b981', // emerald
    product: '#ec4899', // pink
};

// Detect current body theme ('dark' | 'light')
function useTheme() {
    const [theme, setTheme] = useState(() => document.body.className || 'dark');
    useEffect(() => {
        const obs = new MutationObserver(() => setTheme(document.body.className || 'dark'));
        obs.observe(document.body, { attributes: true, attributeFilter: ['class'] });
        return () => obs.disconnect();
    }, []);
    return theme;
}

export default function GraphView({ externalOrderQuery, onClearExternal }) {
    const theme = useTheme();
    const isDark = theme === 'dark';
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const [orderQuery, setOrderQuery] = useState('');

    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [selectedNodeData, setSelectedNodeData] = useState(null);
    const [graphVersion, setGraphVersion] = useState(0);
    const [errorTimer, setErrorTimer] = useState(null);

    // Auto-dismiss errors after 4 s
    const showError = useCallback((msg) => {
        setError(msg);
        if (errorTimer) clearTimeout(errorTimer);
        const t = setTimeout(() => setError(null), 4000);
        setErrorTimer(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const applyForceLayout = useCallback((rawNodes, rawEdges) => {
        const simNodes = rawNodes.map(n => ({ ...n }));
        const simEdges = rawEdges.map(e => ({ ...e, source: e.source, target: e.target }));

        const simulation = d3.forceSimulation(simNodes)
            .force('charge', d3.forceManyBody().strength(-3000))
            .force('link', d3.forceLink(simEdges).id(d => d.id).distance(250))
            .force('center', d3.forceCenter(0, 0))
            .force('collide', d3.forceCollide().radius(100))
            .stop();

        simulation.tick(300);

        const positionedNodes = rawNodes.map((n, i) => ({
            ...n,
            position: { x: simNodes[i].x, y: simNodes[i].y }
        }));

        setNodes(positionedNodes);
        setEdges(rawEdges);
    }, [setNodes, setEdges]);

    const loadFlow = useCallback(async (queryParam, bulkQueries = null) => {
        const idsToFetch = bulkQueries || [queryParam || orderQuery].filter(Boolean);
        if (idsToFetch.length === 0) return;

        setLoading(true);
        setError(null);
        setSelectedNodeData(null);

        try {
            const results = await Promise.all(idsToFetch.map(id => fetchOrderFlow(id.toString().trim())));

            const validResults = results.filter(r => !r.error && r.length > 0);
            if (validResults.length === 0) {
                throw new Error('No data found for the provided IDs.');
            }

            const uniqueNodes = new Map();
            const uniqueEdges = new Map();

            const addNode = (id, type, label, metadata, overrideColor = null) => {
                if (!uniqueNodes.has(id)) {
                    const bg = overrideColor || NODE_COLORS[type];
                    uniqueNodes.set(id, {
                        id,
                        type: 'default',
                        data: { label },
                        metadata: { ...metadata, type },
                        style: {
                            background: bg,
                            color: '#ffffff',
                            border: isDark ? 'none' : '1px solid rgba(0,0,0,0.08)',
                            borderRadius: '12px',
                            padding: '16px 20px',
                            fontSize: '14px',
                            fontWeight: '600',
                            boxShadow: isDark
                                ? '0 8px 24px rgba(0,0,0,0.5)'
                                : '0 4px 12px rgba(0,0,0,0.12)',
                            width: 180,
                            textAlign: 'center'
                        }
                    });
                }
            };

            const addEdge = (source, target, label) => {
                const id = `e-${source}-${target}`;
                if (!uniqueEdges.has(id)) {
                    uniqueEdges.set(id, {
                        id,
                        source,
                        target,
                        label,
                        type: 'smoothstep',
                        animated: true,
                        style: {
                            stroke: isDark ? 'rgba(255,255,255,0.2)' : '#000000',
                            strokeWidth: 2
                        },
                        labelStyle: {
                            fill: isDark ? '#a1a1aa' : '#000000',
                            fontSize: 12,
                            fontWeight: isDark ? '400' : '600',
                        },
                        markerEnd: {
                            type: MarkerType.ArrowClosed,
                            color: isDark ? 'rgba(255,255,255,0.4)' : '#000000'
                        }
                    });
                }
            };

            validResults.forEach(data => {
                const row = data[0];

                // 1. Customer
                if (row.customer_name) {
                    addNode(`cust-${row.customer_id}`, 'customer', row.customer_name, { name: row.customer_name });
                }

                // 2. Order
                addNode(`ord-${row.order_id}`, 'order', `Order #${row.order_id}`, {
                    id: row.order_id,
                    amount: `${row.order_amount} ₹`,
                    status: row.delivery_status
                });
                if (row.customer_name) addEdge(`cust-${row.customer_id}`, `ord-${row.order_id}`, 'placed');

                // 3. Delivery
                if (row.delivery_id) {
                    let label = `Delivery #${row.delivery_id}`;
                    if (row.goods_status === 'C') label += ' ✅';
                    addNode(`del-${row.delivery_id}`, 'delivery', label, {
                        id: row.delivery_id,
                        ship_date: row.ship_date,
                        status: row.goods_status
                    });
                    addEdge(`ord-${row.order_id}`, `del-${row.delivery_id}`, 'fulfilled by');
                }

                // 4. Invoice
                if (row.invoice_id) {
                    const label = `Invoice #${row.invoice_id}`;
                    addNode(`inv-${row.invoice_id}`, 'invoice', label, {
                        id: row.invoice_id,
                        amount: `${row.invoice_amount} ₹`,
                        date: row.invoice_date,
                        cancelled: row.is_cancelled === 1 ? 'Yes' : 'No'
                    });
                    if (row.delivery_id) addEdge(`del-${row.delivery_id}`, `inv-${row.invoice_id}`, 'billed via');
                    else addEdge(`ord-${row.order_id}`, `inv-${row.invoice_id}`, 'billed via');
                }

                // 5. Payment UI Status Logic
                if (row.invoice_id) {
                    const payId = `pay-${row.invoice_id}`;
                    if (row.payment_id) {
                        addNode(payId, 'payment', `Paid ✅`, {
                            id: row.payment_id,
                            amount: `${row.payment_amount} ₹`,
                            date: row.clearing_date,
                            status: 'Paid'
                        }, '#10b981');
                    } else if (row.is_cancelled === 1) {
                        addNode(payId, 'payment', 'Cancelled ⚠️', { status: 'Invoice Cancelled' }, '#6b7280');
                    } else {
                        addNode(payId, 'payment', 'Unpaid ❌', { status: 'Unpaid / Pending' }, '#ef4444');
                    }
                    addEdge(`inv-${row.invoice_id}`, payId, row.payment_id ? 'payment received' : (row.is_cancelled ? 'halted' : 'awaiting payment'));
                }
            });

            setNodes([]);
            setEdges([]);
            applyForceLayout(Array.from(uniqueNodes.values()), Array.from(uniqueEdges.values()));
            setGraphVersion((v) => v + 1);

        } catch (err) {
            showError(err.message || 'Failed to fetch graph data.');
            setNodes([]);
            setEdges([]);
        } finally {
            setLoading(false);
        }
    }, [orderQuery, applyForceLayout, isDark, showError]);

    useEffect(() => {
        if (externalOrderQuery) {
            if (externalOrderQuery.nodes && externalOrderQuery.edges) {
                setLoading(true);
                const styledNodes = externalOrderQuery.nodes.map((n) => {
                    const nodeType = (n.type || n.data?.type || 'order').toLowerCase();
                    const bg = NODE_COLORS[nodeType] || '#888';
                    return {
                        ...n,
                        data: {
                            ...(n.data || {}),
                            label: n.data?.label || n.id,
                            type: nodeType,
                        },
                        style: {
                            background: bg,
                            color: '#ffffff',
                            border: isDark ? 'none' : '1px solid rgba(0,0,0,0.08)',
                            borderRadius: '12px',
                            padding: '16px 20px',
                            fontSize: '14px',
                            fontWeight: '600',
                            boxShadow: isDark ? '0 8px 24px rgba(0,0,0,0.5)' : '0 4px 12px rgba(0,0,0,0.12)',
                            width: 180,
                            textAlign: 'center'
                        }
                    };
                });
                const styledEdges = externalOrderQuery.edges.map(e => ({
                    ...e,
                    type: 'smoothstep',
                    animated: true,
                    style: {
                        stroke: isDark ? 'rgba(255,255,255,0.2)' : '#000000',
                        strokeWidth: 2
                    },
                    labelStyle: {
                        fill: isDark ? '#a1a1aa' : '#000000',
                        fontSize: 12,
                        fontWeight: isDark ? '400' : '600'
                    },
                    markerEnd: {
                        type: MarkerType.ArrowClosed,
                        color: isDark ? 'rgba(255,255,255,0.4)' : '#000000'
                    }
                }));
                setNodes([]);
                setEdges([]);
                applyForceLayout(styledNodes, styledEdges);
                setGraphVersion((v) => v + 1);
                setLoading(false);
                if (onClearExternal) onClearExternal();
            } else if (Array.isArray(externalOrderQuery)) {
                setOrderQuery(externalOrderQuery.join(", "));
                loadFlow(null, externalOrderQuery);
                if (onClearExternal) onClearExternal();
            } else {
                setOrderQuery(externalOrderQuery);
                loadFlow(externalOrderQuery);
                if (onClearExternal) onClearExternal();
            }
        }
    }, [externalOrderQuery, loadFlow, onClearExternal, applyForceLayout, isDark]);

    // Force re-render of existing nodes/edges when theme flips
    useEffect(() => {
        setNodes(nds => nds.map(n => ({
            ...n,
            style: {
                ...n.style,
                border: isDark ? 'none' : '1px solid rgba(0,0,0,0.08)',
                boxShadow: isDark ? '0 8px 24px rgba(0,0,0,0.5)' : '0 4px 12px rgba(0,0,0,0.12)'
            }
        })));
        setEdges(eds => eds.map(e => ({
            ...e,
            style: {
                ...e.style,
                stroke: isDark ? 'rgba(255,255,255,0.2)' : '#000000'
            },
            labelStyle: {
                ...e.labelStyle,
                fill: isDark ? '#a1a1aa' : '#000000',
                fontWeight: isDark ? '400' : '600'
            },
            markerEnd: {
                ...e.markerEnd,
                color: isDark ? 'rgba(255,255,255,0.4)' : '#000000'
            }
        })));
    }, [isDark, setNodes, setEdges]);

    const onNodeClick = (e, node) => {
        setSelectedNodeData({
            id: node.id,
            type: node.data?.type || node.type,
            metadata: node.data || {},
        });
    };

    if (!nodes.length && !loading && !error) {
        return (
            <div className="graph-container" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flexDirection: 'column', gap: 16 }}>
                <div className="graph-overlay-top">
                    <form className="search-box" onSubmit={(e) => { e.preventDefault(); loadFlow(); }}>
                        <Search size={16} color="var(--text-muted)" style={{ marginRight: 8 }} />
                        <input
                            type="text"
                            placeholder="Search Order ID... (e.g. 740506)"
                            value={orderQuery}
                            onChange={e => setOrderQuery(e.target.value)}
                        />
                    </form>
                </div>
                {error && <div className="error-toast">{error}</div>}
                {!error && (
                    <div style={{ color: 'var(--text-muted)', fontSize: 16, maxWidth: 300, textAlign: 'center' }}>
                        Ask a question or enter an order ID to explore your data.
                    </div>
                )}
            </div>
        );
    }

    return (
        <div className="graph-container">
            {loading && (
                <div className="graph-loading">
                    <Loader2 size={24} className="shimmer" /> Loading graph...
                </div>
            )}

            {error && <div className="error-toast">{error}</div>}

            <div className="graph-overlay-top">
                <form className="search-box" onSubmit={(e) => { e.preventDefault(); loadFlow(); }}>
                    <Search size={16} color="var(--text-muted)" style={{ marginRight: 8 }} />
                    <input
                        type="text"
                        placeholder="Search IDs..."
                        value={orderQuery}
                        onChange={e => setOrderQuery(e.target.value)}
                    />
                </form>
            </div>

            <ReactFlow
                key={graphVersion}
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onNodeClick={onNodeClick}
                proOptions={{ hideAttribution: true }}
                fitView
                fitViewOptions={{ padding: 0.5, maxZoom: 1.5 }}
            >
                <Background
                    gap={24}
                    size={2}
                    color={isDark ? 'rgba(255,255,255,0.05)' : '#e5e7eb'}
                />
                <Controls
                    position="bottom-left"
                    style={{
                        display: 'flex',
                        flexDirection: 'column',
                        background: 'var(--bg-panel)',
                        border: '1px solid var(--border-color)',
                        borderRadius: '8px',
                        overflow: 'hidden'
                    }}
                    className="custom-react-flow-controls"
                />

                <Panel position="bottom-center" style={{ display: 'flex', gap: '16px', background: 'var(--card)', padding: '10px 20px', borderRadius: '24px', backdropFilter: 'blur(8px)', border: '1px solid var(--border)' }}>
                    {Object.entries(NODE_COLORS).map(([label, color]) => (
                        <div key={label} style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: 'var(--text-muted)' }}>
                            <span style={{ width: 10, height: 10, borderRadius: '50%', background: color }} />
                            {label}
                        </div>
                    ))}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: 'var(--text-muted)', borderLeft: '1px solid var(--border)', paddingLeft: 16 }}>
                        <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#ef4444' }} />
                        Unpaid
                    </div>
                </Panel>
            </ReactFlow>

            {selectedNodeData && (
                <NodeDetailsCard
                    nodeData={selectedNodeData}
                    onClose={() => setSelectedNodeData(null)}
                />
            )}
        </div>
    );
}
