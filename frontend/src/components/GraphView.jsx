import React, { useEffect, useState, useCallback } from 'react';
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
import { Loader2 } from 'lucide-react';
import NodeDetailsCard from './NodeDetailsCard';
import * as d3 from 'd3-force';

const NODE_COLORS = {
    customer: '#2563eb',
    order: '#16a34a',
    delivery: '#f97316',
    invoice: '#7c3aed',
    payment: '#dc2626',
    product: '#0d9488',
    record: '#64748b',
};

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
    const [loading, setLoading] = useState(false);
    const [selectedNodeData, setSelectedNodeData] = useState(null);
    const [graphVersion, setGraphVersion] = useState(0);

    const applyForceLayout = useCallback((rawNodes, rawEdges) => {
        if (!rawNodes.length) {
            setNodes([]);
            setEdges([]);
            return;
        }
        const simNodes = rawNodes.map((n) => ({ ...n }));
        const simEdges = rawEdges.map((e) => ({ ...e, source: e.source, target: e.target }));
        const simulation = d3.forceSimulation(simNodes)
            .force('charge', d3.forceManyBody().strength(-1700))
            .force('link', d3.forceLink(simEdges).id((d) => d.id).distance(220))
            .force('center', d3.forceCenter(0, 0))
            .force('collide', d3.forceCollide().radius(90))
            .stop();
        simulation.tick(250);
        setNodes(rawNodes.map((n, i) => ({ ...n, position: { x: simNodes[i].x, y: simNodes[i].y } })));
        setEdges(rawEdges);
    }, [setEdges, setNodes]);

    const styleGraph = useCallback((graph) => {
        const rawNodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
        const rawEdges = Array.isArray(graph?.edges) ? graph.edges : [];
        const styledNodes = rawNodes.map((n) => {
            const nodeType = String(n.type || n.data?.type || 'record').toLowerCase();
            return {
                ...n,
                type: 'default',
                data: {
                    ...(n.data || {}),
                    type: nodeType,
                    label: n.data?.label || n.id,
                },
                style: {
                    background: NODE_COLORS[nodeType] || '#64748b',
                    color: '#ffffff',
                    border: isDark ? 'none' : '1px solid rgba(0,0,0,0.08)',
                    borderRadius: '12px',
                    padding: '16px 20px',
                    fontSize: '14px',
                    fontWeight: '600',
                    boxShadow: isDark ? '0 8px 24px rgba(0,0,0,0.5)' : '0 4px 12px rgba(0,0,0,0.12)',
                    width: 190,
                    textAlign: 'center',
                },
            };
        });
        const styledEdges = rawEdges.map((e) => ({
            ...e,
            type: 'smoothstep',
            animated: true,
            style: {
                stroke: isDark ? 'rgba(255,255,255,0.25)' : '#111827',
                strokeWidth: 2,
            },
            labelStyle: {
                fill: isDark ? '#d1d5db' : '#111827',
                fontSize: 12,
                fontWeight: '600',
            },
            markerEnd: {
                type: MarkerType.ArrowClosed,
                color: isDark ? 'rgba(255,255,255,0.4)' : '#111827',
            },
        }));
        return { styledNodes, styledEdges };
    }, [isDark]);

    useEffect(() => {
        if (!externalOrderQuery) {
            return;
        }
        setLoading(true);
        setSelectedNodeData(null);
        setNodes([]);
        setEdges([]);
        const { styledNodes, styledEdges } = styleGraph(externalOrderQuery);
        applyForceLayout(styledNodes, styledEdges);
        setGraphVersion((v) => v + 1);
        setLoading(false);
        onClearExternal?.();
    }, [externalOrderQuery, onClearExternal, applyForceLayout, setEdges, setNodes, styleGraph]);

    useEffect(() => {
        setNodes((nds) => nds.map((n) => ({
            ...n,
            style: {
                ...n.style,
                border: isDark ? 'none' : '1px solid rgba(0,0,0,0.08)',
                boxShadow: isDark ? '0 8px 24px rgba(0,0,0,0.5)' : '0 4px 12px rgba(0,0,0,0.12)',
            },
        })));
        setEdges((eds) => eds.map((e) => ({
            ...e,
            style: { ...e.style, stroke: isDark ? 'rgba(255,255,255,0.25)' : '#111827' },
            labelStyle: { ...e.labelStyle, fill: isDark ? '#d1d5db' : '#111827' },
            markerEnd: { ...e.markerEnd, color: isDark ? 'rgba(255,255,255,0.4)' : '#111827' },
        })));
    }, [isDark, setEdges, setNodes]);

    const onNodeClick = (_, node) => {
        setSelectedNodeData({
            id: node.id,
            type: node.data?.type || node.type,
            metadata: node.data || {},
        });
    };

    if (!nodes.length && !loading) {
        return (
            <div className="graph-container" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <div style={{ color: 'var(--text-muted)', fontSize: 16, maxWidth: 300, textAlign: 'center' }}>
                    Ask a query to generate a graph from result rows.
                </div>
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
                <Background gap={24} size={2} color={isDark ? 'rgba(255,255,255,0.05)' : '#e5e7eb'} />
                <Controls
                    position="bottom-left"
                    style={{ background: 'var(--bg-panel)', border: '1px solid var(--border-color)', borderRadius: '8px' }}
                    className="custom-react-flow-controls"
                />
                <Panel position="bottom-center" style={{ display: 'flex', gap: 16, background: 'var(--card)', padding: '10px 20px', borderRadius: 24, border: '1px solid var(--border)' }}>
                    {Object.entries(NODE_COLORS).map(([label, color]) => (
                        <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: 'var(--text-muted)' }}>
                            <span style={{ width: 10, height: 10, borderRadius: '50%', background: color }} />
                            {label}
                        </div>
                    ))}
                </Panel>
            </ReactFlow>
            {selectedNodeData && (
                <NodeDetailsCard nodeData={selectedNodeData} onClose={() => setSelectedNodeData(null)} />
            )}
        </div>
    );
}
