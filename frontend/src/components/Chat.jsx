import React, { useState, useRef, useEffect } from 'react';
import { sendQuery, sendQueryStream } from '../services/api';
import { Loader2, Send } from 'lucide-react';
import TableView from './TableView';

/** Remove markdown pipe-tables and render **bold** spans */
const renderSummary = (text) => {
    if (!text) return <span>Query completed.</span>;
    // Strip lines that are part of markdown tables (start with | or ---)
    const cleanLines = text
        .split('\n')
        .filter(line => {
            const stripped = line.trim();
            return !(stripped.startsWith('|') || /^[-| ]+$/.test(stripped));
        });
    // Remove trailing empty lines
    while (cleanLines.length && !cleanLines[cleanLines.length - 1].trim()) cleanLines.pop();
    const cleaned = cleanLines.join('\n');
    // Render **bold** spans
    const parts = cleaned.split(/\*\*(.*?)\*\*/g);
    return (
        <span>
            {parts.map((part, i) =>
                i % 2 === 1
                    ? <strong key={i}>{part}</strong>
                    : part
            )}
        </span>
    );
};

const GraphMessage = ({ payload, onEntityDetect }) => {
    const [showTable, setShowTable] = useState(true);

    return (
        <div style={{ width: '100%', display: 'flex', flexDirection: 'column', gap: '10px' }}>
            <div style={{ lineHeight: '1.6' }}>{renderSummary(payload.summary)}</div>

            {showTable && payload.data && payload.data.length > 0 && (
                <TableView data={payload.data} />
            )}

            <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                <button className="modern-btn" onClick={() => setShowTable(!showTable)}>
                    {showTable ? 'Collapse Table' : 'Expand Table'}
                </button>
                <button className="modern-btn primary" onClick={() => onEntityDetect?.(payload.graph)}>
                    Show in Graph
                </button>
            </div>
        </div>
    );
};

export default function Chat({ onEntityDetect }) {
    const [messages, setMessages] = useState([
        {
            id: 'init',
            role: 'ai',
            type: 'text',
            content: 'Hi there. I am Dodge AI. Ask me anything about your orders, customers, or deliveries.',
        },
    ]);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(false);
    const [loadingText, setLoadingText] = useState('Processing...');

    const scrollRef = useRef(null);
    const inputRef = useRef(null);

    const scrollToBottom = () => {
        if (scrollRef.current) {
            scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
        }
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages, loading]);

    const send = async (e) => {
        e?.preventDefault();
        const text = input.trim();
        if (!text || loading) return;

        setInput('');
        if (inputRef.current) inputRef.current.focus();

        const userMsg = { id: Date.now().toString(), role: 'user', type: 'text', content: text };
        setMessages((prev) => [...prev, userMsg]);
        setLoading(true);
        setLoadingText('Fetching data...');

        try {
            let data;
            try {
                data = await sendQueryStream(text, (status) => {
                    if (status) setLoadingText(status);
                });
            } catch {
                data = await sendQuery(text);
            }

            if (data.type === 'graph') {
                setMessages((prev) => [...prev, {
                    id: Date.now().toString(),
                    role: 'ai',
                    type: 'graph',
                    content: data
                }]);
                if (onEntityDetect && data.graph?.nodes?.length > 0) {
                    onEntityDetect(data.graph);
                }
            } else if (data.type === 'empty') {
                setMessages((prev) => [...prev, { id: Date.now().toString(), role: 'ai', type: 'text', content: data.message || 'No data found' }]);
            } else {
                const errorMsg = data.message || data.error || 'Internal error';
                setMessages((prev) => [...prev, { id: Date.now().toString(), role: 'ai', type: 'text', content: errorMsg }]);
            }
        } catch (err) {
            const message = err?.name === 'AbortError'
                ? 'Request timed out. Please try again.'
                : 'Internal connection error. Please try again.';
            setMessages((prev) => [...prev, { id: Date.now().toString(), role: 'ai', type: 'text', content: message }]);
        } finally {
            setLoading(false);
            setLoadingText('Processing...');
            if (inputRef.current) inputRef.current.focus();
        }
    };

    const handleInputKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            send();
        }
    };

    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: 'var(--bg)' }}>
            {/* Messages Scroll Area */}
            <div
                ref={scrollRef}
                style={{
                    flex: 1,
                    overflowY: 'auto',
                    padding: '24px',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '24px'
                }}
            >
                {messages.map((msg) => {
                    const isUser = msg.role === 'user';
                    return (
                        <div key={msg.id} style={{
                            display: 'flex',
                            width: '100%',
                            justifyContent: isUser ? 'flex-end' : 'flex-start'
                        }}>
                            {!isUser && (
                                <div style={{
                                    width: '32px', height: '32px', borderRadius: '8px',
                                    background: 'var(--card)', color: 'var(--text)', display: 'flex',
                                    alignItems: 'center', justifyContent: 'center',
                                    fontSize: '14px', fontWeight: 'bold', marginRight: '12px', flexShrink: 0,
                                    border: '1px solid var(--border)'
                                }}>
                                    D
                                </div>
                            )}

                            <div style={{
                                background: isUser ? 'var(--accent)' : 'var(--card)',
                                color: isUser ? '#ffffff' : 'var(--text)',
                                borderRadius: '12px',
                                padding: '14px 16px',
                                maxWidth: isUser ? '75%' : '85%',
                                fontSize: '14.5px',
                                lineHeight: '1.6',
                                boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                                borderTopRightRadius: isUser ? '4px' : '12px',
                                borderTopLeftRadius: !isUser ? '4px' : '12px',
                                border: !isUser ? '1px solid var(--border)' : 'none'
                            }}>
                                {msg.type === 'text' && <div>{msg.content}</div>}
                                {msg.type === 'graph' && (
                                    <GraphMessage payload={msg.content} onEntityDetect={onEntityDetect} />
                                )}
                            </div>
                        </div>
                    );
                })}

                {loading && (
                    <div style={{ display: 'flex', width: '100%', justifyContent: 'flex-start' }}>
                        <div style={{
                            width: '32px', height: '32px', borderRadius: '8px',
                            background: 'var(--card)', color: 'var(--text)', display: 'flex',
                            alignItems: 'center', justifyContent: 'center',
                            fontSize: '14px', fontWeight: 'bold', marginRight: '12px', flexShrink: 0,
                            border: '1px solid var(--border)'
                        }}>D</div>
                        <div style={{
                            background: 'var(--card)', color: 'var(--text)', borderRadius: '12px', padding: '14px 16px',
                            borderTopLeftRadius: '4px', border: '1px solid var(--border)', display: 'flex', alignItems: 'center', gap: '8px'
                        }}>
                            <Loader2 size={16} className="animate-spin text-slate-400" />
                            <span style={{ color: 'var(--text-muted)', fontSize: '14px' }}>{loadingText}</span>
                        </div>
                    </div>
                )}
            </div>

            {/* Input Fixed at Bottom */}
            <div style={{
                padding: '20px 24px',
                background: 'var(--bg)',
                borderTop: '1px solid var(--border)'
            }}>
                <form
                    onSubmit={send}
                    style={{
                        display: 'flex',
                        background: 'var(--card)',
                        border: '1px solid var(--border)',
                        borderRadius: '24px',
                        padding: '6px 6px 6px 20px',
                        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.2)',
                        alignItems: 'center'
                    }}
                >
                    <textarea
                        ref={inputRef}
                        placeholder="Message Dodge AI..."
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        onKeyDown={handleInputKeyDown}
                        disabled={loading}
                        rows={1}
                        style={{
                            flex: 1,
                            background: 'transparent',
                            border: 'none',
                            color: 'var(--text)',
                            fontSize: '15px',
                            outline: 'none',
                            resize: 'none',
                            minHeight: '24px',
                            maxHeight: '120px',
                            lineHeight: '1.4',
                            paddingTop: '6px',
                            paddingBottom: '6px'
                        }}
                    />
                    <button
                        type="submit"
                        disabled={!input.trim() || loading}
                        style={{
                            width: '36px',
                            height: '36px',
                            borderRadius: '50%',
                            background: (!input.trim() || loading) ? 'var(--border)' : 'var(--accent)',
                            color: '#fff',
                            border: 'none',
                            cursor: (!input.trim() || loading) ? 'not-allowed' : 'pointer',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            transition: 'background 0.2s'
                        }}
                    >
                        <Send size={16} style={{ marginLeft: '-2px' }} />
                    </button>
                </form>
            </div>

            <style>{`
                .modern-btn {
                    background: var(--border);
                    color: var(--text);
                    border: none;
                    padding: 6px 12px;
                    border-radius: 6px;
                    font-size: 13px;
                    font-weight: 500;
                    cursor: pointer;
                    transition: background 0.2s;
                }
                .modern-btn:hover {
                    background: var(--surface-hover);
                }
                .modern-btn.primary {
                    background: var(--accent);
                    color: #ffffff;
                }
                .modern-btn.primary:hover {
                    background: var(--brand); /* use brand for hover brightness */
                }
                
                /* Override scrollbar */
                ::-webkit-scrollbar { width: 8px; height: 8px; }
                ::-webkit-scrollbar-track { background: transparent; }
                ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
                ::-webkit-scrollbar-thumb:hover { background: var(--surface-hover); }

                /* Prevent override on pure white text inside the indigo bubble */
                .text-white { color: #ffffff !important; }
            `}</style>
        </div>
    );
}
