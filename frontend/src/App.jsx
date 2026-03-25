import React, { useState, useEffect, useRef } from 'react';
import GraphView from './components/GraphView';
import Chat from './components/Chat';
import { Moon, Sun } from 'lucide-react';
import './App.css';

export default function App() {
  const MIN_PANE_WIDTH = 300;
  const DEFAULT_SPLIT_PERCENT = 65;
  const [graphHighlight, setGraphHighlight] = useState(null);
  const [leftPaneWidth, setLeftPaneWidth] = useState(() => {
    const saved = sessionStorage.getItem('split-left-width');
    return saved ? Number(saved) : null;
  });
  const [isDragging, setIsDragging] = useState(false);
  const appBodyRef = useRef(null);

  // Initialize theme from localStorage or default to 'dark'
  const [theme, setTheme] = useState(() => localStorage.getItem('theme') || 'dark');

  useEffect(() => {
    // Apply theme strictly to body element
    document.body.className = theme;
    localStorage.setItem('theme', theme);
  }, [theme]);

  const toggleTheme = () => {
    setTheme(prev => prev === 'dark' ? 'light' : 'dark');
  };

  const handleEntityDetect = (entities) => {
    if (entities?.nodes && entities?.edges) {
      setGraphHighlight(entities);
    } else if (Array.isArray(entities)) {
      setGraphHighlight(entities);
    } else if (entities && entities.order_id) {
      setGraphHighlight([entities.order_id]);
    }
  };

  useEffect(() => {
    if (typeof leftPaneWidth === 'number') {
      sessionStorage.setItem('split-left-width', String(leftPaneWidth));
    }
  }, [leftPaneWidth]);

  useEffect(() => {
    if (typeof leftPaneWidth === 'number') return;
    const bodyEl = appBodyRef.current;
    if (!bodyEl) return;
    const defaultWidth = Math.round((bodyEl.clientWidth * DEFAULT_SPLIT_PERCENT) / 100);
    setLeftPaneWidth(defaultWidth);
  }, [leftPaneWidth]);

  useEffect(() => {
    if (!isDragging) return undefined;

    const onMouseMove = (e) => {
      const bodyEl = appBodyRef.current;
      if (!bodyEl) return;
      const rect = bodyEl.getBoundingClientRect();
      const minLeft = MIN_PANE_WIDTH;
      const maxLeft = rect.width - MIN_PANE_WIDTH;
      const next = Math.min(Math.max(e.clientX - rect.left, minLeft), maxLeft);
      setLeftPaneWidth(next);
    };

    const onMouseUp = () => {
      setIsDragging(false);
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
    document.body.classList.add('is-resizing');

    return () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.classList.remove('is-resizing');
    };
  }, [isDragging]);

  return (
    <div className="app">
      <header className="app-header">
        <div className="brand">
          <div className="brand-icon">D</div>
          <div>
            <div className="brand-text">Dodge AI</div>
            <div className="brand-tagline">Navigate your data. Instantly.</div>
          </div>
        </div>

        {/* Theme Toggle Button */}
        <button
          onClick={toggleTheme}
          style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
            color: 'var(--text)',
            borderRadius: '50%',
            width: '40px',
            height: '40px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            cursor: 'pointer',
            transition: 'all 0.2s ease'
          }}
          title={`Switch to ${theme === 'dark' ? 'Light' : 'Dark'} Mode`}
        >
          {theme === 'dark' ? <Sun size={20} /> : <Moon size={20} />}
        </button>
      </header>

      <main className="app-body" ref={appBodyRef}>
        <div
          className="pane pane-graph"
          style={leftPaneWidth ? { width: `${leftPaneWidth}px`, flex: '0 0 auto' } : undefined}
        >
          <GraphView externalOrderQuery={graphHighlight} onClearExternal={() => setGraphHighlight(null)} />
        </div>
        <div
          className="splitter"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize panels"
          onMouseDown={() => setIsDragging(true)}
        />
        <div className="pane pane-chat">
          <Chat onEntityDetect={handleEntityDetect} />
        </div>
      </main>
    </div>
  );
}
