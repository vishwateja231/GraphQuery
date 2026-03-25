# Dodge AI – Graph-Based Data Modeling and Query System

**Turn business data into queryable insights with natural language + graph visualization.**

Dodge AI is a full-stack system that accepts natural language questions, uses LLMs to generate SQL against PostgreSQL (Supabase), executes the query on real relational data, and transforms results into an interactive graph in the UI.

---

## Demo

- **Live App:** _Coming soon_ (deploy target: Vercel + Render + Supabase)  
- **Demo Video:** _Add link here_  
- **Screenshots:**
  - Chat interface with natural-language query input
  - Graph canvas showing customer → order → delivery → invoice → payment relationships
  - Tabular result preview synchronized with graph output

---

## Problem Statement

Operational business data (orders, invoices, deliveries, payments, customers, products) is typically stored across multiple normalized tables. This creates two common challenges:

- **Fragmented visibility:** understanding end-to-end flow requires multiple joins and technical SQL knowledge.
- **Slow investigation cycles:** business users cannot quickly ask ad-hoc questions without analyst support.

A graph-based abstraction helps users understand entity relationships and flow progression faster than raw table output alone.

---

## Solution Overview

Dodge AI provides a natural language interface for operational analytics:

1. User asks a question in chat.
2. Groq generates SQL grounded in the live PostgreSQL schema.
3. Backend validates/sanitizes SQL and executes it.
4. Result rows are transformed into graph nodes and edges.
5. UI renders summary + table + interactive graph.

**Pipeline:** `Natural Language → SQL → PostgreSQL → Graph Model → React UI`

---

## Architecture

### Frontend (React)
- Chat-first interface for submitting questions
- Handles `graph`, `empty`, and `error` API response types
- Renders graph using **React Flow** (`@xyflow/react`)
- Displays tabular query result preview alongside graph interactions

### Backend (FastAPI)
- `/query` endpoint for natural-language query processing
- `/query/stream` endpoint for server-sent progressive status + final payload
- LLM orchestration (Groq SQL generation, Gemini summarization with fallback)
- SQL guardrails and schema-aware validation
- Graph builder for dynamic node/edge generation

### Database (PostgreSQL / Supabase)
- Source of truth for business entities
- Connection pooling via `psycopg_pool`
- Indexed for core query paths (orders/customer/order-flow joins)

### LLM Layer (Groq + Gemini)
- **Groq:** SQL generation from schema-grounded prompt
- **Gemini:** business summary generation for larger result sets
- **Fallback:** summary fallback to Groq or deterministic formatter when needed

### End-to-End Flow

`User → FastAPI → Groq (SQL) → SQL Validation → PostgreSQL → Graph Builder → Gemini/Groq Summary → React UI`

---

## Tech Stack

### Frontend
- React
- Vite
- `@xyflow/react` (React Flow)
- Axios
- D3-force

### Backend
- FastAPI
- Uvicorn
- Pydantic
- Psycopg + Psycopg Pool

### Data / AI
- PostgreSQL (Supabase)
- Groq API
- Gemini API

---

## Features

- Natural language querying over business dataset
- Dynamic SQL generation (no hardcoded query templates as primary path)
- Graph visualization from real query output (`nodes[]`, `edges[]`)
- Multi-entity relationship mapping:
  - customer, order, delivery, invoice, payment, product
- Structured API responses:
  - `type: "graph"`
  - `type: "empty"`
  - `type: "error"`
- Guardrails for dataset-only querying
- Timeout handling, fallback behavior, and error-safe responses

---

## Example Queries

- `Show all orders`
- `Trace order flow`
- `Top products`
- `Unpaid invoices`
- `Show customer 320000083 orders`

---

## Database Design

### Core Tables
- `customers`
- `orders`
- `order_items`
- `deliveries`
- `invoices`
- `payments`
- `products`

### Relationship Highlights
- `orders.customer_id -> customers.customer_id`
- `deliveries.order_id -> orders.order_id`
- `invoices.order_id -> orders.order_id`
- `payments.customer_id -> customers.customer_id`
- `order_items.order_id -> orders.order_id`
- `order_items.product_id -> products.product_id`

### Why PostgreSQL over SQLite
- Better concurrency and stability for multi-request API workloads
- Stronger indexing and query planning for relational analytics
- Production-ready connection pooling and deployment ergonomics
- Better fit for Supabase-managed cloud deployment

---

## LLM Strategy

### SQL Generation
- Groq receives:
  - live database schema (tables + columns + foreign keys)
  - strict prompt rules (SELECT/WITH only, explicit aliases, row limits)
- Expected model output format: JSON `{ "sql": "..." }`

### Schema Grounding
- Backend loads schema from PostgreSQL and injects it into the prompt.
- SQL is sanitized and then validated against known schema identifiers.

### Hallucination Prevention
- Reject non-SELECT SQL and dangerous keywords (`DROP`, `DELETE`, etc.)
- Enforce limits (`LIMIT 20` default if missing)
- Validate aliases and identifier consistency
- Return structured error when SQL fails validation

### Summary Fallback Strategy
- Primary summarization: Gemini (for larger result sets)
- Fallback: Groq summary
- Final fallback: deterministic local text formatter

---

## Graph Modeling

### Node Types
- `customer`
- `order`
- `delivery`
- `invoice`
- `payment`
- `product`

### Edge Types (Canonical O2C Flow)
- customer → order (`placed`)
- order → delivery (`fulfilled by`)
- delivery/order → invoice (`billed via`)
- invoice → payment (`paid by`)
- order → product (`contains`)

### Why Graph
Graph representation makes cross-entity dependency and flow-state analysis much easier than reading flat joined rows, especially for traceability questions.

---

## Guardrails

Dodge AI is intentionally scoped to the business dataset domain.

- Non-dataset questions are rejected.
- Standard guardrail response example:
  - `"This system only answers dataset-related queries."`

This reduces irrelevant model behavior and keeps results aligned with assignment scope.

---

## Installation & Setup

## 1) Backend Setup

```bash
cd backend
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

## 2) Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

## 3) Environment Variables

Create `backend/.env`:

```env
DATABASE_URL=postgresql://<user>:<password>@<host>:<port>/<db>
GROQ_API_KEY=<your_groq_key>
GEMINI_API_KEY=<your_gemini_key>
```

Optional frontend env (`frontend/.env`):

```env
VITE_API_BASE_URL=http://localhost:8000
```

---

## Deployment

Recommended split deployment:

- **Backend:** Render
- **Database:** Supabase (PostgreSQL)
- **Frontend:** Vercel

Deployment checklist:
- Set all environment variables in hosting dashboard
- Ensure CORS is configured for frontend domain
- Verify health endpoint and `/query` responses post-deploy

---

## Challenges & Fixes

- **SQLite → PostgreSQL migration:** moved to production-grade connection pooling and schema loading.
- **LLM incorrect columns/aliases:** added schema-grounded prompts + validation/sanitization guardrails.
- **Graph not rendering reliably:** enforced stable node/edge contracts and frontend rerender reset logic.
- **Intermittent network failures:** improved timeout handling and robust API error-state flows.

---

## Future Improvements

- Better graph UX (entity filtering, edge legends, clustering)
- Query result caching with TTL + invalidation strategy
- Explainable SQL mode (show why query was generated)
- Expanded analytics templates and KPI dashboarding
- Role-aware access controls and audit logging

---

## AI Usage

AI tools were used as development accelerators (e.g., Copilot/Cursor-style assistance) for:

- prompt engineering iterations
- debugging assistance
- refactoring support
- documentation polishing

All generated suggestions were manually reviewed and adapted to match runtime behavior and project requirements.

---

## Conclusion

Dodge AI demonstrates a practical architecture for combining **LLM-driven SQL generation** with **graph-based data interpretation** in a production-oriented full-stack application. It improves data accessibility for business users while preserving backend guardrails, structured error handling, and stable visualization flow.
