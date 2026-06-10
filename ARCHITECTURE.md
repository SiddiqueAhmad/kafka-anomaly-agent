# AI Data Pipeline — Architecture

End-to-end: order events → Kafka → rule-based anomaly detection → Postgres → LangGraph AI agent (Gemini + pgvector RAG) → incident reports.

## Component diagram

```mermaid
flowchart TB
    %% ---------- Producers ----------
    subgraph PROD["Producers (confluent_kafka)"]
        OP["order-producer.py<br/>synthetic order events"]
        P["producer.py<br/>demo events (topic: test)"]
    end

    %% ---------- Streaming ----------
    subgraph KAFKA["Kafka (docker-compose)"]
        ZK["Zookeeper :2181"]
        K[["topic: order"]]
        KT[["topic: test"]]
    end
    ZK -.coordinates.-> K

    %% ---------- Detection ----------
    subgraph DET["Stream Processor — order-consumer.py (kafka-python)"]
        C["KafkaConsumer<br/>group: anomaly-detector-dev<br/>manual offset commit"]
        R{"check_anomalies()<br/>6 rule checks:<br/>missing customer_id · invalid total<br/>missing tracking# · empty items<br/>missing cancel reason · sequence violation"}
    end

    %% ---------- Storage ----------
    subgraph PG["Postgres 16 + pgvector (docker-compose)"]
        T1[("orders_raw")]
        T2[("anomalies<br/>processed flag")]
        T3[("incident_reports<br/>vector(3072)")]
        T4[("llm_usage")]
    end

    %% ---------- AI Agent ----------
    subgraph AGENT["AI Agent — anomaly_agent.py (LangGraph)"]
        N1["supervisor<br/>poll unprocessed (LIMIT 10)<br/>+ max severity"]
        ROUTE{"route_on_anomalies"}
        N2["baseline<br/>historical stats (7d / 24h)"]
        N3["nl_to_sql<br/>sql_agent · gemini-2.5-flash T=0"]
        N4["rag_context<br/>embed + cosine search"]
        N5["report_generator<br/>report_agent · gemini-2.5-flash T=1"]
        N6["alert_dispatcher<br/>write file · mark processed · cost summary"]
    end

    %% ---------- External AI ----------
    subgraph GEMINI["Google Gemini (langchain_google_genai · GOOGLE_API_KEY)"]
        LLM["gemini-2.5-flash<br/>(chat: sql + report)"]
        EMB["gemini-embedding-2-preview<br/>(3072-dim embeddings)"]
    end

    %% ---------- Outputs ----------
    subgraph OUT["Outputs"]
        MD["incidents/*.md"]
        SLACK["Slack webhook (stub)"]
    end

    %% ----- edges: ingestion -----
    OP -->|produce| K
    P -->|produce| KT
    K -->|consume| C
    C -->|every order| T1
    C --> R
    R -->|anomaly rows<br/>processed=FALSE| T2

    %% ----- edges: agent flow -----
    T2 -->|unprocessed| N1
    N1 --> ROUTE
    ROUTE -->|investigate| N2
    ROUTE -->|wait| ENDW(["END"])
    N2 --> N3
    N3 --> N4
    N4 --> N5
    N5 --> N6
    N6 --> ENDD(["END"])

    %% ----- agent <-> stores / models -----
    N2 -.reads.-> T1
    N2 -.reads.-> T2
    N3 <-->|generate SQL| LLM
    N3 -.exec query.-> T1
    N4 -->|embed query| EMB
    N4 -.cosine search.-> T3
    N5 <-->|write report| LLM
    N5 -->|embed report| EMB
    N5 -.insert.-> T3
    N3 -.token usage.-> T4
    N5 -.token usage.-> T4
    N6 --> MD
    N6 -.->|TODO| SLACK
    N6 -.UPDATE processed=TRUE.-> T2

    classDef store fill:#e8f0fe,stroke:#4285f4;
    classDef ext fill:#fef7e0,stroke:#f9ab00;
    class T1,T2,T3,T4 store;
    class LLM,EMB ext;
```

## Flow summary

1. **Produce** — `order-producer.py` publishes order-lifecycle events to Kafka topic `order`.
2. **Detect** — `order-consumer.py` consumes each event, writes the raw record to `orders_raw`, and runs 6 rule-based checks. Violations are written to `anomalies` with `processed = FALSE`. Kafka offsets are committed manually after persistence.
3. **Investigate (LangGraph agent)** — `anomaly_agent.py` runs a 6-node graph:
   - `supervisor` polls up to 10 unprocessed anomalies; a conditional edge routes to `END` if there are none.
   - `baseline` pulls 7-day / 24-hour historical stats for context.
   - `nl_to_sql` uses `gemini-2.5-flash` (T=0) to generate + run a context query.
   - `rag_context` embeds the current anomalies with `gemini-embedding-2-preview` (3072-dim) and cosine-searches `incident_reports` for similar past incidents.
   - `report_generator` uses `gemini-2.5-flash` (T=1) to write a markdown report, then embeds + stores it back into `incident_reports` (the RAG memory loop).
   - `alert_dispatcher` writes the report to `incidents/*.md`, (stub) posts to Slack, marks the anomalies `processed = TRUE`, and prints a token/cost summary.
4. **Token accounting** — SQL and report agents record usage; `llm_usage` table is provisioned for persisting it.

## Infrastructure

- **docker-compose**: Zookeeper, Kafka (`confluentinc/cp-kafka:7.6.0`), Postgres (`pgvector/pgvector:pg16`). `init.sql` seeds all four tables + the `vector` extension on a fresh volume.
- **Secrets**: `GOOGLE_API_KEY` in `.env` (loaded via `load_dotenv()`).
- **k8s/**: Kubernetes manifests (alternate deploy target).
