# kafka-anomaly-agent
A streaming pipeline that consumes order events from Kafka, detects data-quality/sequence anomalies, stores them in Postgres, then runs a LangGraph multi-agent flow (Gemini) with pgvector RAG to generate incident reports with confidence scores.
