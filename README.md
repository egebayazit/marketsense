# 📈 MarketSense: Financial News Analytics & RAG System

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Qdrant](https://img.shields.io/badge/Qdrant-Vector%20Search-bf0d42?style=for-the-badge)](https://qdrant.tech/)
[![LangChain](https://img.shields.io/badge/LangChain-RAG%20Framework-EF2D5E?style=for-the-badge)](https://langchain.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

> **Turning market noise into signal.** MarketSense bridges the gap between unstructured financial news and real-time market data using advanced Vector Search and RAG architectures.

## 🚀 Overview

**MarketSense** is an advanced analytics engine designed to correlate qualitative data (financial news sentiment) with quantitative market movements (stock prices). Unlike traditional screeners that rely on simple keyword matching, MarketSense uses **Semantic Search (Qdrant)** to understand the *context* of news and an **LLM-based RAG pipeline** to generate actionable insights.

*🚧 Status: Active Development (Ongoing)*

## ⚡ Key Features

* **🤖 RAG Architecture (Retrieval-Augmented Generation):** Implemented a sophisticated pipeline that retrieves relevant financial news chunks to ground LLM responses in real-time facts, reducing hallucinations.
* **🔍 High-Performance Vector Search:** Utilizes **Qdrant** to store and query high-dimensional vector embeddings of news articles, enabling semantic sentiment analysis (e.g., detecting "bearish" sentiment even without negative keywords).
* **📊 Data Correlation Engine:** Automatically correlates news timestamps with real-time stock data (via **yfinance**), allowing users to visualize how specific news events impact market volatility.
* **⚡ Scalable API:** Built on **FastAPI** to serve high-throughput requests for sentiment analysis and news retrieval.

## 🏗️ Architecture

The system operates on a modular pipeline:

1.  **Ingestion Layer:** Aggregates unstructured news data from **NewsAPI** and structured market data from **yfinance**.
2.  **Processing Layer:** Cleans text and generates vector embeddings using OpenAI/HuggingFace models.
3.  **Storage Layer:** Stores embeddings in **Qdrant** for millisecond-level similarity search.
4.  **Intelligence Layer (RAG):** **LangChain** orchestrates the retrieval of context and feeds it to the LLM to answer complex queries like *"How did the latest earnings report affect Tech sector volatility?"*

## 🛠️ Tech Stack

* **Backend:** Python, FastAPI
* **AI & LLM Ops:** LangChain, OpenAI API / Local LLMs
* **Vector Database:** Qdrant
* **Data Sources:** NewsAPI, yfinance
* **Containerization:** Docker

## 🔮 Future Roadmap

* [ ] Integration of multiple news sources for cross-verification.
* [ ] Real-time dashboard with Streamlit.
* [ ] Fine-tuning a smaller LLM (Llama-3) specifically for financial sentiment.

---
*Created by [Ege Bayazit](https://www.linkedin.com/in/egebayazit/)*
