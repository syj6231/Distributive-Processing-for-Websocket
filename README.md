# ⚡ Live Stream Local OrderBook System

> **C++ 기반 고성능 In-Memory Orderbook 관리 동기화 엔진 및 차익거래 기회 포착 시스템**

본 프로젝트는 다중 암호화폐 거래소(Bybit, Binance, OKX)의 오더북(BTC, ETH, SOL) 데이터 스트림을 **초저지연(Ultra-Low Latency)**으로 수집·검증 및 병합하여, 차익거래(Arbitrage) 트레이딩 시스템에 즉시 활용 가능한 **단일화된 Global Order Book**을 생성하는 인메모리 엔지니어링 파이프라인입니다.

---

## 📌 Key Highlights

- **Ultra-Low Latency Pipeline**: Mutex 제거를 위한 **Lock-Free 자료구조** 적용 및 컨텍스트 스위칭 최소화를 위한 **CPU Core Pinning**
- **High Availability (A/B Dual Streaming)**: 동일 거래소 스트림을 A/B 이중화 수신하여 패킷 손실 자동 복구 및 SPOF 제거
- **Optimistic Lock-Free Ring Buffer**: Seqlock 기반의 MPSC Wait-Free 슬롯 선점 알고리즘 자체 구현
- **NRT Latch Strategy**: HFT 환경을 고려하여 네트워크 역압(Backpressure) 발생 시 구 데이터를 자동 디스카딩하고 최신 데이터 중심 발행
- **Binary Serialization**: JSON 대비 페이로드 크기 및 디코딩 Overhead를 획기적으로 낮춘 **Protobuf + Batching** 전송

---

## 🏗️ System Architecture

전체 시스템은 **Ingestor(수집/검증)** 계층과 **Aggregator(통합/발행)** 계층으로 명확히 역할이 분리되어 있습니다.
