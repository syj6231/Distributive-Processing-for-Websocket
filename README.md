# ⚡ Live Stream Local OrderBook System
> **C++17 기반 초저지연(Ultra-low Latency) In-Memory 오더북 동기화 및 실시간 글로벌 오더북 집계 파이프라인**

[![C++](https://img.shields.io/badge/C%2B%2B-17-00599C?style=flat-square&logo=c%2B%2B&logoColor=white)](https://isocpp.org/)
[![Boost](https://img.shields.io/badge/Boost-Asio%20%7C%20Beast%20%7C%20Lockfree-orange?style=flat-square)](https://www.boost.org/)
[![Protobuf](https://img.shields.io/badge/Protocol%20Buffers-v3-4285F4?style=flat-square&logo=google&logoColor=white)](https://protobuf.dev/)
[![AWS Kinesis](https://img.shields.io/badge/AWS-Kinesis%20Data%20Streams-FF9900?style=flat-square&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/kinesis/)
[![AWS EC2](https://img.shields.io/badge/AWS-EC2%20Native-FF9900?style=flat-square&logo=amazon-ec2&logoColor=white)](https://aws.amazon.com/ec2/)

다중 암호화폐 거래소(**Binance, Bybit, OKX**)에서 비동기적으로 발생하는 실시간 호가(Orderbook) 스트림을 수집·검증·정규화하여, 고빈도 트레이딩(HFT) 및 차익거래(Arbitrage) 의사결정에 즉시 활용 가능한 단일 글로벌 오더북(Global Order Book)을 생성하는 End-to-End 데이터 파이프라인입니다[cite: 1, 4].

---

## 📑 목차
1. [프로젝트 배경 및 해결 과제](#-1-프로젝트-배경-및-해결-과제)
2. [기술 스택 및 선정 근거](#-2-기술-스택-및-선정-근거)
3. [전체 시스템 아키텍처](#-3-전체-시스템-아키텍처)
4. [Ingestor 계층 상세 설계](#-4-ingestor-계층-상세-설계)
5. [Aggregator 계층 상세 설계](#-5-aggregator-계층-상세-설계)
6. [성능 최적화 및 트러블슈팅 (Benchmarks)](#-6-성능-최적화-및-트러블슈팅-benchmarks)
7. [실행 환경 구성 및 재현성 검증](#-7-실행-환경-구성-및-재현성-검증)
8. [향후 고도화 계획](#-8-향후-고도화-계획)

---

## 🎯 1. 프로젝트 배경 및 해결 과제

24시간 분산 운영되는 암호화폐 시장에서는 동일 자산(`BTC/USDT`, `ETH/USDT`, `SOL/USDT`)이라도 거래소별 유동성 차이에 따라 밀리초(ms) 단위의 가격 괴리가 발생합니다. 이를 포착하기 위한 데이터 파이프라인 구축 과정에서 다음 4가지 기술적 난제를 해결하는 데 집중했습니다.

* **초저지연 및 예측 가능한 지연(Predictable Latency):** GC(Garbage Collection) Pause나 Mutex 락 경합으로 인한 Tail Latency 스파이크를 원천 차단
* **네트워크 불완전성 극복 및 선형화(Linearizability):** WebSocket 패킷 손실, 순서 뒤바뀜(Out-of-Order), 네트워크 지터(Jitter) 환경에서도 무결성이 보장된 단일 이벤트 스트림 재구성
* **이기종 프로토콜 및 시퀀스 체계 통합:** 거래소마다 상이한 스냅샷·델타 규칙, 타임스탬프 포맷, 시퀀스 검증 로직을 단일 내부 스키마로 정규화
* **고처리량 병렬 아키텍처:** 3개 거래소 × 3개 심볼(총 9개 독립 스트림)의 버스트(Burst) 트래픽을 백프레셔(Backpressure) 없이 소화하는 파이프라인 분리[cite: 4, 29]

---

## 🛠 2. 기술 스택 및 선정 근거

| 영역 | 기술 스택 | 도입 및 아키텍처 선정 근거 (Trade-off) |
| :--- | :--- | :--- |
| **Core Engine** | `C++17` | 런타임 GC 오버헤드 제거, 캐시 라인 정렬(`alignas(64)`), 원자적 연산(`std::atomic`) 기반 락프리 자료구조 직접 구현 |
| **Network I/O** | `Boost.Asio`, `Boost.Beast`, `OpenSSL` | Thread-per-Connection 기반 동기 블로킹 I/O 모델로 이벤트 루프 오버헤드 제거 및 패킷 도착 즉시 무지연 수신 |
| **Concurrency** | `boost::lockfree::spsc_queue`, Custom MPSC Ring Buffer, `CPU Pinning` | 스레드 간 뮤텍스 락 경합 및 컨텍스트 스위칭 비용 제거, EC2 물리 코어 직접 고정을 통한 L1/L2 캐시 적중률 극대화 |
| **Serialization** | `Protocol Buffers (proto3)`, `nlohmann/json` | 외부 WebSocket 수신 시에만 JSON 파싱을 수행하고, 내부 파이프라인 및 네트워크 전송은 바이너리(Protobuf)로 전환해 페이로드 **50% 절감** |
| **Streaming & Infra** | `AWS Kinesis Data Streams (EFO)`, `AWS EC2` | 거래소명 기반 파티션 키 라우팅으로 샤드 단위 순서 보장, Enhanced Fan-Out(EFO)을 통한 전용 처리량 확보 및 EC2 네이티브 배포로 호스트 OS 레벨 코어 친화도 제어[cite: 1, 12, 28, 50] |

---

## 🏗 3. 전체 시스템 아키텍처

시스템은 원시 데이터 수집·검증을 담당하는 **Ingestor 계층**과, 스트림 병합·스냅샷 발행을 담당하는 **Aggregator 계층**으로 명확히 분리되어 AWS EC2 환경에서 구동됩니다[cite: 5, 6, 27, 50].

![Overall System Architecture](docs/architecture_overall.png)
*(※ 보고서 `[그림-1.1]` 실시간 데이터 스트림 오더북 시스템 전체 아키텍처 이미지 배치)*

```text
[Exchanges: Binance / Bybit / OKX]
       │ (Dual WebSocket Streams: Instance A & B, 200ms Staggered Start)
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ 1. INGESTOR LAYER (EC2 Instance - 거래소별 Multi-Process 완전 격리)    │
│  ├─ Ingestor A/B (I/O-Bound): Sync Blocking Read & Common Delta 변환  │
│  ├─ Lock-Free SPSC Queue (Capacity: 4096, Zero Dynamic Allocation)   │
│  ├─ Validator (CPU-Bound): Best Delta 중재, Gap/Dup 검증 및 순서 재정렬  │
│  └─ Writer (I/O-Bound): Protobuf 직렬화 & Hybrid Batching (50건/100ms) │
└──────────────────────────────────────────────────────────────────────┘
       │ (AWS Kinesis Data Streams - Partition Key: Exchange Name)
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ 2. AGGREGATOR LAYER (EC2 Instance - 9-Thread Pipelined NRT Engine)   │
│  ├─ Reader Threads x3 (I/O-Bound): Kinesis EFO 비동기 스트림 소비       │
│  ├─ Custom MPSC Optimistic Lock-Free Ring Buffer (65,536 Slots)      │
│  ├─ Processor Threads x3 (CPU-Bound, Pinned to Core 3, 4, 5):        │
│  │    └─ 7ms Time Window + 1ms Watermark 기반 글로벌 오더북 병합         │
│  ├─ NRT Latch (Single-Slot Pointer Swap, Backpressure 원천 차단)       │
│  └─ Publisher Threads x3 (I/O-Bound): 최신 글로벌 오더북 스냅샷 발행      │
└──────────────────────────────────────────────────────────────────────┘
