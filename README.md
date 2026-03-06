# Enabling Fairness in Flow Allocation using P4-Programmable Data Planes

> A P4-based implementation of the **Weighted Max-Min Share (WMMS)** fairness algorithm on programmable data planes, evaluated in a Mininet emulation environment using the BMv2 software switch.

---
## First Words : This is just my personal project, it still has some bugs and has not been optimized, and the results are not really reliable, but the algorithm and operation are completely fine, so everyone can run the code with peace of mind. And my code still has many shortcomings, so I welcome and appreciate everyone's suggestions. Thank you all for visiting my project !!!


## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Test Scenarios](#test-scenarios)
- [Results & Visualization](#results--visualization)

---

## Overview

Traditional TCP congestion control does not account for **RTT unfairness** — flows with shorter RTTs tend to grab disproportionately more bandwidth. This project addresses that problem by implementing WMMS fairness directly in the **data plane** using P4.

**Key idea (WMMS, Equation 1):**

$$C_i = \frac{RTT_i}{\sum_j RTT_j} \times C$$

Flows with longer RTTs are assigned a *higher proportional share* of the bottleneck capacity `C`, compensating for their natural disadvantage.

**What makes this system work:**

- The **P4 switch** (`switch_PDP.p4`) uses a **Count-Min Sketch (CMS)** to track per-flow byte counts and estimates RTT from TCP sequence/ACK numbers — entirely in the data plane.
- The **control plane** (`control_plane_PDP.py`) reads flow digests via **P4Runtime (gRPC)**, computes WMMS weights, and installs meter/queue configurations back into the switch.
- **EWMA smoothing** (α = 0.125, standard TCP) is applied to RTT measurements to filter noise and spikes.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 Control Plane                   │
│   (control_plane_PDP.py via P4Runtime / gRPC)   │
│                                                 │
│  • Receives flow digests (RTT, byte count)      │
│  • Classifies flows (short/long via CMS)        │
│  • Computes WMMS weights                        │
│  • Installs meter configs back to switch        │
└───────────────────┬─────────────────────────────┘
                    │ gRPC (P4Runtime)
┌───────────────────▼─────────────────────────────┐
│              P4 Switch (BMv2)                   │
│              (switch_PDP.p4)                    │
│                                                 │
│  • Parses Ethernet / IPv4 / TCP / UDP           │
│  • Count-Min Sketch for flow tracking           │
│  • RTT estimation from seq/ack numbers          │
│  • Sends digest to control plane                │
│  • Enforces per-flow metering                   │
└───────────────────┬─────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────┐
│           Mininet Topology                      │
│           (topology_20.py)                      │
│                                                 │
│   h1 ──┐                                        │
│   h2 ──┼──► [P4 Switch] ──► h4, h5, h6         │
│   h3 ──┘       20 Mbps bottleneck               │
└─────────────────────────────────────────────────┘
```

---

## Project Structure

```
.
├── switch_PDP.p4                  # P4 program for BMv2 (data plane logic)
├── control_plane_PDP.py           # Python control plane using P4Runtime
├── topology_20.py                 # Mininet topology (20 Mbps bottleneck)
├── traffic.py                     # Traffic generator (iperf-based)
├── result_weight.py               # Theoretical WMMS weight visualization
├── visualize_actual_weights.py    # Actual weight allocation visualization
└── README.md
```

| File | Role |
|---|---|
| `switch_PDP.p4` | Data plane: CMS flow tracking, RTT estimation, digest generation |
| `control_plane_PDP.py` | Control plane: WMMS computation, P4Runtime integration, CSV logging |
| `topology_20.py` | Mininet network with 3 senders (h1–h3) and 3 receivers (h4–h6) |
| `traffic.py` | Automated iperf traffic generation for TB1 and TB2 test scenarios |
| `result_weight.py` | Plots theoretical WMMS weight allocation over time |
| `visualize_actual_weights.py` | Plots actual weight allocation and Jain's Fairness Index from CSV logs |

---

## Requirements

### System
- Linux (Ubuntu 20.04+ recommended)
- Python 3.8+
- [Mininet](http://mininet.org/) ≥ 2.3
- [BMv2](https://github.com/p4lang/behavioral-model) (simple_switch_grpc)
- [P4C compiler](https://github.com/p4lang/p4c)

### Python Packages

```bash
pip install grpcio grpcio-tools numpy pandas matplotlib
# Optional (for natural break clustering)
pip install jenkspy
```

### P4Runtime Python Libraries
Follow the setup guide at [p4lang/tutorials](https://github.com/p4lang/tutorials) to install:
- `p4runtime_lib`
- `p4.v1` (protobuf bindings)

---

## Installation

```bash
# Clone the repository
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>

# Compile the P4 program
p4c --target bmv2 --arch v1model --p4runtime-files switch_PDP.p4info.txt switch_PDP.p4

# Install Python dependencies
pip install grpcio grpcio-tools numpy pandas matplotlib jenkspy
```

---

## Usage

### Step 1 — Start the Mininet Topology

```bash
sudo python3 topology_20.py
```

This brings up the network with a 20 Mbps bottleneck link and starts `simple_switch_grpc` with the compiled P4 program.

### Step 2 — Start the Control Plane

Open a new terminal:

```bash
sudo python3 control_plane_PDP.py --test tb1
```

The controller connects to the switch via gRPC, listens for flow digests, computes WMMS weights, and logs results to `logs/control_log_tb1.csv`.

### Step 3 — Generate Traffic

Open another terminal inside the Mininet environment:

```bash
sudo python3 traffic.py --test tb1
```

### Step 4 — Visualize Results

```bash
# Theoretical weights
python3 result_weight.py tb1

# Actual weights from collected data
python3 visualize_actual_weights.py tb1
```

Output PNG files are saved to `results_tb1/`.

---

## Test Scenarios

### TB1 — RTT Unfairness Test (150 seconds)

Three TCP flows competing over a 20 Mbps bottleneck with different RTTs:

| Flow | Source | Destination | RTT | Active Period | WMMS Weight |
|------|--------|-------------|-----|---------------|-------------|
| Flow 1 | h1 | h4 | 10 ms | t = 0–150s | low |
| Flow 2 | h2 | h5 | 50 ms | t = 30–120s | medium |
| Flow 3 | h3 | h6 | 100 ms | t = 60–90s | high |

**Expected behavior:** Flow 3 (highest RTT) receives the largest share when all three compete (≈ 62.5%), compensating for TCP's inherent RTT bias.

```
t=0s    → Flow 1 starts alone
t=30s   → Flow 2 joins  (Flow 1 + Flow 2)
t=60s   → Flow 3 joins  (3-way competition)
t=90s   → Flow 3 leaves
t=120s  → Flow 2 leaves
t=150s  → Flow 1 finishes
```

**WMMS weight breakdown during 3-way competition:**

| Flow | RTT | Share |
|------|-----|-------|
| Flow 1 | 10 ms | 6.25% |
| Flow 2 | 50 ms | 31.25% |
| Flow 3 | 100 ms | 62.50% |

---

### TB2 — UDP vs TCP Fairness Test (150 seconds)

One UDP flow at fixed 10 Mbps competes with two TCP flows:

| Flow | Type | Source | RTT | Active Period |
|------|------|--------|-----|---------------|
| Flow 1 | UDP | h1 | 10 ms | t = 0–150s |
| Flow 2 | TCP | h2 | 50 ms | t = 30–120s |
| Flow 3 | TCP | h3 | 100 ms | t = 60–90s |

---

## Results & Visualization

### Output Files

| File | Description |
|------|-------------|
| `logs/control_log_<test>.csv` | Per-flow RTT, allocated bandwidth, weight, JFI per second |
| `results_<test>/<test>_weights.png` | Theoretical WMMS weight allocation plot |
| `results_<test>/<test>_actual_weights.png` | Actual weight allocation + Jain's Fairness Index |

---

### results_tb1/

Output directory for **TB1 — RTT Unfairness Test**. Generated by running:

```bash
python3 result_weight.py tb1
python3 visualize_actual_weights.py tb1
```

| File | Description |
|------|-------------|
| `tb1_weights.png` | Theoretical WMMS weight allocation over 150s — shows how each flow's share changes as flows join/leave |
| `tb1_actual_weights.png` | Actual weight allocation measured from the switch + JFI curve over time |

**Expected plots:**
- Flow 1 (RTT=10ms) starts at 100%, then drops sharply as Flow 2 and Flow 3 join
- Flow 3 (RTT=100ms) dominates at ~62.5% during 3-way competition (t=60–90s)
- JFI stays close to **1.0** throughout, confirming WMMS fairness is achieved

---

### results_tb2/

Output directory for **TB2 — UDP vs TCP Fairness Test**. Generated by running:

```bash
python3 result_weight.py tb2
python3 visualize_actual_weights.py tb2
```

| File | Description |
|------|-------------|
| `tb2_weights.png` | Theoretical weight allocation showing UDP vs TCP competition |
| `tb2_actual_weights.png` | Actual weight allocation measured from the switch + JFI curve over time |

**Expected plots:**
- UDP Flow 1 (fixed 10 Mbps) competes against adaptive TCP flows
- WMMS assigns higher weights to longer-RTT TCP flows, limiting UDP dominance
- JFI shows how well the system maintains fairness between mixed traffic types

---

### Jain's Fairness Index (JFI)

The system tracks JFI over time to quantify fairness:

$$JFI = \frac{(\sum_i x_i)^2}{n \cdot \sum_i x_i^2}$$

A JFI of **1.0** indicates perfect fairness. The visualization shows JFI over the entire test duration alongside the per-flow weight allocation.

---

## Key Parameters

| Parameter | Value | Description |
|---|---|---|
| `BOTTLENECK_CAPACITY_MBPS` | 20 | Bottleneck link capacity |
| `WINDOW_SIZE` | 1.0 s | Aggregation window for fairness computation |
| `RTT_EWMA_ALPHA` | 0.125 | EWMA smoothing factor (standard TCP) |
| `RTT_SPIKE_THRESHOLD` | 3.0× | Spike rejection multiplier |
| `MIN_RECONFIG_INTERVAL` | 5.0 s | Minimum interval between switch reconfigurations |
| `CMS_WIDTH` | 4096 | Count-Min Sketch width |
| `CMS_DEPTH` | 4 | Count-Min Sketch depth |
| `LONG_FLOW_THRESHOLD` | 10 MB | Byte threshold to classify a flow as long-lived |

---

## References

- [P4 Language Specification](https://p4.org/p4-spec/docs/P4-16-v1.0.0-spec.html)
- [BMv2 Behavioral Model](https://github.com/p4lang/behavioral-model)
- [Jain's Fairness Index](https://www.cs.wustl.edu/~jain/papers/ftp/fairness.pdf)
- Max-Min Fairness and Weighted Variants — Bertsekas & Gallager, *Data Networks*, 1992

---

## License

MIT License — see [LICENSE](LICENSE) for details.
