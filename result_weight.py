#!/usr/bin/env python3
"""
TB1 WMMS Weight Allocation Visualization

Vẽ weight allocation của từng flow theo timeline TB1:
- Flow 1 (RTT=10ms):  t=0-150s
- Flow 2 (RTT=50ms):  t=30-120s  
- Flow 3 (RTT=100ms): t=60-90s

Không phụ thuộc vào RTT đo được, chỉ dựa trên theoretical WMMS.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os

plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['font.size'] = 11

# Màu sắc cho từng flow
FLOW_COLORS = {
    'Flow 1': '#1f77b4',  # Blue
    'Flow 2': '#ff7f0e',  # Orange  
    'Flow 3': '#2ca02c',  # Green
}


def compute_wmms_weights(rtts):
    """
    Paper Equation 1: Ci = (rttw_i / Σrttw_j) × C
    where rttw_i = RTT_i (higher RTT → higher weight)
    """
    total_rtt = sum(rtts)  # ✅ ĐÚNG - dùng RTT trực tiếp
    weights = [(rtt / total_rtt) * 100.0 for rtt in rtts]
    return weights


def generate_tb1_timeline():
    """
    Tạo timeline cho TB1 với weight của từng flow tại mỗi giây
    
    Returns:
        times: List of time points
        flow_weights: Dict {flow_name: [weights]}
    """
    # Cấu hình flows theo traffic.py
    flows_config = {
        'Flow 1': {'rtt': 10, 'start': 0, 'end': 150},
        'Flow 2': {'rtt': 50, 'start': 30, 'end': 120},
        'Flow 3': {'rtt': 100, 'start': 60, 'end': 90},
    }
    
    times = list(range(0, 151))  # 0 đến 150 giây
    flow_weights = {'Flow 1': [], 'Flow 2': [], 'Flow 3': []}
    
    for t in times:
        # Xác định flows đang active
        active_flows = []
        active_rtts = []
        
        for flow_name, config in flows_config.items():
            if config['start'] <= t <= config['end']:
                active_flows.append(flow_name)
                active_rtts.append(config['rtt'])
        
        # Tính weights cho các flows active
        if active_flows:
            weights = compute_wmms_weights(active_rtts)
            weight_map = dict(zip(active_flows, weights))
        else:
            weight_map = {}
        
        # Gán weight cho mỗi flow (0 nếu không active)
        for flow_name in ['Flow 1', 'Flow 2', 'Flow 3']:
            flow_weights[flow_name].append(weight_map.get(flow_name, 0.0))
    
    return times, flow_weights

def plot_tb1_weights(test_name, output_dir, csv_file=None):
    """
    Vẽ weight allocation cho TB1
    """
    fig, ax_weight = plt.subplots(1, 1, figsize=(15, 8))

    # ============ WEIGHT ALLOCATION PLOT ============
    times, flow_weights = generate_tb1_timeline()
    
    print(f"\n{'='*70}")
    print(f"📊 WEIGHT ALLOCATION")
    print(f"{'='*70}\n")
    
    # Vẽ từng flow
    for flow_name in ['Flow 1', 'Flow 2', 'Flow 3']:
        weights = flow_weights[flow_name]
        color = FLOW_COLORS[flow_name]
        
        # Tính RTT và avg weight khi active
        if flow_name == 'Flow 1':
            rtt, start, end = 10, 0, 150
        elif flow_name == 'Flow 2':
            rtt, start, end = 50, 30, 120
        else:
            rtt, start, end = 100, 60, 90
        
        active_weights = [w for w in weights if w > 0]
        avg_weight = np.mean(active_weights) if active_weights else 0
        
        # Vẽ line
        ax_weight.plot(times, weights,
                      label=f'{flow_name} (RTT={rtt}ms, avg={avg_weight:.1f}%)',
                      color=color, linewidth=3.0, alpha=0.9)
        
        print(f"{flow_name}: RTT={rtt:3d}ms | Active: t={start:3d}-{end:3d}s | Avg Weight: {avg_weight:5.1f}%")
    
    # Timeline annotations
    ax_weight.axvline(x=0, color='gray', linestyle=':', linewidth=1.5, alpha=0.5)
    ax_weight.text(2, 102, 'Flow 1 starts', fontsize=9, color='gray')
    
    ax_weight.axvline(x=30, color='gray', linestyle=':', linewidth=1.5, alpha=0.5)
    ax_weight.text(32, 102, 'Flow 2 starts', fontsize=9, color='gray')
    
    ax_weight.axvline(x=60, color='gray', linestyle=':', linewidth=1.5, alpha=0.5)
    ax_weight.text(62, 102, 'Flow 3 starts', fontsize=9, color='gray')
    
    ax_weight.axvline(x=90, color='gray', linestyle=':', linewidth=1.5, alpha=0.5)
    ax_weight.text(92, 102, 'Flow 3 ends', fontsize=9, color='gray')
    
    ax_weight.axvline(x=120, color='gray', linestyle=':', linewidth=1.5, alpha=0.5)
    ax_weight.text(122, 102, 'Flow 2 ends', fontsize=9, color='gray')
    
    # Formatting
    ax_weight.set_ylabel('Allocated Weight [%]', fontsize=13, fontweight='bold')
    ax_weight.set_ylim(-2, 108)
    ax_weight.set_xlim(0, 150)
    ax_weight.grid(True, alpha=0.3, linestyle='--')
    ax_weight.legend(loc='upper right', fontsize=11, framealpha=0.95)
    ax_weight.set_title(f'WMMS Weight Allocation (Theoretical)', 
                       fontsize=14, fontweight='bold', pad=15)
    

    plt.tight_layout()
    
    # Save
    output_file = os.path.join(output_dir, f'{test_name}_weights.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✅ Saved: {output_file}")
    plt.close()
    
    print(f"{'='*70}\n")


def print_theoretical_summary():
    """In tóm tắt  weights"""
    print(f"\n{'='*70}")
    print(f"📐  WMMS WEIGHTS SUMMARY")
    print(f"{'='*70}\n")
    
    scenarios = [
        ("Flow 1 alone (t=0-30s, t=120-150s)", [10], ['Flow 1']),
        ("Flow 1+2 (t=30-60s, t=90-120s)", [10, 50], ['Flow 1', 'Flow 2']),
        ("Flow 1+2+3 (t=60-90s)", [10, 50, 100], ['Flow 1', 'Flow 2', 'Flow 3']),
    ]
    
    for desc, rtts, flows in scenarios:
        weights = compute_wmms_weights(rtts)
        print(f"{desc}:")
        for flow, weight in zip(flows, weights):
            print(f"  {flow}: {weight:5.1f}%")
        print()
    
    print(f"{'='*70}\n")


def main():
    if len(sys.argv) < 2:
        print("\nUsage: python3 visualize_weights_fixed.py <test_name>")
        print("Example: python3 visualize_weights_fixed.py tb1\n")
        sys.exit(1)
    
    test_name = sys.argv[1].lower()
    csv_file = f"logs/control_log_{test_name}.csv"
    
    if not os.path.exists(csv_file):
        print(f"\n⚠️  Warning: CSV file not found: {csv_file}")
        print(f"   Will generate plot without JFI data.\n")
        csv_file = None
    
    print(f"\n{'='*80}")
    print(f"📊 TB1 WEIGHT ALLOCATION VISUALIZATION")
    print(f"{'='*80}\n")
    
    # Print summary
    print_theoretical_summary()
    
    # Create output directory
    output_dir = f"results_{test_name}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate plot
    plot_tb1_weights(test_name, output_dir, csv_file)
    
    print(f"\n{'='*80}")
    print(f"✅ Visualization complete!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()