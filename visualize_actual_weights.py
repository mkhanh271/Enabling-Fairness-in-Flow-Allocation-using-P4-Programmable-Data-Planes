#!/usr/bin/env python3
"""
TB1 ACTUAL WMMS Weight Allocation Visualization

Đọc CSV thực tế và vẽ:
1. Weight allocation thực tế từ WMMS (từ RTT đo được)
2. JFI theo thời gian

Format giống hệt với theoretical để dễ so sánh.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os

plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['font.size'] = 11

# Màu sắc cho từng flow (GIỐNG THEORETICAL)
FLOW_COLORS = {
    '10.0.0.1': '#1f77b4',  # Blue - Flow 1
    '10.0.0.2': '#ff7f0e',  # Orange - Flow 2
    '10.0.0.3': '#2ca02c',  # Green - Flow 3
}

FLOW_LABELS = {
    '10.0.0.1': 'Flow 1',
    '10.0.0.2': 'Flow 2',
    '10.0.0.3': 'Flow 3',
}


def load_csv_data(csv_file):
    """Load and process CSV data"""
    df = pd.read_csv(csv_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Calculate elapsed time
    start_time = df['timestamp'].min()
    df['elapsed_sec'] = (df['timestamp'] - start_time).dt.total_seconds()
    
    return df


def compute_wmms_weights_from_rtts(rtts_dict):
    """
    Compute WMMS weights from RTTs
    Paper Equation 1: weight_i = RTT_i / Σ RTT_j
    
    Args:
        rtts_dict: {src_ip: rtt_ms}
    Returns:
        {src_ip: weight_percent}
    """
    if not rtts_dict:
        return {}
    
    # Filter out zero/invalid RTTs
    valid_rtts = {ip: rtt for ip, rtt in rtts_dict.items() if rtt > 0}
    
    if not valid_rtts:
        return {}
    
    total_rtt = sum(valid_rtts.values())
    weights = {ip: (rtt / total_rtt) * 100.0 for ip, rtt in valid_rtts.items()}
    
    return weights


def extract_actual_data(df):
    """
    Extract weight allocation from allocated_bw_mbps
    """
    df['time_round'] = df['elapsed_sec'].round()
    times = sorted(df['time_round'].unique())
    
    # ✅ DETECT flows và first_seen timestamp
    flow_ips = sorted(df['src_ip'].unique())
    
    # ✅ SORT BY FIRST APPEARANCE TIME
    flow_first_seen = {}
    for ip in flow_ips:
        first_time = df[df['src_ip'] == ip]['elapsed_sec'].min()
        flow_first_seen[ip] = first_time
    
    # Sort by appearance order
    flow_ips_sorted = sorted(flow_first_seen.keys(), key=lambda x: flow_first_seen[x])
    print(f"✓ Detected flows (in order):")
    for i, ip in enumerate(flow_ips_sorted, 1):
        print(f"  Flow {i}: {ip} (t={flow_first_seen[ip]:.0f}s)")
    rtt_data = {ip: [] for ip in flow_ips}
    weight_data = {ip: [] for ip in flow_ips}
    jfi_data = []
    
    for t in times:
        df_t = df[df['time_round'] == t]
        
        # ✅ OPTION 1: Đọc cột 'weight' (nếu có)
        if 'weight' in df.columns:
            weights_raw = {}
            rtts = {}
            
            for _, row in df_t.iterrows():
                src_ip = row['src_ip']
                if src_ip in flow_ips:
                    weights_raw[src_ip] = row['weight']
                    rtts[src_ip] = row['rtt_ms']
            
            # Nếu tất cả weights = 0, dùng allocated_bw_mbps
            if all(w == 0 for w in weights_raw.values()):
                print(f"[WARN] t={t}: All weights=0, using allocated_bw_mbps")
                # Fallback: dùng allocated_bw_mbps
                allocated_bw = {}
                for _, row in df_t.iterrows():
                    src_ip = row['src_ip']
                    if src_ip in flow_ips:
                        allocated_bw[src_ip] = row['allocated_bw_mbps']
                        rtts[src_ip] = row['rtt_ms']
                
                total_bw = sum(allocated_bw.values())
                if total_bw > 0:
                    weights = {ip: (bw / total_bw) * 100.0 
                              for ip, bw in allocated_bw.items()}
                else:
                    weights = {}
            else:
                # Tính % từ weights
                total_weight = sum(weights_raw.values())
                if total_weight > 0:
                    weights = {ip: (w / total_weight) * 100.0 
                              for ip, w in weights_raw.items()}
                else:
                    weights = {}
        
        # ✅ OPTION 2: Dùng allocated_bw_mbps trực tiếp
        elif 'allocated_bw_mbps' in df.columns:
            allocated_bw = {}
            rtts = {}
            
            for _, row in df_t.iterrows():
                src_ip = row['src_ip']
                if src_ip in flow_ips:
                    allocated_bw[src_ip] = row['allocated_bw_mbps']
                    rtts[src_ip] = row['rtt_ms']
            
            # Tính % từ bandwidth
            total_bw = sum(allocated_bw.values())
            if total_bw > 0:
                weights = {ip: (bw / total_bw) * 100.0 
                          for ip, bw in allocated_bw.items()}
            else:
                weights = {}
        
        # ✅ OPTION 3: FALLBACK - Tính từ RTT
        else:
            rtts = {}
            for _, row in df_t.iterrows():
                src_ip = row['src_ip']
                rtt_ms = row['rtt_ms']
                if src_ip in flow_ips and rtt_ms > 0:
                    rtts[src_ip] = rtt_ms
            
            weights = compute_wmms_weights_from_rtts(rtts)
        
        # Fill data
        for ip in flow_ips:
            rtt_data[ip].append(rtts.get(ip, 0))
            weight_data[ip].append(weights.get(ip, 0))
        
        # JFI
        jfi = df_t['jfi'].iloc[0] if len(df_t) > 0 else 0
        jfi_data.append(jfi)
    
    return np.array(times), rtt_data, weight_data, jfi_data, flow_ips_sorted

def plot_actual_weights(csv_file, test_name, output_dir):
    """
    Plot actual weights and JFI from CSV
    AUTO-SCALE X-AXIS BASED ON DATA
    """
    print(f"\n{'='*70}")
    print(f"📊 LOADING ACTUAL DATA FROM CSV")
    print(f"{'='*70}\n")
    
    # Load data
    df = load_csv_data(csv_file)
    
    # ✅ CHECK: Log các cột có trong CSV
    print(f"📄 CSV Columns: {list(df.columns)}")
    if 'weight' in df.columns:
        print(f"✅ Using 'weight' column from CSV")
    elif 'allocated_bw_mbps' in df.columns:
        print(f"✅ Using 'allocated_bw_mbps' to compute weights")
    else:
        print(f"⚠️  Fallback: Computing weights from RTT")
    print()
    
    times, rtt_data, weight_data, jfi_data, flow_ips_sorted = extract_actual_data(df)
    
    # ✅ GET MAX TIME FOR AUTO-SCALE
    max_time = times[-1]
    
    print(f"✓ Loaded {len(times)} time points")
    print(f"✓ Duration: {max_time:.0f} seconds\n")
    
    # ============ CREATE FIGURE ============
    fig, axes = plt.subplots(2, 1, figsize=(15, 11), 
                            gridspec_kw={'height_ratios': [2.5, 1], 'hspace': 0.3})
    
    ax_weight = axes[0]
    ax_jfi = axes[1]
    
    # ============ WEIGHT ALLOCATION PLOT ============
    print(f"📊 Weight Allocation Statistics:")
    
    # ✅ AUTO-DETECT flows và colors
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

    for idx, ip in enumerate(flow_ips_sorted, 1):
        weights = weight_data[ip]
        rtts = rtt_data[ip]
        non_zero_weights = [w for w in weights if w > 0]
        non_zero_rtts = [r for r in rtts if r > 0]
        
        if non_zero_weights:
            avg_weight = np.mean(non_zero_weights)
            avg_rtt = np.mean(non_zero_rtts) if non_zero_rtts else 0
            
            color = colors[(idx-1) % len(colors)]
            label = f"Flow {idx} ({ip})"
            
            ax_weight.plot(times, weights,
                label=f'{label} (RTT={avg_rtt:.0f}ms, avg={avg_weight:.1f}%)',
                color=color, linewidth=3.0, alpha=0.9)
            
            print(f"  {label}: RTT={avg_rtt:.0f}ms | Avg Weight: {avg_weight:5.1f}%")
    
    # ============ TIMELINE ANNOTATIONS - AUTO-DETECT ============
    # Detect test type from duration
    if max_time >= 500:  # TB1 600s
        annotations = [
            (0, 'Flow 1 starts'),
            (50, 'Flow 2 starts'),
            (100, 'Flow 3 starts'),
            (450, 'Flow 3 ends'),
            (550, 'Flow 2 ends'),
        ]
        print(f"\n📌 Detected TB1 (600s) timeline")
    else:  # TB2/TB3 shorter tests
        annotations = [
            (0, 'Flow 1 starts'),
            (30, 'Flow 2 starts'),
            (60, 'Flow 3 starts'),
            (90, 'Flow 3 ends'),
            (120, 'Flow 2 ends'),
        ]
        print(f"\n📌 Detected TB2/TB3 (150s) timeline")
    
    y_pos = 102  # Fixed position above 100%
    
    for t, label in annotations:
        if t <= max_time:
            ax_weight.axvline(x=t, color='gray', linestyle=':', 
                            linewidth=1.5, alpha=0.5)
            # Dynamic text offset: 1% of max_time
            x_offset = max_time * 0.01
            ax_weight.text(t + x_offset, y_pos, label, 
                          fontsize=9, color='gray', ha='left')
    
    # ============ FORMATTING - AUTO-SCALE X-AXIS ============
    ax_weight.set_ylabel('Allocated Weight [%]', fontsize=13, fontweight='bold')
    ax_weight.set_ylim(-2, 108)
    ax_weight.set_xlim(0, max_time * 1.05)  # ✅ 5% padding
    ax_weight.grid(True, alpha=0.3, linestyle='--')
    ax_weight.legend(loc='upper right', fontsize=11, framealpha=0.95)
    ax_weight.set_title(f'{test_name.upper()}: WMMS Weight Allocation (Actual))', 
                       fontsize=14, fontweight='bold', pad=15)
    
    # ============ JFI PLOT ============
    ax_jfi.plot(times, jfi_data,
               color='#2ca02c', linewidth=3.0, alpha=0.9,
               label="Jain's Fairness Index")
    ax_jfi.fill_between(times, 0, jfi_data,
                       color='#90ee90', alpha=0.3)
    ax_jfi.axhline(y=1.0, color='red', linestyle='--', linewidth=1.5,
                  alpha=0.7, label='Perfect Fairness (1.0)')
    
    avg_jfi = np.mean(jfi_data)
    print(f"\n📉 JFI Statistics:")
    print(f"  Average JFI: {avg_jfi:.4f}")
    print(f"  Min JFI: {np.min(jfi_data):.4f}")
    print(f"  Max JFI: {np.max(jfi_data):.4f}")
    
    ax_jfi.set_xlabel('Time [seconds]', fontsize=13, fontweight='bold')
    ax_jfi.set_ylabel("Jain's Fairness Index", fontsize=13, fontweight='bold')
    ax_jfi.set_xlim(0, max_time * 1.05)  # ✅ Match weight plot
    ax_jfi.set_ylim(0, 1.08)
    ax_jfi.legend(loc='lower right', fontsize=11, framealpha=0.95)
    ax_jfi.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    
    # Save
    output_file = os.path.join(output_dir, f'{test_name}_actual_weights.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✅ Saved: {output_file}")
    plt.close()
    
    print(f"{'='*70}\n")


def main():
    if len(sys.argv) < 2:
        print("\nUsage: python3 visualize_actual_weights_fixed.py <test_name>")
        print("Example: python3 visualize_actual_weights_fixed.py tb1\n")
        sys.exit(1)
    
    test_name = sys.argv[1].lower()
    csv_file = f"logs/control_log_{test_name}.csv"
    
    if not os.path.exists(csv_file):
        print(f"\n❌ Error: CSV file not found: {csv_file}\n")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"📊 ACTUAL WEIGHT ALLOCATION VISUALIZATION")
    print(f"{'='*80}\n")
    
    # Create output directory
    output_dir = f"results_{test_name}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate plot
    plot_actual_weights(csv_file, test_name, output_dir)
    
    print(f"\n{'='*80}")
    print(f"✅ Visualization complete!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()