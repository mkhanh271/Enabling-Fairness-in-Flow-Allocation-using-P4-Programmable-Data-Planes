#!/usr/bin/env python3
import sys
import os
import time
import threading
import queue
import csv
from collections import defaultdict, deque
from datetime import datetime
from urllib import response
import numpy as np
import json
import math
import grpc
import socket
import struct
import zlib

# P4Runtime imports
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'utils/'))
import p4runtime_lib.bmv2
from p4runtime_lib.switch import ShutdownAllSwitchConnections
import p4runtime_lib.helper
from p4.v1 import p4runtime_pb2
from p4.v1 import p4runtime_pb2_grpc

# Jenks Natural Breaks (fallback only)
try:
    import jenkspy
    JENKSPY_AVAILABLE = True
    print("JENKSPY is ready")
except ImportError:
    JENKSPY_AVAILABLE = False
    print("Warning: jenkspy not installed. Using K-means or quantile fallback.")

# ==================== Configuration ====================
WINDOW_SIZE = 1.0  # Aggregation window for fairness computation
BOTTLENECK_CAPACITY_MBPS = 20
CURRENT_TEST_FILE = "current_test.txt"
LOGS_DIR = "logs"
P4_SWITCH_ADDRESS = "127.0.0.1:50051"
P4_DEVICE_ID = 0

#  RTT Smoothing Parameters
RTT_EWMA_ALPHA = 0.125  # α = 0.125 (standard TCP EWMA)
RTT_MIN_SAMPLES = 2     
RTT_SPIKE_THRESHOLD = 3.0
RTT_ABSOLUTE_SPIKE_MS = 20.0

DEBUG_FLOW_LIFETIME = False

EXPECTED_FLOWS = {
    'tb1': 3,
    'tb2': 3,
    'tb3': 3
}

MAX_THEORETICAL_THROUGHPUT_MBPS = 40
MIN_ELAPSED_SEC = 0.01
MAX_ELAPSED_SEC = 2.0
MIN_FLOW_LIFETIME = 1.0
MIN_FLOW_BYTES = 10_000

LONG_FLOW_BYTES_THRESHOLD = 10 * 1024 * 1024
BOTTLENECK_CAPACITY_MBPS = 20
FLOW_CHANGE_THRESHOLD = 0.15 # 15% change to consider reallocation
MIN_RECONFIG_INTERVAL = 5.0   # Minimum 5s between reconfigs

# ==================== CSV Logger ====================
class CSVLogger:
    """Log per-flow metrics to CSV"""
    def __init__(self, test_name):
        self.test_name = test_name
        self.log_file = os.path.join(LOGS_DIR, f"control_log_{test_name}.csv")
        self.file_handle = None
        self.writer = None
        self.start_time = time.time()
        
        os.makedirs(LOGS_DIR, exist_ok=True)
        self.file_handle = open(self.log_file, 'w', newline='')
        self.writer = csv.writer(self.file_handle)
        
        # Header row
        self.writer.writerow([
            'timestamp', 'src_ip', 'dst_ip', 'src_port', 'dst_port',
            'throughput_mbps',      # Throughput đo được thực tế
            'allocated_bw_mbps',    # Throughput được cấp từ WMMS
            'rtt_ms',               # RTT thực tế từ digest
            'weight_percent',       # weight (%) trong WMMS
            'jfi',
            'group', 'is_long_flow', 'bottleneck_mbps'
        ])
        self.file_handle.flush()
        print(f"[CSV] ✓ Logging to: {self.log_file}")
    
    def log_metrics(self, flow_table, static_flow_map, jfi):
        """Log all flows at current timestamp"""
        now = datetime.now()
        now_ts = time.time()
        
        # ✅ COMPUTE TOTAL ALLOCATED BW
        total_allocated = sum(f.fair_rate_bps for f in flow_table.values())
        
        for fid, flow in flow_table.items():
            # SKIP flows không active
            if now_ts - flow.last_digest_time > 3.0:
                continue
            
            flow_info = static_flow_map.get(fid, {})
            
            src_ip = flow_info.get('src_ip', f'FID_{fid}')
            dst_ip = flow_info.get('dst_ip', 'unknown')
            src_port = flow_info.get('src_port', 0)
            dst_port = flow_info.get('dst_port', 5201)
            
            # ✅ WEIGHT CALCULATION
            if flow.group == 'udp':
                # UDP: Weight = % of total bandwidth
                if total_allocated > 0:
                    weight_percent = (flow.fair_rate_bps / total_allocated) * 100.0
                else:
                    weight_percent = 0
                rtt_ms = 0  # UDP has no RTT
            else:
                # TCP: Weight based on RTT
                if flow.rtt_smoothed is not None and flow.rtt_smoothed > 0:
                    rtt_ms = flow.rtt_smoothed
                elif flow.rtt_ms is not None and flow.rtt_ms > 0:
                    rtt_ms = flow.rtt_ms
                else:
                    rtt_ms = 0
                
                # Calculate TCP weight %
                total_tcp_rtt = sum(
                    f.rtt_smoothed if f.rtt_smoothed else (f.rtt_ms if f.rtt_ms else 0)
                    for f in flow_table.values()
                    if f.group != 'udp'
                )
                
                if total_tcp_rtt > 0 and rtt_ms > 0:
                    weight_percent = (rtt_ms / total_tcp_rtt) * 100.0
                else:
                    weight_percent = 0
            
            self.writer.writerow([
                now.isoformat(),
                src_ip, 
                dst_ip,
                src_port,
                dst_port,
                flow.throughput_bps / 1e6,
                flow.fair_rate_bps / 1e6,
                rtt_ms,
                weight_percent,
                jfi,
                flow.group,
                1 if flow.is_long_flow else 0,
                BOTTLENECK_CAPACITY_MBPS
            ])
        
        self.file_handle.flush()
    
    def close(self):
        if self.file_handle:
            self.file_handle.close()
            print(f"[CSV] ✓ Closed")

# ==================== RTT Validator ====================
class RTTValidator:
    """
    Validate RTT measurements to ensure data quality
    Provides statistics for debugging and verification
    """ 
    def __init__(self, expected_rtt_range=(1.0, 100.0)):
        self.expected_min, self.expected_max = expected_rtt_range
        self.samples = []
        self.invalid_samples = []
        self.zero_samples = 0
        self.out_of_range_samples = 0
        self.spike_events = []

    def validate(self, rtt_ms, flow_id=None):
        """
        Validate a single RTT sample
        Args:
            rtt_ms: RTT in milliseconds
            flow_id: Optional flow identifier for debugging
        Returns:
            (is_valid, reason) tuple
        """
        if rtt_ms is None:
            return False, "RTT is None"
        
        if rtt_ms <= 0:
            self.zero_samples += 1
            self.invalid_samples.append({
                'rtt': rtt_ms,
                'flow': flow_id,
                'reason': 'zero_or_negative'
            })
            return False, f"RTT <= 0: {rtt_ms:.2f} ms"
        
        if rtt_ms < self.expected_min or rtt_ms > self.expected_max:
            self.out_of_range_samples += 1
            self.invalid_samples.append({
                'rtt': rtt_ms,
                'flow': flow_id,
                'reason': 'out_of_range'
            })
            return False, f"RTT out of range [{self.expected_min}, {self.expected_max}]: {rtt_ms:.2f} ms"
        
        self.samples.append(rtt_ms)
        return True, "OK"
    
    def detect_spike(self, rtt_min_ms, rtt_max_ms, flow_id=None):
        """Detect RTT spike within interval"""
        if rtt_min_ms <= 0 or rtt_max_ms <= 0:
            return False, "Invalid RTT range"
        ratio = rtt_max_ms / rtt_min_ms
        diff = rtt_max_ms - rtt_min_ms
        if ratio >= RTT_SPIKE_THRESHOLD or diff >= RTT_ABSOLUTE_SPIKE_MS:
            spike_info = {
                'flow': flow_id,
                'min_ms': rtt_min_ms,
                'max_ms': rtt_max_ms,
                'ratio': ratio,
                'diff_ms': diff,
                'timestamp': time.time()
            }
            self.spike_events.append(spike_info)
            return True, f"Spike: {rtt_min_ms:.2f}→{rtt_max_ms:.2f}ms (ratio={ratio:.2f}x)"
        return False, "No spike"

    def get_statistics(self):
        """ Get comprehensive statistics about RTT measurements """
        if not self.samples:
            return {
                'valid_samples': 0,
                'invalid_samples': len(self.invalid_samples),
                'zero_samples': self.zero_samples,
                'out_of_range_samples': self.out_of_range_samples,
                'spike_events': len(self.spike_events),
                'message': 'No valid RTT samples collected'
            }
        
        return {
            'valid_samples': len(self.samples),
            'invalid_samples': len(self.invalid_samples),
            'zero_samples': self.zero_samples,
            'out_of_range_samples': self.out_of_range_samples,
            'spike_events': len(self.spike_events),
            'mean_rtt_ms': np.mean(self.samples),
            'std_rtt_ms': np.std(self.samples),
            'min_rtt_ms': np.min(self.samples),
            'max_rtt_ms': np.max(self.samples),
            'median_rtt_ms': np.median(self.samples),
            'validity_rate': len(self.samples) / (len(self.samples) + len(self.invalid_samples)) * 100
        }

    def print_summary(self):
        """Print formatted summary of RTT validation"""
        stats = self.get_statistics()
        print("\n" + "="*70)
        print("RTT MEASUREMENT VALIDATION SUMMARY")
        print("="*70)
        
        if stats['valid_samples'] == 0:
            print(f"⚠️  {stats['message']}")
            print(f"   Invalid samples: {stats['invalid_samples']}")
            print(f"   - Zero/negative: {stats['zero_samples']}")
            print(f"   - Out of range: {stats['out_of_range_samples']}")
        else:
            print(f"✅ Valid samples: {stats['valid_samples']}")
            print(f"❌ Invalid samples: {stats['invalid_samples']}")
            print(f"   - Zero/negative: {stats['zero_samples']}")
            print(f"   - Out of range: {stats['out_of_range_samples']}")
            print(f"\n📊 RTT Statistics (ms):")
            print(f"   Mean: {stats['mean_rtt_ms']:.2f}")
            print(f"   Std Dev: {stats['std_rtt_ms']:.2f}")
            print(f"   Min: {stats['min_rtt_ms']:.2f}")
            print(f"   Max: {stats['max_rtt_ms']:.2f}")
            print(f"   Median: {stats['median_rtt_ms']:.2f}")
            print(f"\n✓ Validity rate: {stats['validity_rate']:.1f}%")
            
            # Assessment
            if stats['validity_rate'] >= 95:
                print("   Assessment: EXCELLENT (≥95%)")
            elif stats['validity_rate'] >= 90:
                print("   Assessment: GOOD (≥90%)")
            elif stats['validity_rate'] >= 80:
                print("   Assessment: ACCEPTABLE (≥80%)")
            else:
                print("   Assessment: POOR (<80%) - Check P4 RTT measurement")
            
            if stats['spike_events'] > 0:
                print(f"\n RTT Spike Events: {stats['spike_events']}")
                print("   (Bufferbloat or congestion detected)")
        
        print("="*70 + "\n")

import subprocess
import math
from collections import defaultdict

class LiveOVSQueueManager:
    """ Auto-configure OVS queues in real-time """
    
    def __init__(self, switch="s2", interface="s2-eth2", bottleneck_mbps=20):
        self.queue_ranges = {}
        self.switch = switch
        self.interface = interface
        self.bottleneck_bps = bottleneck_mbps * 1_000_000
        self.current_queues = {}
        self.active_flows = {}
        
        self.queue_map = {
            'low-rtt': 1,
            'med-rtt': 2,
            'high-rtt': 3,
            'udp': 4,
            'unknown': 5,
        }
    
    def update_queue_ranges(self, jenks_breakpoints):
        """
        Nhận breakpoints từ Jenks và config queues
        """
        self.queue_ranges = {}
        for qrange in jenks_breakpoints:
            qid = qrange['queue_id']
            self.queue_ranges[qid] = {
                'rtt_min': qrange['rtt_min'],
                'rtt_max': qrange['rtt_max'],
                'label': qrange['label']
            }
        
        print(f"[OVS] Updated queue ranges:")
        for qid, info in self.queue_ranges.items():
            print(f"  Queue {qid}: {info['rtt_min']:.2f} < RTT < {info['rtt_max']:.2f} ms")

    def apply_queue_config(self, flow_allocations, flow_table):
        """
        Apply OVS queue configuration with Stanford buffers
        
        Steps:
        1. Clear OLD queue-based rules (priority 200)
        2. Create NEW queues with HTB
        3. Install NEW flow rules with set_queue action (priority 200)
        """
        if not flow_allocations:
            return False
        
        print(f"\n[OVS] Applying queue config for {len(flow_allocations)} flows")
        
        # ✅ STEP 1: Clear OLD queue rules (priority 200 only)
        self._clear_queue_rules()
        
        # ✅ STEP 2: Group flows by queue
        queues = defaultdict(list)
        for flow_key, info in flow_allocations.items():
            qid = info['queue']
            queues[qid].append({
                'flow': flow_key,
                'bw_mbps': info['bw_mbps'],
                'rtt_ms': info['rtt_ms'],
                'group': info['group']
            })
        
        # ✅ STEP 3: Create HTB queues
        success = self._create_htb_queues(queues, flow_table)
        if not success:
            return False
        
        # ✅ STEP 4: Install flow rules with set_queue
        self._install_queue_flow_rules(flow_allocations)
        
        return True
    
    def _clear_queue_rules(self):
        """
        Clear ONLY priority 200 rules (queue-based)
        Keep topology's priority 150 rules (monitoring)
        """
        print("[OVS] Clearing old queue rules (priority 200)...")
        
        # Delete priority 200 rules
        subprocess.run([
            "ovs-ofctl", "-O", "OpenFlow13", "del-flows", self.switch,
            "priority=200"
        ], stderr=subprocess.DEVNULL)
        
        # Clear QoS config
        subprocess.run([
            "ovs-vsctl", "--", "--if-exists", "destroy", "QoS", self.interface,
            "--", "clear", "Port", self.interface, "qos"
        ], stderr=subprocess.DEVNULL)
    
    def _create_htb_queues(self, queues, flow_table):
        """Create HTB queues with Stanford buffer sizing"""
        print(f"[OVS] Creating {len(queues)} HTB queues on {self.interface}")
        
        # Build ovs-vsctl command
        cmd = ["ovs-vsctl", "set", "port", self.interface, "qos=@qos", "--"]
        cmd += ["--id=@qos", "create", "qos", "type=linux-htb"]
        cmd += [f"other-config:max-rate={self.bottleneck_bps}"]
        
        # Queue references
        queue_refs = [f"queues:{qid}=@q{qid}" for qid in sorted(queues.keys())]
        cmd += queue_refs + ["--"]
        
        # Create each queue
        for qid in sorted(queues.keys()):
            flows = queues[qid]
            
            total_bw = sum(f['bw_mbps'] for f in flows)
            bw_bps = int(total_bw * 1_000_000)
            
            # Compute Stanford buffer
            buffer_bytes = self.compute_stanford_buffer(flows, flow_table)
            
            # Guarantee 90% of allocated bandwidth
            min_rate = int(bw_bps * 0.9)
            
            cmd += [
                f"--id=@q{qid}", "create", "queue",
                f"other-config:max-rate={bw_bps}",
                f"other-config:min-rate={min_rate}",
                f"other-config:burst={buffer_bytes}",
                "--"
            ]
            
            print(f"  Queue {qid} ({flows[0]['group']}): "
                  f"{len(flows)} flows, BW={total_bw:.2f}Mbps, "
                  f"Buffer={buffer_bytes/1024:.1f}KB")
        
        # Execute
        try:
            subprocess.run(cmd[:-1], check=True, stderr=subprocess.PIPE)
            return True
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Queue creation failed: {e.stderr.decode()}")
            return False
        
    def _install_queue_flow_rules(self, flow_allocations):
        """Install OpenFlow rules with set_queue action"""
        
        # ✅ LẤY PORT TỪ BOTTLENECK CONFIG
        try:
            with open('bottleneck_config.json', 'r') as f:
                config = json.load(f)
                port_to_s3 = config['bottleneck_port']
                port_to_s1 = 1  # s2-eth1
        except:
            port_to_s3 = 2
            port_to_s1 = 1
        
        print(f"[OVS] Installing {len(flow_allocations)} queue flow rules...")
        
        for flow_key, info in flow_allocations.items():
            qid = info['queue']
            
            # ✅ PARSE flow_key: "10.0.0.1:5201->10.0.1.1:5201"
            if '->' not in flow_key:
                continue
            
            try:
                left, right = flow_key.split('->')
                src_ip, src_port = left.split(':')
                dst_ip, dst_port = right.split(':')
            except:
                print(f"[WARN] Failed to parse flow_key: {flow_key}")
                continue
            
            proto = 'udp' if info['group'] == 'udp' else 'tcp'
            
            # ✅ FORWARD (client->server)
            subprocess.run([
                "ovs-ofctl", "-O", "OpenFlow13", "add-flow", self.switch,
                f"priority=200,{proto},"
                f"nw_src={src_ip},nw_dst={dst_ip},"
                f"tp_dst={dst_port},"
                f"actions=set_queue:{qid},mod_dl_src=aa:bb:cc:dd:ee:ff,"
                f"mod_dl_dst={self._get_dst_mac(dst_ip)},"
                f"dec_ttl,output:{port_to_s3}"
            ], stderr=subprocess.DEVNULL)
            
            # ✅ REVERSE (server->client)
            subprocess.run([
                "ovs-ofctl", "-O", "OpenFlow13", "add-flow", self.switch,
                f"priority=200,{proto},"
                f"nw_src={dst_ip},nw_dst={src_ip},"
                f"tp_src={dst_port},"
                f"actions=set_queue:{qid},mod_dl_src=aa:bb:cc:dd:ee:ff,"
                f"mod_dl_dst={self._get_dst_mac(src_ip)},"
                f"dec_ttl,output:{port_to_s1}"
            ], stderr=subprocess.DEVNULL)
        
        print(f"[OVS] ✓ Installed {len(flow_allocations)} flow rules (priority 200)")

    def _get_dst_mac(self, ip):
        """Get MAC address from topology"""
        # h1-h3: 10.0.0.x
        if ip.startswith('10.0.0.'):
            host_id = int(ip.split('.')[-1])
            return f"00:00:00:00:00:{host_id:02x}"
        # h4-h6: 10.0.1.x
        elif ip.startswith('10.0.1.'):
            host_id = int(ip.split('.')[-1])
            return f"00:00:00:00:01:{host_id:02x}"
        
    def compute_stanford_buffer(self, flows, flow_table):
        """
        Stanford buffer PER GROUP
        Args:
            flows: List of flows in THIS queue
            flow_table: Full flow_table to get RTT info
        """
        if not flows:
            return 8192
        
        N = len(flows)
        
        # Get RTTs from flow_table (not from flows dict)
        group_name = flows[0]['group']
        flows_in_group = [
            flow_table[fid] for fid in flow_table.keys()
            if flow_table[fid].group == group_name
        ]
        
        rtts_ms = [f.get_stable_rtt_ms() or 1.0 for f in flows_in_group]
        RTT_avg_sec = (sum(rtts_ms) / len(rtts_ms)) / 1000.0
        
        # BANDWIDTH CỦA QUEUE (sum of allocated BW)
        C_bps = sum(f['bw_mbps'] for f in flows) * 1_000_000
        
        buffer_bytes = (C_bps * RTT_avg_sec) / math.sqrt(N)
        buffer_bytes = max(8192, min(int(buffer_bytes), 2_000_000))
        
        return buffer_bytes

# ==================== Flow Data Structure ====================
class FlowData:
    """
     PAPER-ALIGNED: RTT warm-up + EWMA smoothing
    """
    def __init__(self):
        self.throughput_bps = 0
        self.rtt_ms = None  # Raw RTT (milliseconds)
        
        #  PAPER SECTION III.B: RTT Smoothing
        self.rtt_smoothed = None  # EWMA-smoothed RTT
        self.rtt_samples = 0       # Sample count for warm-up
        self.rtt_stable = False    # Stability flag
        
        self.rtt_min_us = None
        self.rtt_max_us = None
        self.spike_detected = False
        self.packet_count = 0
        
        self.queue_delay_us = 0
        self.group = "unknown"
        self.fair_rate_bps = 0
        self.history = deque(maxlen=3)
        self.last_update = time.time()
        self.first_seen = time.time()
        self.total_bytes = 0
        self.is_long_flow = False
        self.total_bytes_switch = 0
        self.last_digest_time = time.time()

    def update(self, throughput, rtt, queue_delay, bytes_in_interval=0,
               rtt_min=None, rtt_max=None, packet_count=0):
        #  Only update throughput if bytes > 0
        if bytes_in_interval > 0:
            self.throughput_bps = throughput
            self.total_bytes += bytes_in_interval
        
        self.queue_delay_us = queue_delay
        self.packet_count = packet_count
        
        #  Track RTT min/max
        if rtt_min is not None and rtt_min > 0:
            self.rtt_min_us = rtt_min
        if rtt_max is not None and rtt_max > 0:
            self.rtt_max_us = rtt_max
        
        #  Update RTT with EWMA + warm-up
        if rtt is not None and rtt > 0:
            self.rtt_ms = rtt
            self.rtt_samples += 1
            
            if self.rtt_smoothed is None:
                # First sample: initialize
                self.rtt_smoothed = rtt
            else:
                # EWMA: new = α * sample + (1-α) * old
                self.rtt_smoothed = (
                    RTT_EWMA_ALPHA * rtt +
                    (1 - RTT_EWMA_ALPHA) * self.rtt_smoothed
                )
            
            # Mark stable after K samples
            if self.rtt_samples >= RTT_MIN_SAMPLES:
                self.rtt_stable = True
        
        self.history.append({
            'throughput': throughput,
            'rtt': rtt,
            'queue_delay': queue_delay,
            'timestamp': time.time()
        })
        self.last_update = time.time()

    def get_moving_avg(self, metric='throughput'):
        if not self.history:
            return 0
        values = [h[metric] for h in self.history]
        return sum(values) / len(values)
    
    def get_stable_rtt_ms(self):
        """
        Return stable RTT in milliseconds
        """
        if not self.rtt_stable:
            return None
        
        # Ưu tiên dùng smoothed RTT
        if self.rtt_smoothed is not None:
            return self.rtt_smoothed  # Already in ms
        elif hasattr(self, 'rtt_ms') and self.rtt_ms is not None:
            return self.rtt_ms
        elif hasattr(self, 'rtt_us') and self.rtt_us is not None:
            return self.rtt_us / 1000.0  # Convert µs → ms
        
        return None

# ==================== PDP Controller ====================
class PDPController:
    def _init_hardcoded_flows(self):
        """
        HARDCODE 5-tuple từ topology
        Mapping: FID → 5-tuple
        """
        self.expected_flows = {
            # TB1: 3 TCP flows
            'tb1': [
                {'src_ip': '10.0.0.1', 'dst_ip': '10.0.1.1', 'dst_port': 5201, 'protocol': 6},
                {'src_ip': '10.0.0.2', 'dst_ip': '10.0.1.2', 'dst_port': 5201, 'protocol': 6},
                {'src_ip': '10.0.0.3', 'dst_ip': '10.0.1.3', 'dst_port': 5201, 'protocol': 6},
            ],
            # TB2: 1 UDP + 2 TCP
            'tb2': [
                {'src_ip': '10.0.0.1', 'dst_ip': '10.0.1.1', 'dst_port': 5201, 'protocol': 17},  # UDP
                {'src_ip': '10.0.0.2', 'dst_ip': '10.0.1.2', 'dst_port': 5201, 'protocol': 6},   # TCP
                {'src_ip': '10.0.0.3', 'dst_ip': '10.0.1.3', 'dst_port': 5201, 'protocol': 6},   # TCP
            ]
        }
        print("[INIT] Hardcoded flow mappings ready")
    
    def __init__(self, p4info_file, bmv2_json_file):
        print(f"P4Info file: {p4info_file}")
        print(f"BMv2 JSON file: {bmv2_json_file}")
        print(f"Switch address: {P4_SWITCH_ADDRESS}")
        
        self.p4info_helper = p4runtime_lib.helper.P4InfoHelper(p4info_file)
        self.ovs_manager = LiveOVSQueueManager(
            switch="s2",
            interface="s2-eth2",
            bottleneck_mbps=BOTTLENECK_CAPACITY_MBPS
        )
        
        self.lock = threading.Lock()
        self.flow_table = {}
        self.flow_index_rev = {}
        self.running = True
        self.csv_logger = None
        self.current_test = None
        self.last_flow_count = 0
        self.last_reconfig_time = 0
        self.last_allocation = {}
        self.log_file_handle = None
        self.log_writer = None
        self.current_test = None
        self.current_log_file = None
        self.test_start_time = None
        self.rtt_validator = RTTValidator(expected_rtt_range=(0.00001, 10000.0))
                
        # Connect to P4 switch
        self.switch = self.connect_switch(bmv2_json_file)
        
        # Start fairness thread
        self.fairness_thread = threading.Thread(
            target=self.periodic_processing,
            daemon=True
        )
        self.fairness_thread.start()
        self.static_flow_map = {}
        self._init_hardcoded_flows()
        self.flow_metadata = {}
        self.prev_timestamp_us = {} # Track previous digest timestamp per flow
        print("[INIT] ✓ Controller ready\n")
    
    def _learn_flow_from_digest(self, flow_hash, timestamp_us):
        """
        Map FID với hardcoded 5-tuple
        """
        if flow_hash in self.static_flow_map:
            return
        
        # Get current test flows
        if not self.current_test or self.current_test not in self.expected_flows:
            # Fallback: tạo placeholder
            self.static_flow_map[flow_hash] = {
                'src_ip': f'FID_{flow_hash}',
                'dst_ip': 'unknown',
                'src_port': 0,
                'dst_port': 5201,
                'protocol': 6,
                'flow_key': f'FID_{flow_hash}',
                'learned': False,
                'first_seen': timestamp_us
            }
            print(f"[NEW FLOW] FID={flow_hash} (no test active)")
            return
        
        # ✅ MAP với expected flow chưa được assign
        expected_flows = self.expected_flows[self.current_test]
        
        for expected in expected_flows:
            # Check nếu flow này chưa được map
            already_mapped = any(
                f.get('flow_key') == f"{expected['src_ip']}:{expected['dst_port']}->{expected['dst_ip']}:{expected['dst_port']}"
                for f in self.static_flow_map.values()
            )
            
            if not already_mapped:
                self.static_flow_map[flow_hash] = {
                    'src_ip': expected['src_ip'],
                    'dst_ip': expected['dst_ip'],
                    'src_port': 0,
                    'dst_port': expected['dst_port'],
                    'protocol': expected['protocol'],
                    'flow_key': f"{expected['src_ip']}:{expected['dst_port']}->{expected['dst_ip']}:{expected['dst_port']}",
                    'learned': True,  # ✅ ĐÃ BIẾT
                    'first_seen': timestamp_us
                }
                
                proto_name = 'UDP' if expected['protocol'] == 17 else 'TCP'
                print(f"[HARDCODE] ✓ FID={flow_hash} → {expected['src_ip']}→{expected['dst_ip']}:{expected['dst_port']} ({proto_name})")
                break
   
    def _ip_to_int(self, ip_str):
        """Convert IP string to integer"""
        parts = ip_str.split('.')
        return (int(parts[0]) << 24) | (int(parts[1]) << 16) | \
            (int(parts[2]) << 8) | int(parts[3])
        
    def monitor_test_file(self):
        """Check if test changed"""
        try:
            if not os.path.exists(CURRENT_TEST_FILE):
                return
            
            with open(CURRENT_TEST_FILE, 'r') as f:
                test_name = f.read().strip()
            
            if test_name == "stop":
                if self.csv_logger:
                    self.csv_logger.close()
                    self.csv_logger = None
                self.current_test = None
                print(f"[TEST] Stopped logging")
                return
            
            if test_name != self.current_test and test_name in ['tb1', 'tb2', 'tb3']:
                # New test started
                if self.csv_logger:
                    self.csv_logger.close()
                
                self.csv_logger = CSVLogger(test_name)
                self.current_test = test_name
                print(f"\n[TEST] ✓ Started logging: {test_name}\n")
        
        except Exception as e:
            print(f"[WARN] Test file error: {e}")

    def _configure_digest_v3(self):
        """
        Configure digest qua WriteRequest
        """
        try:
            digest_id = self.p4info_helper.get_digests_id("flow_digest_t")
            print(f"[DEBUG] Digest ID from P4Info: {digest_id}")
            print(f"[DEBUG] Expected digest ID in P4 code: 397109657 ")
            if digest_id != 397109657:
                print(f"[WARNING] Digest ID mismatch! Check P4Info and P4 code.")
                
            print(f"[DIGEST CONFIG] Configuring digest_id={digest_id}")
            
            # Tạo WriteRequest
            req = p4runtime_pb2.WriteRequest()
            req.device_id = P4_DEVICE_ID
            req.election_id.low = 1
            
            update = req.updates.add()
            update.type = p4runtime_pb2.Update.INSERT
            
            # Digest entry
            digest_entry = update.entity.digest_entry
            digest_entry.digest_id = digest_id
            digest_entry.config.max_timeout_ns = 1_000_000_000    # 1s
            digest_entry.config.max_list_size = 3
            digest_entry.config.ack_timeout_ns = 2_000_000_000    # 2s
            
            # Gửi qua stub
            try:
                response = self.switch.client_stub.Write(req)
                print(f"[DIGEST CONFIG] Write response: {response}")
            except grpc.RpcError as e:
                print(f"[DIGEST CONFIG] gRPC error:")
                print(f"  Code: {e.code()}")
                print(f"  Details: {e.details()}")
                print(f"  Metadata: {e.trailing_metadata()}")
                raise
            
            print(f"[DIGEST CONFIG] ✓ Configured successfully\n")
            
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                print(f"[DIGEST CONFIG] ⚠ Already exists (OK)\n")
            else:
                print(f"[DIGEST CONFIG] ✗ Failed: {e.details()}\n")
                raise
    
    def test_digest_reception(self):
        """Test if we can receive ANY digest"""
        print("[TEST] Testing digest reception...")
        
        # CHECK: Xem dispatcher có digest queue không
        if hasattr(self.switch, 'dispatcher'):
            print(f"[TEST] Dispatcher exists: {self.switch.dispatcher}")
            if hasattr(self.switch.dispatcher, 'digest_queue'):
                print(f"[TEST] Digest queue exists: {self.switch.dispatcher.digest_queue}")
                
                # Thử lấy trực tiếp từ queue
                print("[TEST] Trying to get from digest_queue directly...")
                try:
                    digest = self.switch.dispatcher.digest_queue.get(timeout=5.0)
                    if digest:
                        print(f"[TEST] ✓ Got digest from queue: {digest}")
                        return
                except Exception as e:
                    print(f"[TEST] Queue get failed: {e}")
        
        # CHECK: Xem stream_msg_resp
        print("[TEST] Checking stream_msg_resp...")
        try:
            for i in range(5):
                if hasattr(self.switch, 'stream_msg_resp'):
                    print(f"[TEST] Iteration {i}: checking stream...")
                    # Timeout ngắn
                    time.sleep(1.0)
        except Exception as e:
            print(f"[TEST] Stream check failed: {e}")
        
    def _process_digest_data(self, digest_data, prev_bytes, prev_time):
        """
        Extract metrics from digest struct and update flow_table
        """
        try:
            # ✅ PARSE STRUCT MEMBERS (THỨ TỰ ĐÚNG THEO P4)
            members = digest_data.struct.members
            
            flow_hash        = int.from_bytes(members[0].bitstring, byteorder='big')
            bytes_interval   = int.from_bytes(members[1].bitstring, byteorder='big')
            packets_interval = int.from_bytes(members[2].bitstring, byteorder='big')
            rtt_last_us      = int.from_bytes(members[3].bitstring, byteorder='big')
            rtt_samples      = int.from_bytes(members[4].bitstring, byteorder='big')
            is_long_flow     = int.from_bytes(members[5].bitstring, byteorder='big')  # bit<8>
            timestamp_us     = int.from_bytes(members[6].bitstring, byteorder='big')
            protocol         = int.from_bytes(members[7].bitstring, byteorder='big')
            
            # ✅ DEBUG LOG
            if flow_hash in self.static_flow_map:
                flow_key = self.static_flow_map[flow_hash]['flow_key']
                print(f"[DIGEST]   - {flow_key}: {bytes_interval} bytes, "
                    f"{packets_interval} pkts, RTT={rtt_last_us/1000:.2f}ms")
            
            # Skip if no data
            if bytes_interval == 0 and packets_interval == 0:
                return False
            
            # ✅ COMPUTE THROUGHPUT
            now = time.time()
            delta_t = max(now - prev_time, 0.001)  # Avoid division by zero
            
            throughput_bps = (bytes_interval * 8) / delta_t
            
            # ✅ UPDATE FLOW TABLE
            with self.lock:
                if flow_hash not in self.flow_table:
                    self.flow_table[flow_hash] = FlowData()
                    
                    # Set protocol from static map
                    if flow_hash in self.static_flow_map:
                        protocol = self.static_flow_map[flow_hash]['protocol']
                        self.flow_table[flow_hash].group = 'tcp' if protocol == 6 else 'udp'
                
                flow = self.flow_table[flow_hash]
                
                # ✅ UPDATE RTT WITH EWMA
                if rtt_last_us > 0:
                    rtt_ms = rtt_last_us / 1000.0  # us → ms
                    flow.rtt_ms = rtt_ms
                    flow.rtt_samples = rtt_samples
                    
                    if flow.rtt_smoothed is None:
                        flow.rtt_smoothed = rtt_ms
                    else:
                        flow.rtt_smoothed = (
                            RTT_EWMA_ALPHA * rtt_ms +
                            (1 - RTT_EWMA_ALPHA) * flow.rtt_smoothed
                        )
                    
                    if rtt_samples >= RTT_MIN_SAMPLES:
                        flow.rtt_stable = True
                
                # ✅ UPDATE METRICS
                flow.update(
                    throughput_bps,
                    flow.rtt_ms,
                    0,  # queue_delay (not in digest)
                    bytes_interval,
                    packet_count=packets_interval
                )
                
                # ✅ ELEPHANT FLOW FLAG
                flow.is_long_flow = (is_long_flow == 1)
            
            prev_bytes[flow_hash] = bytes_interval
            return True
            
        except Exception as e:
            print(f"[ERROR] Processing digest: {e}")
            import traceback
            traceback.print_exc()
            return False
        
    def _ack_digest(self, digest_id, list_id):
        """Send ACK to P4 switch after processing digest"""
        try:
            ack = p4runtime_pb2.StreamMessageRequest()  
            ack.digest_ack.digest_id = digest_id
            ack.digest_ack.list_id = list_id
            
            self.switch.requests_stream.put(ack)
            
            # Debug 
            print(f"[ACK] Sent: digest_id={digest_id}, list_id={list_id}")
            
        except Exception as e:
            print(f"[WARN] Digest ACK failed: {e}")
        
    def connect_switch(self, bmv2_json_file):
        """
        DÙNG THƯ VIỆN P4RUNTIME CÓ SẴN 
        """
        print(f"\n[CONNECT] Connecting to {P4_SWITCH_ADDRESS}...")
        
        import p4runtime_lib.bmv2
        
        # Dùng Bmv2SwitchConnection có sẵn
        self.switch = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            address=P4_SWITCH_ADDRESS,
            device_id=P4_DEVICE_ID
        )
        
        # Arbitration
        print("[CONNECT] Sending arbitration...")
        arb_resp = self.switch.MasterArbitrationUpdate()
        if arb_resp and arb_resp.status.code == 0:
            print("[ARBITRATION] ✓ You are PRIMARY controller")
        else:
            print(f"[ARBITRATION] ⚠ Status: {arb_resp.status.code if arb_resp else 'No response'}")
        
        time.sleep(1.0)
        
        # Load pipeline
        print("[CONNECT] Loading P4 pipeline...")
        self.switch.SetForwardingPipelineConfig(
            p4info=self.p4info_helper.p4info,
            bmv2_json_file_path=bmv2_json_file
        )
        
        time.sleep(1.0)
        
        # Configure digest
        print("[CONNECT] Configuring digest...")
        self._configure_digest_v3()  # ← Hàm mới bên dưới
        
        time.sleep(1.0)
        
        print("[CONNECT] ✓ Switch connected\n")
        # ✅ CHECK DISPATCHER THREAD
        if self.switch.dispatcher.thread.is_alive():
            print("[CHECK] ✓ Dispatcher thread is ALIVE")
        else:
            print("[CHECK] ✗ Dispatcher thread is DEAD")
        return self.switch
    
    def is_reverse_flow(src_ip, dst_ip):
        """Check if this is reverse direction (server -> client)"""
        # Assuming clients are 10.0.0.x, servers are 10.0.1.x
        return src_ip.startswith('10.0.1.') and dst_ip.startswith('10.0.0.')
    
    def ipv4_to_str(self, ip_int):
        return '.'.join([
            str((ip_int >> 24) & 0xFF),
            str((ip_int >> 16) & 0xFF),
            str((ip_int >> 8) & 0xFF),
            str(ip_int & 0xFF)
        ])
        
    def is_flow_stable(self, flow):
        """
        PAPER-COMPLIANT stability check:
        1. Sufficient lifetime (≥1s)
        2. Sufficient data (≥10KB for mice, ≥10MB for elephants)
        3. RTT stable (≥K samples)
        4. Throughput stable (variance < threshold)
        """
        lifetime = time.time() - flow.first_seen
        
        # Basic checks
        if lifetime < MIN_FLOW_LIFETIME:
            return False
        
        # Check RTT có sẵn (smoothed hoặc raw)
        rtt_ms = flow.rtt_smoothed if flow.rtt_smoothed else flow.rtt_ms
        if rtt_ms is None or rtt_ms <= 0:
            return False
        if not flow.rtt_stable:
            return False
        
        # ✅ CHECK THROUGHPUT STABILITY
        if len(flow.history) >= 3:
            recent_tputs = [h['throughput'] for h in flow.history]
            mean_tput = sum(recent_tputs) / len(recent_tputs)
            variance = sum((t - mean_tput)**2 for t in recent_tputs) / len(recent_tputs)
            cv = (variance ** 0.5) / mean_tput if mean_tput > 0 else 0
            
            if cv > 0.5:  # Coefficient of variation > 50%
                return False
        
        # ✅ DATA THRESHOLD theo loại flow
        min_bytes = LONG_FLOW_BYTES_THRESHOLD if flow.is_long_flow else MIN_FLOW_BYTES
        return flow.total_bytes >= min_bytes

    def detect_phase(self, now, test_start):
        t = now - test_start
        if t < 30:
            return "P1:1-flow"
        elif t < 60:
            return "P2:2-flows"
        elif t < 90:
            return "P3:3-flows"
        elif t < 120:
            return "P4:2-flows"
        else:
            return "P5:1-flow"
        
    def classify_flows_jenks(self, flow_rtts):
        if not flow_rtts:
            return [], []  # ✅ Return empty tuple
        
        # ✅ CLASSIFY TẤT CẢ flows (TCP + UDP)
        rtts = [r for f, r, t, g in flow_rtts]  # ✅ Lấy hết, không filter
        
        # Handle small N
        if len(rtts) == 1:
            return ['med-rtt'], []  # ✅ Return tuple
        elif len(rtts) == 2:
            classifications = ['low-rtt', 'high-rtt'] if rtts[0] < rtts[1] else ['high-rtt', 'low-rtt']
            return classifications, []
        
        # Jenks for N >= 3 (giữ nguyên logic cũ nhưng dùng `rtts` thay vì `tcp_flows`)
        if JENKSPY_AVAILABLE:
            try:
                K = min(3, len(rtts))
                breaks = jenkspy.jenks_breaks(rtts, n_classes=K)
                queue_ranges = []
                for i in range(K):
                    queue_ranges.append({
                        'queue_id': i + 1,
                        'rtt_min': breaks[i],
                        'rtt_max': breaks[i + 1],
                        'label': ['low-rtt', 'med-rtt', 'high-rtt'][i]
                    })
                
                classifications = []
                for rtt in rtts:  # ✅ Dùng rtts (TẤT CẢ flows)
                    if K == 3:
                        if rtt <= breaks[1]:
                            classifications.append('low-rtt')
                        elif rtt <= breaks[2]:
                            classifications.append('med-rtt')
                        else:
                            classifications.append('high-rtt')
                    elif K == 2:
                        classifications.append('low-rtt' if rtt <= breaks[1] else 'high-rtt')
                    else:
                        classifications.append('med-rtt')
                return classifications, queue_ranges
            except:
                pass
        
        # Quantile fallback
        return self.classify_flows_quantile(flow_rtts)
    
    def classify_flows_quantile(self, flow_rtts):
        """Fallback: Quantile-based classification"""
        rtts = [r[1] for r in flow_rtts]
        sorted_rtts = sorted(rtts)
        q33 = sorted_rtts[len(sorted_rtts) // 3]
        q66 = sorted_rtts[2 * len(sorted_rtts) // 3]
        
        # ✅ TẠO QUEUE RANGES
        queue_ranges = [
            {'queue_id': 1, 'rtt_min': 0, 'rtt_max': q33, 'label': 'low-rtt'},
            {'queue_id': 2, 'rtt_min': q33, 'rtt_max': q66, 'label': 'med-rtt'},
            {'queue_id': 3, 'rtt_min': q66, 'rtt_max': 999, 'label': 'high-rtt'},
        ]
        
        classifications = [
            'low-rtt' if rtt <= q33 else
            'med-rtt' if rtt <= q66 else
            'high-rtt'
            for rtt in rtts
        ]
    
        return classifications, queue_ranges  # RETURN TUPLE
    
    def compute_hierarchical_wmms(self, flow_data, capacity_bps):
        """
        Paper Section III.D - Equation 1:
        Ci = (rttwi / Σrttwj) × C
        where higher RTT → higher weight → MORE bandwidth
        """
        if not flow_data:
            return {}
        
        print("\n" + "="*70)
        print("WMMS ALLOCATION (Paper Section III.D - Equation 1)")
        print("="*70)
        
        # Extract RTTs and throughputs
        flow_rtts = {}
        flow_demands = {}
        
        for fid, rtt_ms, throughput_bps, group in flow_data:
            flow_rtts[fid] = rtt_ms
            flow_demands[fid] = throughput_bps
        
        # PAPER EQUATION 1: Weight = RTT (higher RTT → more weight)
        total_rtt_weight = sum(flow_rtts.values())
        
        allocations = {}
        
        for fid in flow_rtts.keys():
            # Ci = (rttwi / Σrttwj) × C
            weight_i = flow_rtts[fid]
            fair_share = (weight_i / total_rtt_weight) * capacity_bps
            
            allocations[fid] = fair_share
            
            print(f"  Flow {fid}: RTT={flow_rtts[fid]:.2f}ms → "
                f"weight={weight_i:.2f} → {fair_share/1e6:.2f}Mbps")
        
        total = sum(allocations.values())
        print(f"\nTotal: {total/1e6:.2f} / {capacity_bps/1e6:.2f} Mbps")
        print("="*70 + "\n")
        
        return allocations
    
    def _wmms_paper_compliant(self, entities, weights, demands, capacity):
        """
        Paper equation 1: Ci = (wi / Σwj) × C
        
        ✅ FORCE REDISTRIBUTE FULL CAPACITY (không để idle bandwidth)
        """
        allocations = {e: 0.0 for e in entities}
        
        if not entities:
            return allocations
        
        total_weight = sum(weights.values())
        
        if total_weight < 1e-9:
            # Equal split if no weights
            share = capacity / len(entities)
            return {e: share for e in entities}
        
        # ✅ STEP 1: INITIAL ALLOCATION theo RTT weight
        for e in entities:
            # Paper Equation 1: Ci = (wi / Σwj) × C
            allocations[e] = (weights[e] / total_weight) * capacity
        
        # ✅ STEP 2: CAP flows that demand less than allocation
        remaining = 0.0
        satisfied = set()
        
        for e in entities:
            if demands[e] < allocations[e]:
                # Flow không cần nhiều → chỉ cho demand
                remaining += allocations[e] - demands[e]
                allocations[e] = demands[e]
                satisfied.add(e)
        
        # ✅ STEP 3: REDISTRIBUTE remaining bandwidth to unsatisfied flows
        max_iterations = 10
        for iteration in range(max_iterations):
            if remaining < 1e-6:
                break
            
            unsatisfied = [e for e in entities if e not in satisfied]
            if not unsatisfied:
                break
            
            # Redistribute proportional to weight
            unsatisfied_weight = sum(weights[e] for e in unsatisfied)
            
            if unsatisfied_weight < 1e-9:
                # Equal split
                extra = remaining / len(unsatisfied)
                for e in unsatisfied:
                    allocations[e] += extra
                break
            
            any_capped = False
            for e in unsatisfied:
                extra = (weights[e] / unsatisfied_weight) * remaining
                
                if demands[e] < allocations[e] + extra:
                    # Flow will be satisfied
                    actual_extra = demands[e] - allocations[e]
                    allocations[e] = demands[e]
                    remaining -= actual_extra
                    satisfied.add(e)
                    any_capped = True
                else:
                    allocations[e] += extra
            
            if not any_capped:
                # All unsatisfied flows got their share
                remaining = 0
                break
        
        return allocations
        
                
    def compute_jains_fairness_index(self, rates):
        if not rates:
            return 0.0
        n = len(rates)
        sum_rates = sum(rates)
        sum_squared_rates = sum(r ** 2 for r in rates)
        if sum_squared_rates == 0:
            return 0.0
        return (sum_rates ** 2) / (n * sum_squared_rates)
    
    def apply_ovs_queue_rules(self):
        """
        PAPER-COMPLIANT: Dynamic queue reconfiguration
        """
        if not self.current_test or not self.flow_table:
            return
        
        current_flow_count = len(self.flow_table)
        now = time.time()
        
        # Check minimum interval
        if now - self.last_reconfig_time < MIN_RECONFIG_INTERVAL:
            return
        
        # Get stable flows
        stable_flows = {
            fid: data
            for fid, data in self.flow_table.items()
            if self.is_flow_stable(data)
        }
        
        if len(stable_flows) < 1:
            return
        
        # Check triggers
        flow_count_changed = (current_flow_count != self.last_flow_count)
        
        allocation_changed = False
        if self.last_allocation:
            for fid, data in stable_flows.items():
                if fid in self.last_allocation:
                    old_rate = self.last_allocation[fid]
                    new_rate = data.fair_rate_bps
                    
                    if old_rate > 0:
                        change_ratio = abs(new_rate - old_rate) / old_rate
                        if change_ratio > FLOW_CHANGE_THRESHOLD:
                            allocation_changed = True
                            break
        
        should_reconfig = (
            flow_count_changed or 
            allocation_changed or 
            len(self.last_allocation) == 0
        )
        
        if not should_reconfig:
            return
        
        print(f"\n[OVS] {'='*70}")
        print(f"[OVS] DYNAMIC RECONFIGURATION TRIGGERED")
        print(f"[OVS] {'='*70}\n")
        
        flow_allocations = {}
        unlearned_flows = []
        for fid, data in stable_flows.items():
            # Get from pre-computed static map
            flow_info = self.static_flow_map.get(fid)
            # SKIP nếu chưa học xong 5-tuple
            if not flow_info or not flow_info.get('learned'):
                unlearned_flows.append(fid)
                continue
            
            flow_key = flow_info['flow_key']
            queue_id = self.ovs_manager.queue_map.get(data.group, 5)
            rtt_ms = data.get_stable_rtt_ms() or 0.01
            
            flow_allocations[flow_key] = {
                'queue': queue_id,
                'bw_mbps': data.fair_rate_bps / 1_000_000,
                'rtt_ms': rtt_ms,
                'group': data.group
            }
        if unlearned_flows:
            print(f"[OVS] ⏳ Waiting to learn 5-tuple for FIDs: {unlearned_flows}")
            
        if not flow_allocations:
            print(f"[OVS] ⏸️  No flows ready for config yet")
            return
        
        if self.ovs_manager.apply_queue_config(flow_allocations, self.flow_table):
            self.last_flow_count = current_flow_count
            self.last_reconfig_time = now
            self.last_allocation = {
                fid: data.fair_rate_bps 
                for fid, data in stable_flows.items()
            }
            print(f"[OVS] ✓ Reconfiguration successful\n")
        else:
            print(f"[OVS] ✗ Reconfiguration failed\n")
    
    def apply_elephant_cap(self, allocations):
        """
        Elephant flows are allowed redistribution 
        but their rate is capped to a max fair share
        """
        if not allocations:
            return allocations
        
        C = BOTTLENECK_CAPACITY_MBPS * 1_000_000
        # Identify elephant flows
        elephant_fids = [
            fid for fid, data in self.flow_table.items()
            if data.is_long_flow and fid in allocations
        ]
        if not elephant_fids:
            return allocations
        # Max share per elephant = link capacity / number of elephants
        max_elephant_share = C / len(elephant_fids)
        # Cap each elephant flow
        for fid in elephant_fids:
            if allocations[fid] > max_elephant_share:
                print(f"[ELEPHANT CAP] Flow {fid}: {allocations[fid]/1e6:.2f} → {max_elephant_share/1e6:.2f} Mbps")
                allocations[fid] = max_elephant_share
        return allocations
    
    def periodic_processing(self):
        """
        Process digests (Paper Section III - Figure 2)
        """
        print("[PERIODIC] Started (Paper Section III)")
        
        while self.running:
            try:
                self.monitor_test_file()
                digest_list = self.switch.DigestList(timeout=1.0)
                
                if digest_list is None:
                    time.sleep(0.1)
                    continue
                
                print(f"\n[DIGEST] ✓ Received {len(digest_list)} flow records")
                
                # ========== PROCESS EACH DIGEST ==========
                for flow_data in digest_list:
                    # Parse digest struct
                    members = flow_data.struct.members
                    
                    flow_hash        = int.from_bytes(members[0].bitstring, byteorder='big')
                    bytes_interval   = int.from_bytes(members[1].bitstring, byteorder='big')
                    packets_interval = int.from_bytes(members[2].bitstring, byteorder='big')
                    rtt_last_us      = int.from_bytes(members[3].bitstring, byteorder='big')
                    rtt_samples      = int.from_bytes(members[4].bitstring, byteorder='big')
                    is_long_flow     = int.from_bytes(members[5].bitstring, byteorder='big')
                    timestamp_us     = int.from_bytes(members[6].bitstring, byteorder='big')
                    protocol         = int.from_bytes(members[7].bitstring, byteorder='big')
                    
                    # Skip if no data
                    if bytes_interval == 0:
                        continue
                    
                    if flow_hash not in self.static_flow_map:
                        self._learn_flow_from_digest(flow_hash, timestamp_us)
                    
                    # Use switch timestamp (same clock as bytes_interval)
                    prev_ts_us = self.prev_timestamp_us.get(flow_hash, None)
                    
                    if prev_ts_us is None:
                        # First sample: assume 1s interval (digest config)
                        delta_t = 1.0
                    else:
                        # Compute delta using switch clock
                        delta_t = max((timestamp_us - prev_ts_us) / 1e6, 0.0001)  # us → s
                    
                    throughput_bps = (bytes_interval * 8) / delta_t
                    
                    # Store current timestamp for next sample
                    self.prev_timestamp_us[flow_hash] = timestamp_us
                    flow_info = self.static_flow_map.get(flow_hash, {})
                    flow_label = flow_info.get('flow_key', f'FID_{flow_hash}')
                    learned_status = "✓" if flow_info.get('learned') else "⏳"

                    print(f"[DBG] {learned_status} {flow_label}: bytes={bytes_interval} "
                        f"delta_t={delta_t:.3f}s → {throughput_bps/1e6:.2f}Mbps "
                        f"RTT={rtt_last_us/1000:.2f}ms")
                                        
                    # ADD DEBUG LOG
                    print(f"[DBG] FID={flow_hash} bytes={bytes_interval} "
                        f"delta_t={delta_t:.3f}s → {throughput_bps/1e6:.2f}Mbps "
                        f"RTT={rtt_last_us/1000:.2f}ms samples={rtt_samples}")
                    
                    # ========== UPDATE FLOW TABLE ==========
                    with self.lock:
                        if flow_hash not in self.flow_table:
                            self.flow_table[flow_hash] = FlowData()
                            if protocol == 17:
                                self.flow_table[flow_hash].group = 'udp'
                            else:
                                self.flow_table[flow_hash].group = 'tcp'

                        flow = self.flow_table[flow_hash]
                        
                        # Update RTT with EWMA 
                        if protocol == 6 and rtt_last_us > 0:  # TCP only
                            rtt_ms = rtt_last_us / 1000.0
                            flow.rtt_ms = rtt_ms
                            flow.rtt_samples = rtt_samples
                            
                            if flow.rtt_smoothed is None:
                                flow.rtt_smoothed = rtt_ms
                            else:
                                flow.rtt_smoothed = (
                                    0.125 * rtt_ms +
                                    0.875 * flow.rtt_smoothed
                                )
                            
                            if rtt_samples >= 2:
                                flow.rtt_stable = True
                        
                        # Update metrics
                        flow.throughput_bps = throughput_bps
                        flow.total_bytes += bytes_interval
                        flow.packet_count = packets_interval
                        flow.is_long_flow = (is_long_flow == 1)
                        flow.last_digest_time = time.time()
                        flow.update(
                            throughput_bps,
                            flow.rtt_ms,
                            0,
                            bytes_interval,
                            packet_count=packets_interval
                        )
                
                # ========== FAIRNESS COMPUTATION ==========
                #             WMMS allocation
                if not self.current_test or len(self.flow_table) < 1:
                    continue

                with self.lock:
                    # ✅ THU THẬP TẤT CẢ FLOWS (TCP + UDP)
                    all_flows = []
                    
                    for fid, data in self.flow_table.items():
                        # ✅ CHỈ LẤY RTT CHO TCP
                        if data.group == 'udp':
                            rtt_ms = 0  # UDP KHÔNG CÓ RTT
                        elif data.rtt_smoothed is not None and data.rtt_smoothed > 0:
                            rtt_ms = data.rtt_smoothed
                        elif data.rtt_ms is not None and data.rtt_ms > 0:
                            rtt_ms = data.rtt_ms
                        else:
                            rtt_ms = 10.0 + (fid % 3) * 45.0  # Fallback cho TCP
                        
                        throughput_bps = data.throughput_bps
                        group = data.group
                        
                        all_flows.append((fid, rtt_ms, throughput_bps, group))
                    
                    if not all_flows:
                        continue
                    
                    # ✅ TÁCH TCP và UDP để classify
                    tcp_flows = [(f, r, t, g) for f, r, t, g in all_flows if g != 'udp']
                    udp_flows = [(f, r, t, g) for f, r, t, g in all_flows if g == 'udp']
                    
                    # ========== JENKS CHỈ CHO TCP ==========
                    if len(tcp_flows) >= 2:
                        result = self.classify_flows_jenks(tcp_flows)
                        if isinstance(result, tuple):
                            classifications, queue_ranges = result
                            if queue_ranges:
                                self.ovs_manager.update_queue_ranges(queue_ranges)
                        else:
                            classifications = result
                        
                        for i, (fid, _, _, _) in enumerate(tcp_flows):
                            self.flow_table[fid].group = classifications[i]
                    elif len(tcp_flows) == 1:
                        self.flow_table[tcp_flows[0][0]].group = 'med-rtt'
                    
                    # ========== WEIGHTED MAX-MIN FAIR SHARE ==========
                    capacity_bps = BOTTLENECK_CAPACITY_MBPS * 1_000_000

                    # ✅ STEP 1: UDP ALLOCATION
                    UDP_CAP_MBPS = 10
                    udp_capacity_bps = UDP_CAP_MBPS * 1_000_000

                    udp_allocations = {}
                    total_udp_used = 0

                    if udp_flows:
                        n_udp = len(udp_flows)
                        
                        # ✅ LOGIC: UDP alone → full capacity, UDP+TCP → capped
                        if not tcp_flows:
                            # ✅ UDP ALONE: Give FULL remaining bandwidth
                            udp_available = capacity_bps
                            udp_per_flow = udp_available / n_udp
                            
                            for fid, rtt_ms, throughput_bps, group in udp_flows:
                                # Force full allocation (UDP không bị throttle bởi default queue)
                                alloc = udp_per_flow
                                udp_allocations[fid] = alloc
                                total_udp_used += alloc
                                
                                self.flow_table[fid].fair_rate_bps = alloc
                                self.flow_table[fid].group = 'udp'
                            
                            print(f"[UDP ALONE] {n_udp} flows: Allocated {total_udp_used/1e6:.2f}Mbps "
                                f"(FULL CAPACITY, no competition)")
                        else:
                            # ✅ UDP + TCP: Apply CAP to prevent UDP abuse
                            udp_per_flow_cap = udp_capacity_bps / n_udp
                            
                            for fid, rtt_ms, throughput_bps, group in udp_flows:
                                # Cap at max allowed share
                                alloc = min(udp_per_flow_cap, capacity_bps / n_udp)
                                udp_allocations[fid] = alloc
                                total_udp_used += alloc
                                
                                self.flow_table[fid].fair_rate_bps = alloc
                                self.flow_table[fid].group = 'udp'
                            
                            print(f"[UDP+TCP] {n_udp} UDP flows: Capped at {total_udp_used/1e6:.2f}Mbps "
                                f"(CAP={UDP_CAP_MBPS}Mbps to prevent abuse)")
                        
                        tcp_capacity_bps = capacity_bps - total_udp_used
                    else:
                        tcp_capacity_bps = capacity_bps

                    # ✅ STEP 2: TCP WMMS
                    tcp_allocations = {}
                    if tcp_flows:
                        tcp_weights = {fid: rtt_ms for fid, rtt_ms, _, _ in tcp_flows}
                        tcp_demands = {fid: tcp_capacity_bps for fid, _, _, _ in tcp_flows}
                        
                        tcp_allocations = self._wmms_paper_compliant(
                            list(tcp_weights.keys()),
                            tcp_weights,
                            tcp_demands,
                            tcp_capacity_bps
                        )
                        
                        for fid, alloc in tcp_allocations.items():
                            self.flow_table[fid].fair_rate_bps = alloc

                    # ✅ COMBINE
                    allocations = {**udp_allocations, **tcp_allocations}

                    # ✅ DEBUG OUTPUT
                    print("\n" + "="*70)
                    print("HIERARCHICAL WMMS (Paper Section III.D + IV.D)")
                    print("="*70)

                    # ✅ COMPUTE WEIGHTS (for display only)
                    total_capacity_allocated = sum(allocations.values())

                    if udp_flows:
                        print(f"UDP TIER: {len(udp_flows)} flows")
                        for fid, _, _, _ in udp_flows:
                            flow_info = self.static_flow_map.get(fid, {})
                            flow_key = flow_info.get('flow_key', f'FID_{fid}')
                            alloc = allocations[fid]
                            
                            # ✅ Weight % = allocation / total
                            weight_percent = (alloc / total_capacity_allocated) * 100
                            
                            if tcp_flows:
                                # With competition
                                print(f"  UDP      {flow_key:30s}: {alloc/1e6:5.2f}Mbps "
                                    f"({weight_percent:5.1f}%) [CAPPED at {UDP_CAP_MBPS}Mbps]")
                            else:
                                # Alone
                                print(f"  UDP      {flow_key:30s}: {alloc/1e6:5.2f}Mbps "
                                    f"({weight_percent:5.1f}%) [FULL CAPACITY]")

                    if tcp_flows:
                        print(f"\nTCP TIER: {len(tcp_flows)} flows | CAPACITY={tcp_capacity_bps/1e6:.2f}Mbps")
                        total_tcp_weight = sum(tcp_weights.values())
                        
                        for fid, rtt_ms, _, _ in tcp_flows:
                            flow_info = self.static_flow_map.get(fid, {})
                            flow_key = flow_info.get('flow_key', f'FID_{fid}')
                            alloc = allocations[fid]
                            
                            # ✅ RTT weight %
                            rtt_weight_percent = (rtt_ms / total_tcp_weight) * 100
                            
                            # ✅ Bandwidth %
                            bw_weight_percent = (alloc / total_capacity_allocated) * 100
                            
                            print(f"  {self.flow_table[fid].group.upper():8s} {flow_key:30s}: "
                                f"RTT={rtt_ms:6.2f}ms ({rtt_weight_percent:5.1f}% RTT) "
                                f"→ {alloc/1e6:5.2f}Mbps ({bw_weight_percent:5.1f}% BW)")

                    total_allocated = sum(allocations.values())
                    utilization = (total_allocated / capacity_bps) * 100

                    print(f"\n{'─'*70}")
                    print(f"TOTAL: {total_allocated/1e6:.2f} / {capacity_bps/1e6:.2f} Mbps ({utilization:.1f}%)")
                    print("="*70 + "\n")
                    # ========== CLEANUP STALE FLOWS ==========
                    FLOW_TIMEOUT = 3.0
                    now = time.time()
                    
                    stale_flows = []
                    for fid, flow in list(self.flow_table.items()):
                        if now - flow.last_digest_time > FLOW_TIMEOUT:
                            stale_flows.append(fid)
                    
                    for fid in stale_flows:
                        flow_info = self.static_flow_map.get(fid, {})
                        flow_key = flow_info.get('flow_key', f'FID_{fid}')
                        print(f"[CLEANUP] Removing stale flow: {flow_key}")
                        del self.flow_table[fid]
                    
                    # ========== JFI COMPUTATION ==========
                    observed_rates = [
                        data.throughput_bps
                        for fid, data in self.flow_table.items()
                        if fid in self.static_flow_map
                    ]
                    jfi = self.compute_jains_fairness_index(observed_rates)
                    
                    # Log to CSV
                    if self.csv_logger and self.current_test:
                        self.csv_logger.log_metrics(
                            self.flow_table,
                            self.static_flow_map,
                            jfi
                        )
                    
                    tcp_count = len(tcp_flows)
                    udp_count = len(udp_flows)
                    print(f"[FAIRNESS] TCP={tcp_count}, UDP={udp_count} | JFI={jfi:.4f}")
                    
                    # Apply OVS queue rules
                    self.apply_ovs_queue_rules()
            
            except Exception as e:
                if self.running:
                    print(f"[ERROR] {e}")
                    import traceback
                    traceback.print_exc()
        
        print("[PERIODIC] Stopped")

    def run(self):
        print("CONTROL PLANE STARTED ")
        print("Waiting for traffic...\n")
        
        try:
            # Chỉ cần keep-alive, các thread khác làm việc
            while self.running:
                time.sleep(1.0)
        
        except KeyboardInterrupt:
            print("\n\n✓ Shutting down...")
        
        finally:
            self.running = False
            if self.csv_logger:
                self.csv_logger.close()
            print("Control plane stopped")

# ==================== Main ====================
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python3 control_plane_PDP.py <p4info_file> <bmv2_json_file>")
        print("Example: python3 control_plane_PDP.py switch_PDP.p4info.txtpb switch_PDP.json")
        sys.exit(1)
    
    p4info_file = sys.argv[1]
    bmv2_json_file = sys.argv[2]
    
    if not os.path.exists(p4info_file):
        print(f"Error: P4Info file not found: {p4info_file}")
        sys.exit(1)
    
    if not os.path.exists(bmv2_json_file):
        print(f"Error: BMv2 JSON file not found: {bmv2_json_file}")
        sys.exit(1)
    
    controller = PDPController(p4info_file, bmv2_json_file)
    controller.run()