#!/usr/bin/env python3

import argparse
import time
import os
import sys
import subprocess

CURRENT_TEST_FILE = "current_test.txt"
LOGS_DIR = "logs"

def write_current_test(test_id):
    """Write current test ID to file for controller to read"""
    try:
        with open(CURRENT_TEST_FILE, 'w') as f:
            f.write(test_id)
        print(f" Signaled controller: test={test_id}\n")
    except Exception as e:
        print(f"Warning: Could not write test file: {e}")

def get_host_pid(host_name):
    """Get PID of a Mininet host"""
    try:
        result = subprocess.run(
            f"ps aux | grep 'mininet:{host_name}' | grep bash | grep -v grep | awk '{{print $2}}'",
            shell=True, capture_output=True, text=True
        )
        pid = result.stdout.strip()
        return pid if pid else None
    except:
        return None

def run_cmd_on_host(host_name, cmd):
    """Execute command on a Mininet host using nsenter"""
    pid = get_host_pid(host_name)
    if not pid:
        print(f"Warning: Could not find PID for {host_name}")
        return False
    
    full_cmd = f"nsenter -t {pid} -n -m {cmd}"
    subprocess.Popen(full_cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return True

def stop_all_iperf():
    """Kill all iperf processes on all hosts"""
    print("\n Stopping all iperf processes...")
    for i in range(1, 7):
        host = f"h{i}"
        run_cmd_on_host(host, "killall -9 iperf iperf3 2>/dev/null")
    time.sleep(1)
    print(" All iperf processes stopped\n")

def start_iperf_servers():
    """Start iperf servers on receiver hosts h4-h6"""
    print(" Starting iperf servers on h4-h6...")
    for i in range(4, 7):
        host = f"h{i}"
        # TCP server
        run_cmd_on_host(host, "iperf -s -p 5201 > /dev/null 2>&1 &")
        # UDP server
        run_cmd_on_host(host, "iperf -s -u -p 5201 > /dev/null 2>&1 &")
    time.sleep(2)
    print("✓ Servers started\n")
    
    
# ==================== TEST TB1 ====================
def run_tb1():
    """
    TB1: RTT Unfairness Test (150s duration)
    """
    print("\n" + "="*80)
    print("TEST TB1: RTT UNFAIRNESS - 3 TCP Flows (150s)")
    print("="*80 + "\n")

    write_current_test("tb1")
    
    # Flow 1: SHORT RTT (10ms) - runs entire 150s
    print(f"t=0s:   ✓ Flow 1 started (h1→h4, RTT=10ms, duration=150s)")
    run_cmd_on_host("h1", "iperf -c 10.0.1.1 -p 5201 -w 4M -t 150 > /dev/null 2>&1 &")
    
    print(f"        Waiting 30s... (Flow 1 alone)")
    time.sleep(30)
    
    # Flow 2: MEDIUM RTT (50ms) - runs 90s (30s to 120s)
    print(f"t=30s:  ✓ Flow 2 started (h2→h5, RTT=50ms, duration=90s)")
    run_cmd_on_host("h2", "iperf -c 10.0.1.2 -p 5201 -w 4M -t 90 > /dev/null 2>&1 &")
    
    print(f"        Waiting 30s... (Flow 1 + Flow 2)")
    time.sleep(30)
    
    # Flow 3: HIGH RTT (100ms) - runs 30s (60s to 90s)
    print(f"t=60s:  ✓ Flow 3 started (h3→h6, RTT=100ms, duration=30s)")
    run_cmd_on_host("h3", "iperf -c 10.0.1.3 -p 5201 -w 4M -t 30 > /dev/null 2>&1 &")
    
    print(f"        Waiting 30s... (3-way competition)")
    time.sleep(30)
    
    print(f"t=90s:  ✓ Flow 3 finished")
    print(f"        Waiting 30s... (Flow 1 + Flow 2)")
    time.sleep(30)
    
    print(f"t=120s: ✓ Flow 2 finished")
    print(f"        Waiting 30s... (Flow 1 alone)")
    time.sleep(30)
    
    print(f"t=150s: ✓ Flow 1 finished\n")
    
    stop_all_iperf()
    write_current_test("stop")
    
    print("="*80)
    print("TB1 COMPLETED!")
    print("="*80 + "\n")

# ==================== TEST TB2 ====================
def run_tb2():
    """
    TB2: UDP vs TCP Fairness Test (150s duration)
    """
    print("\n" + "="*80)
    print("TEST TB2: UDP vs TCP - 1 UDP + 2 TCP (150s)")
    print("="*80 + "\n")

    write_current_test("tb2")
    
    # Flow 1: UDP (10ms RTT) - runs entire 150s
    print(f"t=0s:   ✓ UDP Flow started (h1, 10Mbps, duration=150s)")
    run_cmd_on_host("h1", "iperf -c 10.0.1.1 -u -p 5201 -b 10M -t 150 > /dev/null 2>&1 &")
    
    print(f"        Waiting 30s... (UDP alone)")
    time.sleep(30)
    
    # Flow 2: TCP (50ms RTT) - runs 90s (30s to 120s)
    print(f"t=30s:  ✓ TCP Flow 2 started (h2, RTT=50ms, duration=90s)")
    run_cmd_on_host("h2", "iperf -c 10.0.1.2 -p 5201 -w 4M -t 90 > /dev/null 2>&1 &")
    
    print(f"        Waiting 30s... (UDP + TCP)")
    time.sleep(30)
    
    # Flow 3: TCP (100ms RTT) - runs 30s (60s to 90s)
    print(f"t=60s:  ✓ TCP Flow 3 started (h3, RTT=100ms, duration=30s)")
    run_cmd_on_host("h3", "iperf -c 10.0.1.3 -p 5201 -w 4M -t 30 > /dev/null 2>&1 &")
    
    print(f"        Waiting 30s... (UDP+TCP+TCP)")
    time.sleep(30)
    
    print(f"t=90s:  ✓ Flow 3 finished")
    print(f"        Waiting 30s... (UDP + TCP)")
    time.sleep(30)
    
    print(f"t=120s: ✓ Flow 2 finished")
    print(f"        Waiting 30s... (UDP alone)")
    time.sleep(30)
    
    print(f"t=150s: ✓ All flows finished\n")
    
    stop_all_iperf()
    write_current_test("stop")
    
    print("="*80)
    print("TB2 COMPLETED!")
    print("="*80 + "\n")


# ==================== MAIN ====================
def main():
    if os.geteuid() != 0:
        print("Error: This script must be run as root (sudo)")
        print("Usage: sudo python3 traffic.py --test tb1")
        sys.exit(1)
    
    parser = argparse.ArgumentParser(
        description='PDP Fairness Traffic Generator (3 Flows, 20 Mbps)'
    )
    parser.add_argument(
        '--test',
        required=True,
        choices=['tb1', 'tb2','all'],
        help='Test scenario to run'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("PDP FAIRNESS TRAFFIC GENERATOR")
    print("="*80)
    print(f"Test: {args.test.upper()}")
    print(f"Topology: 3 flows, 20 Mbps bottleneck")
    print("="*80)
    
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    stop_all_iperf()
    start_iperf_servers()
    
    if args.test == 'tb1':
        run_tb1()
    elif args.test == 'tb2':
        run_tb2()
    elif args.test == 'all':
        print("\n" + "="*80)
        print("RUNNING ALL TESTS SEQUENTIALLY")
        print("="*80 + "\n")
        
        run_tb1()
        time.sleep(5)
        
        start_iperf_servers()
        run_tb2()
        time.sleep(5)
        
        start_iperf_servers()
        run_tb3()
    
    print("\n" + "="*80)
    print("ALL TESTS COMPLETED!")
    print("="*80)
    print(f"\nCheck logs in: {LOGS_DIR}/")
    print("="*80 + "\n")

if __name__ == '__main__':
    main()