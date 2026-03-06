#!/usr/bin/env python3

import os
from mininet.net import Mininet
from mininet.node import Controller, OVSSwitch
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import subprocess
import time
import shutil
import sys


class PDPTopology:
    def __init__(self):
        self.net = None
        self.veth_mirror = "veth-mirror"
        self.p4_veth = "veth-p4"
        self.p4_veth_peer = "veth-p4-peer"
        
        self.router_mac = "aa:bb:cc:dd:ee:ff"
        self.router_ip_left = "10.0.0.254"
        self.router_ip_right = "10.0.1.254"
        self.host_macs = {}
        
        self.observation_point = None
        
    def get_link_interfaces(self, node_a, node_b):
        """Return (intf_a, intf_b) for the link between node_a and node_b"""
        for intf in node_a.intfList():
            if intf.link:
                other = intf.link.intf1 if intf.link.intf1.node != node_a else intf.link.intf2
                if other.node == node_b:
                    return intf.name, other.name
        return None, None
    
    def check_required_tools(self):
        """Check if required system tools are available"""
        required_tools = ['ethtool', 'tc', 'brctl', 'ip']
        missing = []
        
        for tool in required_tools:
            if not shutil.which(tool):
                missing.append(tool)
        
        if missing:
            info(f"\nERROR: Missing required tools: {', '.join(missing)}\n")
            return False
        return True

    def create_p4_veth(self):
        """Create veth pair for P4 switch"""
        info("*** Creating veth pair for P4 switch\n")
        
        subprocess.run(["ip", "link", "del", self.p4_veth],
                      stderr=subprocess.DEVNULL)
        time.sleep(0.3)
        
        subprocess.run([
            "ip", "link", "add", self.p4_veth,
            "type", "veth",
            "peer", "name", self.p4_veth_peer
        ], check=True)
        
        subprocess.run(["ip", "link", "set", self.p4_veth, "up"], check=True)
        subprocess.run(["ip", "link", "set", self.p4_veth_peer, "up"], check=True)
        subprocess.run(["ip", "link", "set", self.p4_veth, "promisc", "on"], check=True)
        
        for iface in [self.p4_veth, self.p4_veth_peer]:
            subprocess.run([
                "ethtool", "-K", iface,
                "tx", "off", "rx", "off", "sg", "off",
                "tso", "off", "gso", "off", "gro", "off",
                "lro", "off", "ufo", "off"
            ], stderr=subprocess.DEVNULL)
        
        info(f"*** Veth pair: {self.p4_veth} <-> {self.p4_veth_peer}\n")

    def create_veth_pair(self):
        """Create veth pair for traffic mirroring"""
        info("*** Creating veth pair for TC mirroring\n")
        
        subprocess.run(["ip", "link", "del", self.veth_mirror], stderr=subprocess.DEVNULL)
        subprocess.run(["ip", "link", "del", "veth-tap"], stderr=subprocess.DEVNULL)
        time.sleep(0.5)

        subprocess.run([
            "ip", "link", "add", self.veth_mirror,
            "type", "veth", "peer", "name", "veth-tap"
        ], check=True)

        subprocess.run(["ip", "link", "set", self.veth_mirror, "up"], check=True)
        subprocess.run(["ip", "link", "set", "veth-tap", "up"], check=True)
        
        subprocess.run(["ip", "link", "set", self.veth_mirror, "promisc", "on"], check=True)
        subprocess.run(["ip", "link", "set", "veth-tap", "promisc", "on"], check=True)
        
        for iface in [self.veth_mirror, "veth-tap"]:
            subprocess.run([
                "ethtool", "-K", iface,
                "tx", "off", "rx", "off", "sg", "off",
                "tso", "off", "gso", "off", "gro", "off",
                "lro", "off", "ufo", "off"
            ], stderr=subprocess.DEVNULL)

        info(f"*** Veth pair: {self.veth_mirror} <-> veth-tap\n")

    def build_topology(self):
        """Build topology với RTT diversity để test RTT unfairness"""
        info("\n" + "="*70 + "\n")
        info("*** Building topology: 3 flows with RTT diversity\n")
        info("="*70 + "\n")

        self.net = Mininet(
            controller=Controller,
            switch=OVSSwitch,
            link=TCLink,
            autoSetMacs=False,
            autoStaticArp=False
        )

        if shutil.which("controller"):
            self.net.addController("c0")

        s1 = self.net.addSwitch("s1", protocols="OpenFlow13", failMode="standalone")
        s2 = self.net.addSwitch("s2", protocols="OpenFlow13", failMode="standalone")
        s3 = self.net.addSwitch("s3", protocols="OpenFlow13", failMode="standalone")

        # ==================== SENDERS với RTT KHÁC NHAU ====================
        # Base RTT từ s1-s2-s3 links = 2 × 5ms = 10ms
        # Thêm delay vào host links để tạo RTT diversity
        
        # h1: LOW RTT - "Local" connection
        ip1 = "10.0.0.1"
        mac1 = "00:00:00:00:00:01"
        h1 = self.net.addHost("h1", ip=f"{ip1}/24", mac=mac1,
                            defaultRoute=f"via {self.router_ip_left}")
        self.host_macs[ip1] = mac1
        # NO extra delay → Total RTT = 10ms (base only)
        self.net.addLink(h1, s1, bw=100, delay="0ms", use_htb=True)
        info(f"  h1 (10.0.0.1): delay=0ms  → RTT ≈ 10ms  (local)\n")
        
        # h2: MEDIUM RTT - "Cross-country" connection  
        ip2 = "10.0.0.2"
        mac2 = "00:00:00:00:00:02"
        h2 = self.net.addHost("h2", ip=f"{ip2}/24", mac=mac2,
                            defaultRoute=f"via {self.router_ip_left}")
        self.host_macs[ip2] = mac2
        # Add 20ms each way → Total RTT = 10ms + 2×20ms = 50ms
        self.net.addLink(h2, s1, bw=100, delay="20ms", use_htb=True)
        info(f"  h2 (10.0.0.2): delay=20ms → RTT ≈ 50ms  (cross-country)\n")
        
        # h3: HIGH RTT - "Intercontinental" connection
        ip3 = "10.0.0.3"
        mac3 = "00:00:00:00:00:03"
        h3 = self.net.addHost("h3", ip=f"{ip3}/24", mac=mac3,
                            defaultRoute=f"via {self.router_ip_left}")
        self.host_macs[ip3] = mac3
        # Add 45ms each way → Total RTT = 10ms + 2×45ms = 100ms
        self.net.addLink(h3, s1, bw=100, delay="45ms", use_htb=True)
        info(f"  h3 (10.0.0.3): delay=45ms → RTT ≈ 100ms (intercontinental)\n")

        # ==================== RECEIVERS (no extra delay) ====================
        for i in range(4, 7):
            ip = f"10.0.1.{i-3}"
            mac = f"00:00:00:00:01:{i-3:02x}"
            h = self.net.addHost(f"h{i}", ip=f"{ip}/24", mac=mac,
                                defaultRoute=f"via {self.router_ip_right}")
            self.host_macs[ip] = mac
            self.net.addLink(h, s3, bw=100, use_htb=True)

        # ==================== BOTTLENECK LINKS ====================
        # Base delay: 5ms each direction
        self.net.addLink(s1, s2, bw=20, delay="5ms", use_htb=True)
        self.net.addLink(s2, s3, bw=20, delay="5ms", use_htb=True)
        
        info("\n" + "-"*70 + "\n")
        info("Topology built successfully\n")
        info("-"*70 + "\n\n")

        return s2
    def start_network(self):
        """Start Mininet network"""
        info("*** Starting Mininet\n")
        self.net.start()
        time.sleep(2)

    def setup_forwarding(self):
        """Install OpenFlow forwarding rules"""
        info("*** Installing forwarding rules\n")

        # ==================== S1, S3: Simple learning ====================
        for sw in ["s1", "s3"]:
            subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", sw])
            subprocess.run([
                "ovs-ofctl", "-O", "OpenFlow13", "add-flow", sw,
                "priority=0,actions=normal"
            ], check=True)

        # ==================== S2: L3 Router with per-flow rules ====================
        subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", "s2"])
        
        subprocess.run([
            "ovs-vsctl", "set", "Bridge", "s2",
            f"other-config:hwaddr={self.router_mac}"
        ])

        s1 = self.net.get('s1')
        s2 = self.net.get('s2')
        s3 = self.net.get('s3')

        s2_to_s1, _ = self.get_link_interfaces(s2, s1)
        s2_to_s3, _ = self.get_link_interfaces(s2, s3)

        port_s1 = s2.ports[s2.intf(s2_to_s1)]
        port_s3 = s2.ports[s2.intf(s2_to_s3)]

        # ✅ PER-FLOW RULES for monitoring (priority 150)
        # These will be OVERRIDDEN by control plane's queue rules (priority 200)
        
        info("*** Installing per-flow monitoring rules on s2\n")
        
        flows_to_monitor = [
            ("10.0.0.1", "10.0.1.1", 5201),  # h1 -> h4
            ("10.0.0.2", "10.0.1.2", 5201),  # h2 -> h5
            ("10.0.0.3", "10.0.1.3", 5201),  # h3 -> h6
        ]
        
        for i, (src_ip, dst_ip, port) in enumerate(flows_to_monitor, 1):
            # Forward direction (client -> server)
            subprocess.run([
                "ovs-ofctl", "-O", "OpenFlow13", "add-flow", "s2",
                f"priority=150,tcp,"
                f"nw_src={src_ip},nw_dst={dst_ip},"
                f"tp_dst={port},"
                f"actions=mod_dl_src:{self.router_mac},"
                f"mod_dl_dst:{self.host_macs[dst_ip]},"
                f"dec_ttl,output:{port_s3}"
            ], check=True)
            
            # Reverse direction (server -> client)
            subprocess.run([
                "ovs-ofctl", "-O", "OpenFlow13", "add-flow", "s2",
                f"priority=150,tcp,"
                f"nw_src={dst_ip},nw_dst={src_ip},"
                f"tp_src={port},"
                f"actions=mod_dl_src:{self.router_mac},"
                f"mod_dl_dst:{self.host_macs[src_ip]},"
                f"dec_ttl,output:{port_s1}"
            ], check=True)
            
            info(f"  Flow {i}: {src_ip}:{port} <-> {dst_ip}:{port}\n")
        
        # ==================== Generic forwarding (priority 100) ====================
        for i in range(1, 4):
            # To servers
            subprocess.run([
                "ovs-ofctl", "-O", "OpenFlow13", "add-flow", "s2",
                f"priority=100,ip,nw_dst=10.0.1.{i},"
                f"actions=mod_dl_src:{self.router_mac},"
                f"mod_dl_dst:{self.host_macs[f'10.0.1.{i}']},"
                f"dec_ttl,output:{port_s3}"
            ], check=True)

            # To clients
            subprocess.run([
                "ovs-ofctl", "-O", "OpenFlow13", "add-flow", "s2",
                f"priority=100,ip,nw_dst=10.0.0.{i},"
                f"actions=mod_dl_src:{self.router_mac},"
                f"mod_dl_dst:{self.host_macs[f'10.0.0.{i}']},"
                f"dec_ttl,output:{port_s1}"
            ], check=True)
        
        info("*** Forwarding rules installed successfully\n")
        info("    - Priority 150: Per-flow monitoring (TCP port 5201)\n")
        info("    - Priority 100: Generic L3 forwarding\n")
        info("    - Priority 200: Will be used by control plane for queues\n")

    def setup_static_arp(self):
        """Configure static ARP entries"""
        info("*** Setting static ARP\n")
        for i in range(1, 4):
            self.net.get(f"h{i}").cmd(f"arp -s {self.router_ip_left} {self.router_mac}")
        for i in range(4, 7):
            self.net.get(f"h{i}").cmd(f"arp -s {self.router_ip_right} {self.router_mac}")
            
    def disable_offloading(self):
        """Disable TCP offloading"""
        info("*** Disabling TCP offloading\n")
        
        for i in range(1, 7):
            self.net.get(f"h{i}").cmd(
                f"ethtool -K h{i}-eth0 tx off rx off sg off "
                f"tso off gso off gro off lro off ufo off 2>/dev/null"
            )

        s1 = self.net.get('s1')
        s2 = self.net.get('s2')
        s3 = self.net.get('s3')

        links = [
            self.get_link_interfaces(s1, s2),
            self.get_link_interfaces(s2, s3)
        ]

        for intf_a, intf_b in links:
            for iface in [intf_a, intf_b]:
                if iface:
                    subprocess.run([
                        "ethtool", "-K", iface,
                        "tx", "off", "rx", "off", "sg", "off",
                        "tso", "off", "gso", "off", "gro", "off",
                        "lro", "off", "ufo", "off"
                    ], stderr=subprocess.DEVNULL)

    def setup_tc_mirroring(self):
        info("\n" + "="*70 + "\n")
        info("*** TC MIRRORING SETUP \n")
        info("="*70 + "\n")

        mirror_if = self.veth_mirror
        
        sw2 = self.net.get('s2')
        sw3 = self.net.get('s3')
        
        # BOTTLENECK = s2 <-> s3
        s2_to_s3, _ = self.get_link_interfaces(sw2, sw3)
        if not s2_to_s3:
            info(" ERROR: Cannot detect s2-eth2, using fallback\n")
            s2_to_s3 = "s2-eth2"
        
        self.observation_point = s2_to_s3
        info(f"*** Bottleneck interface: {s2_to_s3}\n")
        info(f"*** Mirroring BOTH directions to: {mirror_if}\n")
        # remove old qdisc
        subprocess.run(["tc", "qdisc", "del", "dev", s2_to_s3, "clsact"],
                       stderr=subprocess.DEVNULL)
        # add clsact
        subprocess.run(["tc", "qdisc", "add", "dev", s2_to_s3, "clsact"], check=True)
        
        # DATA: client -> server (into queue)
        subprocess.run([
            "tc", "filter", "add",
            "dev", s2_to_s3,
            "ingress",
            "matchall",
            "action", "mirred", "egress", "mirror",
            "dev", mirror_if
        ], check=True)
        
        # ACK: server -> client (leaving queue)
        subprocess.run([
            "tc", "filter", "add",
            "dev", s2_to_s3,
            "egress",
            "matchall",
            "action", "mirred", "egress", "mirror",
            "dev", mirror_if
        ], check=True)
        info("*** TC mirroring configured on bottleneck link\n")
        return True
            

    def bridge_to_p4_interface(self):
        """Bridge veth-tap to veth-p4 for P4 switch"""
        info("*** Bridging monitoring path to P4 switch\n")
        
        subprocess.run(["ip", "link", "del", "br-pdp"], stderr=subprocess.DEVNULL)
        
        subprocess.run(["ip", "link", "add", "br-pdp", "type", "bridge"],
                      stderr=subprocess.DEVNULL)
        
        subprocess.run(["ip", "link", "set", "veth-tap", "master", "br-pdp"], check=True)
        subprocess.run(["ip", "link", "set", self.p4_veth, "master", "br-pdp"], check=True)
        
        subprocess.run(["ip", "link", "set", "veth-tap", "promisc", "on"], check=True)
        subprocess.run(["ip", "link", "set", self.p4_veth, "promisc", "on"], check=True)
        
        subprocess.run(["ip", "link", "set", "br-pdp", "type", "bridge", "ageing_time", "0"],
                      stderr=subprocess.DEVNULL)
        
        subprocess.run(["ip", "link", "set", "br-pdp", "up"], check=True)
        
        subprocess.run([
            "ethtool", "-K", "br-pdp",
            "tx", "off", "rx", "off", "sg", "off",
            "tso", "off", "gso", "off"
        ], stderr=subprocess.DEVNULL)
        
        info(f"*** Bridge: veth-tap <-> br-pdp <-> {self.p4_veth}\n")

    def save_bottleneck_info(self):
        """Save bottleneck configuration"""
        info("*** Saving bottleneck configuration\n")
        
        s2 = self.net.get('s2')
        s3 = self.net.get('s3')
        s2_to_s3, _ = self.get_link_interfaces(s2, s3)
        
        if not s2_to_s3:
            s2_to_s3 = "s2-eth2"
        
        port_num = s2.ports[s2.intf(s2_to_s3)]
        
        config = {
            'bottleneck_interface': s2_to_s3,
            'bottleneck_port': port_num,
            'observation_point': self.observation_point,
            'switch': 's2',
        }
        
        import json
        with open('bottleneck_config.json', 'w') as f:
            json.dump(config, f, indent=2)
        
        info(f"*** Configuration saved to bottleneck_config.json\n")

    def print_system_status(self):
        """Print system status"""
        info("\n" + "="*70 + "\n")
        info("TOPOLOGY READY\n")
        info("="*70 + "\n\n")

    def run(self):
        """Main execution flow"""
        try:
            info("\n*** Checking required tools...\n")
            if not self.check_required_tools():
                sys.exit(1)
            
            self.create_p4_veth()
            self.create_veth_pair()
            self.build_topology()
            self.start_network()
            self.setup_forwarding()
            self.setup_static_arp()
            self.disable_offloading()
            
            mirror_ok = self.setup_tc_mirroring()
            if not mirror_ok:
                info("\n  WARNING: Mirroring incomplete!\n\n")
            
            self.bridge_to_p4_interface()
            self.save_bottleneck_info()
            self.print_system_status()
            
            CLI(self.net)

        except KeyboardInterrupt:
            info("\n*** Interrupted\n")
        except Exception as e:
            info(f"\n Error: {e}\n")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        info("\n*** Cleaning up\n")
        
        if hasattr(self, 'observation_point') and self.observation_point:
            subprocess.run(["tc", "qdisc", "del", "dev", self.observation_point, "clsact"],
                          stderr=subprocess.DEVNULL)
        
        for iface in ["s2-eth1", "s2-eth2"]:
            subprocess.run(["tc", "qdisc", "del", "dev", iface, "clsact"],
                          stderr=subprocess.DEVNULL)
        
        subprocess.run(["ip", "link", "set", "br-pdp", "down"], stderr=subprocess.DEVNULL)
        subprocess.run(["ip", "link", "del", "br-pdp"], stderr=subprocess.DEVNULL)
        
        if self.net:
            self.net.stop()

        subprocess.run(["ip", "link", "del", self.veth_mirror], stderr=subprocess.DEVNULL)
        subprocess.run(["ip", "link", "del", self.p4_veth], stderr=subprocess.DEVNULL)
        
        subprocess.run(["mn", "-c"], stderr=subprocess.DEVNULL)


def main():
    setLogLevel("info")
    PDPTopology().run()


if __name__ == "__main__":
    main()