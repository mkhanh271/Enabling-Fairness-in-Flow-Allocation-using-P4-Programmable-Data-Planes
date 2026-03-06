#include <core.p4>
#include <v1model.p4>

const bit<16> TYPE_IPV4 = 0x800;
const bit<8>  TYPE_TCP  = 6;
const bit<8>  TYPE_UDP  = 17;

const bit<32> LONG_FLOW_THRESHOLD = 10485760;
const bit<32> CMS_WIDTH = 4096;
const bit<32> CMS_DEPTH = 4;

typedef bit<48> macAddr_t;
typedef bit<32> ip4Addr_t;

// ==================== HEADERS ====================
header ethernet_t {
    macAddr_t dstAddr;
    macAddr_t srcAddr;
    bit<16>   etherType;
}

header ipv4_t {
    bit<4>    version;
    bit<4>    ihl;
    bit<8>    diffserv;
    bit<16>   totalLen;
    bit<16>   identification;
    bit<3>    flags;
    bit<13>   fragOffset;
    bit<8>    ttl;
    bit<8>    protocol;
    bit<16>   hdrChecksum;
    ip4Addr_t srcAddr;
    ip4Addr_t dstAddr;
}

header tcp_t {
    bit<16> srcPort;
    bit<16> dstPort;
    bit<32> seqNo;
    bit<32> ackNo;
    bit<4>  dataOffset;
    bit<4>  res;
    bit<8>  flags;
    bit<16> window;
    bit<16> checksum;
    bit<16> urgentPtr;
}

header udp_t {
    bit<16> srcPort;
    bit<16> dstPort;
    bit<16> length;
    bit<16> checksum;
}

// ==================== DIGEST STRUCTURE ====================
// ✅ FIX: Struct phải được khai báo NGOÀI metadata
struct flow_digest_t {
    bit<32> flow_hash;
    bit<32> bytes_interval;
    bit<32> packets_interval;
    bit<32> rtt_last;
    bit<32> rtt_samples;
    bit<8>  is_long_flow;  
    bit<64> timestamp;
    bit<8>  protocol;
}

// ==================== METADATA ====================
struct metadata {
    bit<32> flow_hash;
    bit<32> pkt_len;
    bit<32> payload_len;
    bit<1>  is_data_packet;
    bit<1>  is_ack_packet;
    
    bit<32> cms_hash1;
    bit<32> cms_hash2;
    bit<32> cms_hash3;
    bit<32> cms_hash4;

    bit<1>  is_client_to_server;
}

struct headers {
    ethernet_t ethernet;
    ipv4_t     ipv4;
    tcp_t      tcp;
    udp_t      udp;
}

// ==================== REGISTERS ====================
register<bit<32>>(8192) flow_bytes_interval;
register<bit<32>>(8192) flow_packets_interval;
register<bit<32>>(8192) flow_queue_delay;

register<bit<32>>(4096) cms_counter_row1;
register<bit<32>>(4096) cms_counter_row2;
register<bit<32>>(4096) cms_counter_row3;
register<bit<32>>(4096) cms_counter_row4;

register<bit<32>>(8192) rtt_min_interval;
register<bit<32>>(8192) rtt_max_interval;

register<bit<32>>(8192) flow_total_bytes;
register<bit<1>>(8192)  flow_is_elephant;
register<bit<32>>(1) elephant_flow_count;

register<bit<48>>(8192) flow_last_digest_time;

register<bit<48>>(8192) flow_syn_timestamp;       // SYN timestamp
register<bit<32>>(8192) flow_handshake_rtt_us;    // RTT from handshake
register<bit<32>>(8192) flow_rtt_sample_count;    // 0 or 1
register<bit<48>>(8192) flow_last_data_timestamp;  // Last DATA packet timestamp
register<bit<32>>(8192) flow_rtt_current_us;       // Current RTT from DATA/ACK     


const bit<48> DIGEST_INTERVAL_US = 1000000 ; // 1 second
// TCP Flags
const bit<8> TCP_FLAG_SYN = 0x02;
const bit<8> TCP_FLAG_ACK = 0x10;

// ==================== PARSER ====================
parser MyParser(packet_in packet,
                out headers hdr,
                inout metadata meta,
                inout standard_metadata_t standard_metadata) {
    state start { 
        transition parse_ethernet; 
    }

    state parse_ethernet {
        packet.extract(hdr.ethernet);
        transition select(hdr.ethernet.etherType) {
            TYPE_IPV4: parse_ipv4;
            default: accept;
        }
    }

    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
            TYPE_TCP: parse_tcp;
            TYPE_UDP: parse_udp;
            default: accept;
        }
    }

    state parse_tcp { 
        packet.extract(hdr.tcp); 
        transition accept; 
    }
    
    state parse_udp { 
        packet.extract(hdr.udp); 
        transition accept; 
    }
}

control MyVerifyChecksum(inout headers hdr, inout metadata meta) {
    apply { }
}

// ==================== INGRESS ====================
control MyIngress(inout headers hdr,
                  inout metadata meta,
                  inout standard_metadata_t standard_metadata) {
    
    action compute_flow_hash() {
        bit<16> src_port = 0;
        bit<16> dst_port = 0;

        if (hdr.tcp.isValid()) {
            src_port = hdr.tcp.srcPort;
            dst_port = hdr.tcp.dstPort;
        } else if (hdr.udp.isValid()) {
            src_port = hdr.udp.srcPort;
            dst_port = hdr.udp.dstPort;
        }

        bit<32> ip_a;
        bit<32> ip_b;
        bit<16> port_a;
        bit<16> port_b;
        
        // Subnet-based direction detection
        bit<24> src_net = hdr.ipv4.srcAddr[31:8];
        bit<24> dst_net = hdr.ipv4.dstAddr[31:8];
        
        if (src_net == 0x0A0000 && dst_net == 0x0A0001) {
            // Client → Server
            ip_a = hdr.ipv4.srcAddr;
            ip_b = hdr.ipv4.dstAddr;
            port_a = src_port;
            port_b = dst_port;
            meta.is_client_to_server = 1;
        } else if (src_net == 0x0A0001 && dst_net == 0x0A0000) {
            // Server → Client
            ip_a = hdr.ipv4.dstAddr;
            ip_b = hdr.ipv4.srcAddr;
            port_a = dst_port;
            port_b = src_port;
            meta.is_client_to_server = 0;
        } else {
            // Fallback
            if (hdr.ipv4.srcAddr < hdr.ipv4.dstAddr || 
                (hdr.ipv4.srcAddr == hdr.ipv4.dstAddr && src_port < dst_port)) {
                ip_a = hdr.ipv4.srcAddr;
                ip_b = hdr.ipv4.dstAddr;
                port_a = src_port;
                port_b = dst_port;
                meta.is_client_to_server = 1;
            } else {
                ip_a = hdr.ipv4.dstAddr;
                ip_b = hdr.ipv4.srcAddr;
                port_a = dst_port;
                port_b = src_port;
                meta.is_client_to_server = 0;
            }
        }

        hash(meta.flow_hash, HashAlgorithm.crc32, (bit<32>)0,
            { ip_a, ip_b, port_a, port_b, hdr.ipv4.protocol },
            (bit<32>)8192);

        hash(meta.cms_hash1, HashAlgorithm.crc32, (bit<32>)0,
            { ip_a, ip_b, port_a, port_b, hdr.ipv4.protocol },
            (bit<32>)4096);
        
        hash(meta.cms_hash2, HashAlgorithm.crc32, (bit<32>)1,
            { ip_a, ip_b, port_a, port_b, hdr.ipv4.protocol },
            (bit<32>)4096);
        
        hash(meta.cms_hash3, HashAlgorithm.crc32, (bit<32>)2,
            { ip_a, ip_b, port_a, port_b, hdr.ipv4.protocol },
            (bit<32>)4096);
        
        hash(meta.cms_hash4, HashAlgorithm.crc32, (bit<32>)3,
            { ip_a, ip_b, port_a, port_b, hdr.ipv4.protocol },
            (bit<32>)4096);
    }

    action normalize_5tuple(
        out bit<32> ip_a,
        out bit<32> ip_b,
        out bit<16> port_a,
        out bit<16> port_b
    ){
        bit<16> sp = hdr.tcp.srcPort;
        bit<16> dp = hdr.tcp.dstPort;

        if (hdr.ipv4.srcAddr < hdr.ipv4.dstAddr ||
        (hdr.ipv4.srcAddr == hdr.ipv4.dstAddr && sp < dp)) {
            ip_a = hdr.ipv4.srcAddr;
            ip_b = hdr.ipv4.dstAddr;
            port_a = sp;
            port_b = dp;
        } else {
            ip_a = hdr.ipv4.dstAddr;
            ip_b = hdr.ipv4.srcAddr;
            port_a = dp;
            port_b = sp;
        }
    }

    action cms_update() {
        bit<32> count1;
        bit<32> count2;
        bit<32> count3;
        bit<32> count4;
        
        cms_counter_row1.read(count1, meta.cms_hash1);
        cms_counter_row2.read(count2, meta.cms_hash2);
        cms_counter_row3.read(count3, meta.cms_hash3);
        cms_counter_row4.read(count4, meta.cms_hash4);
        
        cms_counter_row1.write(meta.cms_hash1, count1 + meta.pkt_len);
        cms_counter_row2.write(meta.cms_hash2, count2 + meta.pkt_len);
        cms_counter_row3.write(meta.cms_hash3, count3 + meta.pkt_len);
        cms_counter_row4.write(meta.cms_hash4, count4 + meta.pkt_len);
    }

    action detect_elephant_flow() {
        bit<32> count1;
        bit<32> count2;
        bit<32> count3;
        bit<32> count4;
        
        cms_counter_row1.read(count1, meta.cms_hash1);
        cms_counter_row2.read(count2, meta.cms_hash2);
        cms_counter_row3.read(count3, meta.cms_hash3);
        cms_counter_row4.read(count4, meta.cms_hash4);
        
        bit<32> min_count = count1;
        if (count2 < min_count) { min_count = count2; }
        if (count3 < min_count) { min_count = count3; }
        if (count4 < min_count) { min_count = count4; }
        
        bit<1> is_elephant_cms = 0;
        if (min_count >= LONG_FLOW_THRESHOLD) {
            is_elephant_cms = 1;
        }
        
        bit<32> total_bytes;
        flow_total_bytes.read(total_bytes, meta.flow_hash);
        total_bytes = total_bytes + meta.pkt_len;
        flow_total_bytes.write(meta.flow_hash, total_bytes);
        
        bit<1> current_elephant_status;
        flow_is_elephant.read(current_elephant_status, meta.flow_hash);
        
        if (total_bytes >= LONG_FLOW_THRESHOLD || is_elephant_cms == 1) {
            if (current_elephant_status == 0) {
                flow_is_elephant.write(meta.flow_hash, 1);
                
                bit<32> elephant_count;
                elephant_flow_count.read(elephant_count, 0);
                elephant_flow_count.write(0, elephant_count + 1);
            }
        }
    }

    action calculate_payload() {
        bit<32> payload_len = 0;
        if (hdr.tcp.isValid()) {
            bit<32> packet_len = (bit<32>)standard_metadata.packet_length;
            meta.pkt_len = packet_len;
            bit<32> ip_hlen = (bit<32>)hdr.ipv4.ihl * 4;
            bit<32> tcp_hlen = (bit<32>)hdr.tcp.dataOffset * 4;
            if (packet_len > ip_hlen + tcp_hlen) {
                payload_len = packet_len - ip_hlen - tcp_hlen;
            }
            meta.payload_len = payload_len;

            meta.is_data_packet = (payload_len > 0) ? (bit<1>)1 : (bit<1>)0;
            meta.is_ack_packet = ((hdr.tcp.flags & 0x10) == 0x10) ? (bit<1>)1 : (bit<1>)0;

        } else if (hdr.udp.isValid()) {
            bit<32> ip_hlen = (bit<32>)hdr.ipv4.ihl * 4;
            bit<32> udp_hlen = 8;
            bit<32> packet_len = (bit<32>)standard_metadata.packet_length;
            
            if (packet_len > ip_hlen + udp_hlen) {
                payload_len = packet_len - ip_hlen - udp_hlen;
            }
            meta.pkt_len = payload_len;
            meta.payload_len = payload_len;
            meta.is_data_packet = 1;
            meta.is_ack_packet = 0;
        }
    }

    action rtt_track_syn() {
        bit<48> now = standard_metadata.ingress_global_timestamp;
        flow_syn_timestamp.write(meta.flow_hash, now);
        
    }

    action rtt_compute_from_synack() {
        bit<48> now = standard_metadata.ingress_global_timestamp;
        
        // Read SYN timestamp
        bit<48> syn_ts;
        flow_syn_timestamp.read(syn_ts, meta.flow_hash);
        
        if (syn_ts > 0 && now > syn_ts) {
            bit<48> rtt_us = now - syn_ts;
            
            // Validate: 1us - 500ms
            if (rtt_us > 0 && rtt_us < 500000) {
                // Store RTT
                flow_handshake_rtt_us.write(meta.flow_hash, (bit<32>)rtt_us);
                flow_rtt_sample_count.write(meta.flow_hash, 1);
                
                // Clear SYN timestamp
                flow_syn_timestamp.write(meta.flow_hash, 0);
            }
        }
    }

    action rtt_track_data_packet() {
        // Client → Server: Store timestamp when DATA sent
        bit<48> now = standard_metadata.ingress_global_timestamp;
        flow_last_data_timestamp.write(meta.flow_hash, now);
    }

    action rtt_compute_from_ack() {
        // Server → Client: Calculate RTT when ACK received
        bit<48> now = standard_metadata.ingress_global_timestamp;
        
        bit<48> data_ts;
        flow_last_data_timestamp.read(data_ts, meta.flow_hash);
        
        if (data_ts > 0 && now > data_ts) {
            bit<48> rtt_sample = now - data_ts;
            
            // ✅ Validate: 10µs - 10s (looser than handshake)
            if (rtt_sample >= 10 && rtt_sample <= 10000000) {
                // ✅ Simple overwrite 
                flow_rtt_current_us.write(meta.flow_hash, (bit<32>)rtt_sample);
                flow_rtt_sample_count.write(meta.flow_hash, 1);
            }
        }
    }
    
    apply {
        if (hdr.ipv4.isValid()) {
            calculate_payload();
            compute_flow_hash();

            // ==================== FLOW COUNTERS ====================
            bit<32> bytes;
            flow_bytes_interval.read(bytes, meta.flow_hash);
            flow_bytes_interval.write(meta.flow_hash, bytes + meta.pkt_len);

            bit<32> pkts;
            flow_packets_interval.read(pkts, meta.flow_hash);
            flow_packets_interval.write(meta.flow_hash, pkts + 1);

            // RTT MEASUREMENT
            if (hdr.tcp.isValid()) {
                // ========== BASE RTT: Handshake ==========
                if (meta.is_client_to_server == 1 && 
                    (hdr.tcp.flags & TCP_FLAG_SYN) == TCP_FLAG_SYN &&
                    (hdr.tcp.flags & TCP_FLAG_ACK) == 0) {
                    rtt_track_syn();
                }
                
                if (meta.is_client_to_server == 0 &&
                    (hdr.tcp.flags & TCP_FLAG_SYN) == TCP_FLAG_SYN &&
                    (hdr.tcp.flags & TCP_FLAG_ACK) == TCP_FLAG_ACK) {
                    rtt_compute_from_synack();
                }
                
                // ========== CONTINUOUS RTT: DATA/ACK ==========
                // Track DATA packets (client → server)
                if (meta.is_client_to_server == 1 && meta.is_data_packet == 1) {
                    rtt_track_data_packet();
                }
                
                // Compute RTT from ACK (server → client)
                if (meta.is_client_to_server == 0 && meta.is_ack_packet == 1) {
                    rtt_compute_from_ack();
                }
            }

            // LONG-FLOW DETECTION
            cms_update();
            detect_elephant_flow();
            
            // ✅ FIX: Check digest timer
            bit<48> now = standard_metadata.ingress_global_timestamp;
            bit<48> last_digest_time;
            flow_last_digest_time.read(last_digest_time, meta.flow_hash);

            // ✅ FIX: Cho phép gửi digest TRƯỚC KHI đủ 1s (cho flow mới)
            bit<1> should_send = 0;
            
            if (last_digest_time == 0) {
                // Flow mới - CHỈ INIT timestamp, KHÔNG GỬI
                flow_last_digest_time.write(meta.flow_hash, now);
                should_send = 0;  // KHÔNG gửi digest ngay
            } else {
                bit<48> time_diff = now - last_digest_time;
                if (time_diff >= DIGEST_INTERVAL_US) {
                    should_send = 1;
                }
            }
            
            if (should_send == 1) {
                bit<32> bytes_int;
                flow_bytes_interval.read(bytes_int, meta.flow_hash);
                
                bit<32> pkts_int;
                flow_packets_interval.read(pkts_int, meta.flow_hash);
                
                if (bytes_int > 0) {
                    // HYBRID: Continuous RTT + Handshake fallback
                    bit<32> rtt_current;
                    flow_rtt_current_us.read(rtt_current, meta.flow_hash);
                    
                    bit<32> rtt_to_send;
                    if (rtt_current > 0) {
                        // Priority: Use continuous RTT
                        rtt_to_send = rtt_current;
                    } else {
                        // Fallback: Use handshake RTT
                        flow_handshake_rtt_us.read(rtt_to_send, meta.flow_hash);
                    }
                    
                    bit<32> samples;
                    flow_rtt_sample_count.read(samples, meta.flow_hash);
                    
                    bit<1> elephant;
                    flow_is_elephant.read(elephant, meta.flow_hash);

                    digest<flow_digest_t>(1, {
                        meta.flow_hash,
                        bytes_int,
                        pkts_int,
                        rtt_to_send,  // Luôn có giá trị
                        samples,
                        (bit<8>)elephant,
                        (bit<64>)now,
                        hdr.ipv4.protocol
                    });
                    
                    flow_bytes_interval.write(meta.flow_hash, 0);
                    flow_packets_interval.write(meta.flow_hash, 0);
                    flow_last_digest_time.write(meta.flow_hash, now);
                }
            }
        }
    }
}

// ==================== EGRESS ====================
control MyEgress(inout headers hdr,
                 inout metadata meta,
                 inout standard_metadata_t standard_metadata) {
    apply { }
}

// ==================== CHECKSUM ====================
control MyComputeChecksum(inout headers hdr, inout metadata meta) {
    apply {
        update_checksum(
            hdr.ipv4.isValid(),
            { hdr.ipv4.version, hdr.ipv4.ihl,
              hdr.ipv4.diffserv, hdr.ipv4.totalLen,
              hdr.ipv4.identification, hdr.ipv4.flags,
              hdr.ipv4.fragOffset, hdr.ipv4.ttl,
              hdr.ipv4.protocol, hdr.ipv4.srcAddr,
              hdr.ipv4.dstAddr },
            hdr.ipv4.hdrChecksum,
            HashAlgorithm.csum16);
    }
}

// ==================== DEPARSER ====================
control MyDeparser(packet_out packet, in headers hdr) {
    apply {
        packet.emit(hdr.ethernet);
        packet.emit(hdr.ipv4);
        packet.emit(hdr.tcp);
        packet.emit(hdr.udp);
    }
}

// ==================== MAIN ====================
V1Switch(
    MyParser(),
    MyVerifyChecksum(),
    MyIngress(),
    MyEgress(),
    MyComputeChecksum(),
    MyDeparser()
) main;