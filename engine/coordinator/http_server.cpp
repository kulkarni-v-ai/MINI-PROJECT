// ============================================================================
// http_server.cpp - Native HTTP/1.1 Upload API & Live PowerShell/CMD Console
// ============================================================================
#include "http_server.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <vector>

#include "../chunking/chunk_manager.hpp"
#include "../coding/gf256.hpp"
#include "../coding/reed_solomon.hpp"
#include "../communication/protocol.hpp"
#include "../communication/tcp_client.hpp"
#include "coordinator_service.hpp"

namespace fs = std::filesystem;

namespace {

std::string to_lower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

std::string trim(const std::string& s) {
    size_t a = 0;
    while (a < s.size() && (s[a] == ' ' || s[a] == '\t' || s[a] == '\r' || s[a] == '\n')) ++a;
    size_t b = s.size();
    while (b > a && (s[b - 1] == ' ' || s[b - 1] == '\t' || s[b - 1] == '\r' || s[b - 1] == '\n')) --b;
    return s.substr(a, b - a);
}

std::string url_decode(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            auto hex = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                return -1;
            };
            int hi = hex(s[i + 1]);
            int lo = hex(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>((hi << 4) | lo));
                i += 2;
                continue;
            }
        } else if (s[i] == '+') {
            out.push_back(' ');
            continue;
        }
        out.push_back(s[i]);
    }
    return out;
}

std::string get_query_param(const std::string& query, const std::string& key) {
    size_t pos = 0;
    while (pos < query.size()) {
        size_t amp = query.find('&', pos);
        std::string pair = (amp == std::string::npos) ? query.substr(pos) : query.substr(pos, amp - pos);
        size_t eq = pair.find('=');
        if (eq != std::string::npos) {
            if (url_decode(pair.substr(0, eq)) == key) {
                return url_decode(pair.substr(eq + 1));
            }
        }
        if (amp == std::string::npos) break;
        pos = amp + 1;
    }
    return "";
}

std::string bytes_to_hex(const std::vector<uint8_t>& buf, size_t max_bytes) {
    std::ostringstream os;
    const size_t n = std::min(buf.size(), max_bytes);
    for (size_t i = 0; i < n; ++i) {
        if (i > 0) os << ' ';
        os << std::uppercase << std::hex << std::setfill('0') << std::setw(2)
           << static_cast<unsigned>(buf[i]);
    }
    return os.str();
}

std::string bytes_to_ascii(const std::vector<uint8_t>& buf, size_t max_bytes) {
    std::string out;
    const size_t n = std::min(buf.size(), max_bytes);
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        const uint8_t b = buf[i];
        out.push_back((b >= 32 && b <= 126) ? static_cast<char>(b) : '.');
    }
    return out;
}

std::string daca_tier_description(uint64_t file_size, uint64_t shard_size) {
    uint64_t expected_base = 8ULL * 1024ULL * 1024ULL;
    std::string tier = "> 1 GB tier -> 8 MB default";
    if (file_size <= 1048576ULL) {
        expected_base = 65536ULL;
        tier = "<= 1 MB tier -> 64 KB chunks";
    } else if (file_size <= 10485760ULL) {
        expected_base = 262144ULL;
        tier = "1 MB - 10 MB tier -> 256 KB chunks";
    } else if (file_size <= 104857600ULL) {
        expected_base = 1048576ULL;
        tier = "10 MB - 100 MB tier -> 1 MB chunks";
    } else if (file_size <= 1073741824ULL) {
        expected_base = 4194304ULL;
        tier = "100 MB - 1 GB tier -> 4 MB chunks";
    }
    if (shard_size > 0 && shard_size < expected_base) {
        tier += " (DACA stepped down to fit tightest peer storage headroom)";
    }
    return tier;
}

bool fetch_chunk_from_peer(const PeerInfo& peer, const std::string& chunk_id, std::vector<uint8_t>& payload_out) {
    TCPClient c(1200, 3000);
    if (!c.connect_to(peer.address, peer.port)) return false;
    json::Json req = json::Json::object();
    req["type"] = proto::GET_CHUNK;
    req["chunk_id"] = chunk_id;
    json::Json resp;
    if (c.request(req, {}, resp, payload_out) != proto::RecvStatus::OK) return false;
    return resp.contains("type") &&
           resp["type"].as_string() == proto::CHUNK_RESPONSE &&
           (!resp.contains("status") || resp["status"].as_string() == "OK");
}

bool parse_multipart_file(const std::string& content_type,
                          const std::vector<uint8_t>& body,
                          std::string& filename_out,
                          std::vector<uint8_t>& file_bytes_out) {
    std::string ct_low = to_lower(content_type);
    size_t bpos = ct_low.find("boundary=");
    if (bpos == std::string::npos) return false;
    std::string boundary = content_type.substr(bpos + 9);
    size_t semi = boundary.find(';');
    if (semi != std::string::npos) boundary = boundary.substr(0, semi);
    boundary = trim(boundary);
    if (boundary.size() >= 2 && boundary.front() == '"' && boundary.back() == '"') {
        boundary = boundary.substr(1, boundary.size() - 2);
    }
    if (boundary.empty()) return false;

    const std::string delim = "--" + boundary;
    const std::string raw(reinterpret_cast<const char*>(body.data()), body.size());

    size_t pos = raw.find(delim);
    while (pos != std::string::npos) {
        size_t part_start = pos + delim.size();
        if (part_start + 2 <= raw.size() && raw.substr(part_start, 2) == "--") break;
        if (part_start + 2 <= raw.size() && raw.substr(part_start, 2) == "\r\n") part_start += 2;

        size_t hdr_end = raw.find("\r\n\r\n", part_start);
        if (hdr_end == std::string::npos) break;

        std::string part_hdr = raw.substr(part_start, hdr_end - part_start);
        size_t data_start = hdr_end + 4;
        size_t next_delim = raw.find("\r\n" + delim, data_start);
        if (next_delim == std::string::npos) break;

        size_t fn_pos = part_hdr.find("filename=\"");
        if (fn_pos != std::string::npos) {
            fn_pos += 10;
            size_t fn_end = part_hdr.find('"', fn_pos);
            if (fn_end != std::string::npos) {
                filename_out = fs::path(part_hdr.substr(fn_pos, fn_end - fn_pos)).filename().string();
            }
            file_bytes_out.assign(body.begin() + static_cast<std::ptrdiff_t>(data_start),
                                  body.begin() + static_cast<std::ptrdiff_t>(next_delim));
            return true;
        }
        pos = next_delim + 2;
    }
    return false;
}

void send_http_response(socket_t fd,
                        int status_code,
                        const std::string& status_text,
                        const std::string& content_type,
                        const std::string& body,
                        const std::string& extra_headers = "") {
    std::ostringstream hdr;
    hdr << "HTTP/1.1 " << status_code << " " << status_text << "\r\n"
        << "Content-Type: " << content_type << "\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Access-Control-Allow-Origin: *\r\n"
        << "Access-Control-Allow-Headers: Content-Type, Authorization, X-Auth-Token, X-Filename\r\n"
        << "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
        << "Connection: close\r\n"
        << extra_headers
        << "\r\n";
    const std::string h = hdr.str();
    proto::send_all(fd, h.data(), h.size());
    if (!body.empty()) proto::send_all(fd, body.data(), body.size());
}

void send_json(socket_t fd, int status_code, const std::string& status_text, const json::Json& j) {
    send_http_response(fd, status_code, status_text, "application/json; charset=utf-8", j.dump(2));
}

// Dashboard HTML split into chunks (< 10 KB each) to respect MSVC C2026 string literal limits.
const char* DASH_PART_1 = R"HTML(<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Resilient Multi-Peer File Storage — PowerShell / CMD Live Monitor</title>
<style>
  :root {
    --bg:#011830; --bg-deep:#010e1f; --panel:#032244; --panel-alt:#042b56;
    --border:#174c82; --border-bright:#2a75be;
    --text:#e6f1ff; --muted:#8cb3d9; --dim:#5c85b0;
    --ps-prompt:#facc15; --cyan:#38bdf8; --ok:#22c55e; --warn:#f59e0b; --bad:#ef4444; --purple:#c084fc;
  }
  body.cmd-theme {
    --bg:#0c0c0c; --bg-deep:#050505; --panel:#141414; --panel-alt:#1a1a1a;
    --border:#2c3e30; --border-bright:#22c55e;
    --text:#d4f7dc; --muted:#7ab588; --dim:#4e7858;
    --ps-prompt:#facc15; --cyan:#22d3ee; --ok:#4ade80; --warn:#fbbf24; --bad:#f87171; --purple:#a78bfa;
  }
  * { box-sizing:border-box; }
  body {
    margin:0; background:var(--bg); color:var(--text);
    font-family:'Cascadia Code','Consolas','Courier New',monospace; font-size:12.5px; line-height:1.45;
  }
  .term-chrome {
    background:var(--bg-deep); border-bottom:2px solid var(--border-bright);
    padding:10px 18px; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px;
    position:sticky; top:0; z-index:40;
  }
  .win-title { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
  .win-dots { display:flex; gap:6px; }
  .dot { width:10px; height:10px; border-radius:50%; display:inline-block; }
  .dot.r{background:#ef4444;} .dot.y{background:#f59e0b;} .dot.g{background:#22c55e;}
  h1 { margin:0; font-size:14px; font-weight:700; color:var(--cyan); letter-spacing:.2px; }
  .prompt { color:var(--ps-prompt); font-weight:700; }
  .sub { font-size:11.5px; color:var(--muted); }
  .wrap { max-width:1420px; margin:0 auto; padding:14px 16px 28px; display:grid; gap:14px; }
  .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  .grid-kpi { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:10px; }
  .grid-2 { display:grid; grid-template-columns:1.15fr 1fr; gap:12px; }
  @media(max-width:1020px){ .grid-2 { grid-template-columns:1fr; } }
  .card {
    background:var(--panel); border:1px solid var(--border); border-radius:6px;
    padding:12px 14px; box-shadow:0 4px 16px rgba(0,0,0,.28);
  }
  .card-hdr {
    display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;
    padding-bottom:8px; margin-bottom:10px; border-bottom:1px dashed var(--border);
  }
  .card-title { font-size:13px; font-weight:700; color:var(--text); }
  .kpi { background:var(--panel-alt); border:1px solid var(--border); border-left:3px solid var(--cyan); padding:9px 12px; border-radius:4px; }
  .kpi .val { font-size:20px; font-weight:700; margin-top:2px; }
  input, select, button {
    font-family:inherit; font-size:12px; padding:6px 10px; border-radius:4px;
    border:1px solid var(--border-bright); background:var(--bg-deep); color:var(--text);
  }
  button {
    cursor:pointer; font-weight:700; background:#0c4a6e; border-color:var(--cyan); color:#f0f9ff;
  }
  button:hover { filter:brightness(1.2); }
  button.ok { background:#14532d; border-color:var(--ok); color:#dcfce7; }
  button.warn { background:#78350f; border-color:var(--warn); color:#fef3c7; }
  button.sec { background:var(--panel-alt); border-color:var(--border); color:var(--text); }
  button.active-pill { background:var(--cyan); color:#020617; border-color:var(--cyan); }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid rgba(42,117,190,.28); }
  th { color:var(--ps-prompt); font-size:11px; text-transform:uppercase; }
  .tag {
    display:inline-block; padding:1px 7px; border-radius:3px; font-size:11px; font-weight:700;
    border:1px solid currentColor;
  }
  .ONLINE,.AVAILABLE,.HEALTHY,.OK,.MATCH { color:var(--ok); background:rgba(34,197,94,.12); }
  .SUSPECT,.DEGRADED,.BUSY,.RECOVERY_PENDING,.WARN { color:var(--warn); background:rgba(245,158,11,.13); }
  .OFFLINE,.UNAVAILABLE,.CORRUPT,.UNRECOVERABLE,.FAIL { color:var(--bad); background:rgba(239,68,68,.15); }
  .STORED,.ARMA,.INFO { color:var(--cyan); background:rgba(56,189,248,.13); }
  .PIPE,.PARITY { color:var(--purple); background:rgba(192,132,252,.13); }
</style>
</head>
<body>
<div class="term-chrome">
  <div class="win-title">
    <div class="win-dots"><span class="dot r"></span><span class="dot y"></span><span class="dot g"></span></div>
    <h1>Resilient Multi-Peer File Storage</h1>
    <span class="sub">[Windows PowerShell / CMD Live Architecture &amp; Recovery Visualizer]</span>
  </div>
  <div class="row">
    <button class="sec" onclick="toggleTheme()" id="themeBtn">Theme: PowerShell</button>
    <input id="tok" type="password" value="rfe-demo-token" style="width:145px" title="Cluster Auth Token">
    <button onclick="login()">Connect-Auth</button>
    <span id="authState" class="tag INFO">AUTH_PENDING</span>
  </div>
</div>
)HTML";

const char* DASH_PART_2 = R"HTML(
<style>
  .pipeline {
    display:grid; grid-template-columns:repeat(8, minmax(122px,1fr)); gap:6px; overflow-x:auto; padding-bottom:4px;
  }
  .pipe-box {
    background:var(--bg-deep); border:1px solid var(--border); border-radius:4px; padding:8px; position:relative;
    transition:border-color .2s;
  }
  .pipe-box.ok { border-color:var(--ok); }
  .pipe-box.warn { border-color:var(--warn); box-shadow:0 0 10px rgba(245,158,11,.25); }
  .pipe-box.bad { border-color:var(--bad); box-shadow:0 0 10px rgba(239,68,68,.28); }
  .pipe-step { font-size:10px; color:var(--ps-prompt); font-weight:700; }
  .pipe-name { font-size:11.5px; font-weight:700; color:var(--cyan); margin:2px 0; }
  .pipe-detail { font-size:10.5px; color:var(--muted); min-height:28px; }
  .peer-rack { display:grid; grid-template-columns:repeat(auto-fit,minmax(225px,1fr)); gap:10px; margin-top:10px; }
  .peer-node {
    background:var(--bg-deep); border:1px solid var(--ok); border-radius:5px; padding:9px 10px;
  }
  .peer-node.OFFLINE { border-color:var(--bad); background:rgba(127,29,29,.22); }
  .peer-node.SUSPECT,.peer-node.BUSY { border-color:var(--warn); background:rgba(120,53,15,.18); }
  .disk-bar { height:6px; background:#091a2e; border:1px solid var(--border); border-radius:2px; overflow:hidden; margin:5px 0; }
  .disk-fill { height:100%; background:var(--ok); }
  .shard-chip {
    display:inline-flex; align-items:center; gap:4px; margin:2px 3px 2px 0; padding:2px 6px;
    border-radius:3px; border:1px solid var(--border); background:var(--panel); font-size:11px;
  }
  .shard-chip.parity { border-color:var(--purple); color:var(--purple); }
  .shard-chip.repl { border-color:var(--cyan); }
  .shard-chip.bad { border-color:var(--bad); color:var(--bad); }
  .daca-strip {
    display:flex; flex-wrap:wrap; gap:4px; padding:8px; background:var(--bg-deep); border:1px solid var(--border); border-radius:4px;
  }
  .daca-seg {
    padding:6px 8px; border-radius:3px; border:1px solid var(--cyan); background:rgba(56,189,248,.1);
    font-size:11px; min-width:128px;
  }
  .daca-seg.pad { border-style:dashed; border-color:var(--warn); }
  .matrix-box {
    background:var(--bg-deep); border:1px solid var(--border); border-radius:4px; padding:9px 11px; overflow-x:auto;
  }
  .hex-row { display:grid; grid-template-columns:135px 1fr 125px 95px; gap:8px; padding:4px 0; border-bottom:1px dashed rgba(92,133,176,.28); align-items:center; }
  .hex-row.dead { opacity:.65; color:var(--bad); }
  .flow9 { display:grid; grid-template-columns:repeat(auto-fit,minmax(165px,1fr)); gap:7px; }
  .fnode {
    background:var(--bg-deep); border:1px solid var(--border); border-radius:4px; padding:7px 9px; font-size:11px;
  }
  .fnode.lit-ok { border-color:var(--ok); background:rgba(34,197,94,.14); }
  .fnode.lit-warn { border-color:var(--warn); background:rgba(245,158,11,.15); }
  .fnode.lit-bad { border-color:var(--bad); background:rgba(239,68,68,.16); }
  .fnode.lit-cyan { border-color:var(--cyan); background:rgba(56,189,248,.16); }
  .term-log {
    background:#020611; border:1px solid var(--border); border-radius:4px; padding:10px; max-height:240px;
    overflow:auto; font-size:11.5px; white-space:pre-wrap;
  }
  .cmd-strip {
    background:var(--bg-deep); border:1px dashed var(--border); border-radius:4px; padding:7px 10px;
    font-size:11.5px; color:var(--muted); display:flex; flex-wrap:wrap; gap:10px; align-items:center;
  }
  code.cmd { background:var(--panel); padding:2px 6px; border:1px solid var(--border); border-radius:3px; color:var(--ps-prompt); }
</style>
<div class="wrap">
  <div class="cmd-strip">
    <span class="prompt">PS C:\RFE\CLUSTER&gt;</span>
    <span>Get-ClusterTelemetry -Poll 1500ms</span>
    <span style="margin-left:auto" id="liveClock" class="sub">syncing...</span>
  </div>
  <div class="grid-kpi" id="summary"></div>

  <div class="card">
    <div class="card-hdr">
      <div class="card-title"><span class="prompt">PS&gt;</span> End-to-End File Pipeline Flow (<span id="pipeFileLabel">No file selected</span>)</div>
      <div class="row">
        <input type="file" id="fileInput">
        <button onclick="uploadFile()" class="ok">Invoke-RfeUpload (HTTP -&gt; DACA -&gt; RS -&gt; PASPA/ARMA)</button>
        <span id="uploadMsg" class="sub"></span>
      </div>
    </div>
    <div class="pipeline" id="pipelineFlow"></div>
  </div>

  <div class="card">
    <div class="card-hdr">
      <div class="card-title"><span class="prompt">PS&gt;</span> Participating Storage Peers &amp; Live Shard Residency (<code class="cmd">rfe peers</code>)</div>
      <span class="sub">Green = ONLINE &bull; Yellow = SUSPECT/BUSY &bull; Red = OFFLINE (HPFD Heartbeat Timeout)</span>
    </div>
    <table>
      <thead><tr><th>PEER_ID</th><th>ENDPOINT</th><th>HEALTH</th><th>FREE STORAGE</th><th>TRANSFERS</th><th>HEARTBEAT AGE</th></tr></thead>
      <tbody id="peersBody"></tbody>
    </table>
    <div class="peer-rack" id="peerRack"></div>
  </div>

  <div class="card">
    <div class="card-hdr">
      <div class="card-title"><span class="prompt">PS&gt;</span> File Catalogue (<code class="cmd">rfe files</code>)</div>
      <span class="sub">Click <b>Visualize Internals</b> on any file to inspect DACA slicing, GF(256) RS parity bytes &amp; peer recovery math</span>
    </div>
    <table>
      <thead><tr><th>FILE_ID</th><th>FILENAME</th><th>SIZE</th><th>RS POLICY</th><th>SHARD_SIZE</th><th>STRIPES</th><th>STATUS</th><th>ACTIONS</th></tr></thead>
      <tbody id="filesBody"></tbody>
    </table>
  </div>
)HTML";

const char* DASH_PART_3 = R"HTML(
  <div class="card" id="inspectorCard">
    <div class="card-hdr">
      <div class="card-title"><span class="prompt">PS&gt;</span> Deep Algorithm &amp; Parity Proof Visualizer (<span id="inspTitle">Select or Upload a File</span>)</div>
      <div class="row">
        <div id="stripePills" class="row"></div>
        <button onclick="probeSelectedStripe()" id="probeBtn">Probe Live Peers (Read Real Shards &amp; Verify RS Math)</button>
      </div>
    </div>
    <div id="inspBody" class="sub">Upload a file or click <b>Visualize Internals</b> on a tracked file above.</div>
  </div>

  <div class="card">
    <div class="card-hdr">
      <div class="card-title"><span class="prompt">PS&gt;</span> HRERA 9-Step Hybrid Recovery Flowchart (Live Cluster Path Trace)</div>
      <span id="hreraBadge" class="tag INFO">MONITORING</span>
    </div>
    <div class="flow9" id="hreraFlow"></div>
  </div>

  <div class="card">
    <div class="card-hdr">
      <div class="card-title"><span class="prompt">PS&gt;</span> Get-RfeEvents -Tail 25 (Live Integrity, DACA, PASPA, ARMA &amp; HRERA Audit Stream)</div>
      <div class="row" id="cmdQuickStrip"></div>
    </div>
    <div class="term-log" id="eventsBox">PS C:\RFE\CLUSTER&gt; waiting for events...</div>
  </div>
</div>
)HTML";

const char* DASH_PART_4 = R"HTML(<script>
let sessionToken = localStorage.getItem('rfe_token') || '';
let selectedFileId = '';
let selectedStripeIdx = 0;
let lastInspectData = null;
let simulatedDrops = new Set();
let lastCatalog = [];
let lastPeers = [];
let lastEvents = [];
let lastStatus = {};
let lastHealthSig = '';

// Real GF(256) with primitive polynomial 0x11D (identical to engine/coding/gf256.cpp)
const GF_EXP = new Uint8Array(512);
const GF_LOG = new Uint8Array(256);
(function initGF() {
  let x = 1;
  for (let i = 0; i < 255; ++i) {
    GF_EXP[i] = x;
    GF_EXP[i + 255] = x;
    GF_LOG[x] = i;
    x <<= 1;
    if (x & 0x100) x ^= 0x11d;
  }
  GF_EXP[510] = GF_EXP[0];
})();
function gfMul(a, b) { return (a === 0 || b === 0) ? 0 : GF_EXP[GF_LOG[a] + GF_LOG[b]]; }
function gfDiv(a, b) {
  if (b === 0) return 0;
  if (a === 0) return 0;
  return GF_EXP[(GF_LOG[a] + 255 - GF_LOG[b]) % 255];
}
function gfInvert(mat, k) {
  const aug = [];
  for (let r = 0; r < k; ++r) {
    const row = new Array(2 * k).fill(0);
    for (let c = 0; c < k; ++c) row[c] = mat[r * k + c];
    row[k + r] = 1;
    aug.push(row);
  }
  for (let col = 0; col < k; ++col) {
    let piv = -1;
    for (let r = col; r < k; ++r) if (aug[r][col] !== 0) { piv = r; break; }
    if (piv < 0) return null;
    if (piv !== col) { const t = aug[col]; aug[col] = aug[piv]; aug[piv] = t; }
    const invP = gfDiv(1, aug[col][col]);
    for (let j = 0; j < 2 * k; ++j) aug[col][j] = gfMul(aug[col][j], invP);
    for (let r = 0; r < k; ++r) {
      if (r === col || aug[r][col] === 0) continue;
      const f = aug[r][col];
      for (let j = 0; j < 2 * k; ++j) aug[r][j] ^= gfMul(f, aug[col][j]);
    }
  }
  const out = new Array(k * k);
  for (let r = 0; r < k; ++r) for (let c = 0; c < k; ++c) out[r * k + c] = aug[r][k + c];
  return out;
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function hex2(n) { return '0x' + Number(n).toString(16).toUpperCase().padStart(2, '0'); }
function hexByte(n) { return Number(n).toString(16).toUpperCase().padStart(2, '0'); }
function fmtBytes(b) {
  b = Number(b || 0);
  if (b >= 1073741824) return (b/1073741824).toFixed(2)+' GB';
  if (b >= 1048576) return (b/1048576).toFixed(2)+' MB';
  if (b >= 1024) return (b/1024).toFixed(2)+' KB';
  return b+' B';
}
function hdrs(extra = {}) {
  return sessionToken ? Object.assign({'Authorization':'Bearer '+sessionToken}, extra) : extra;
}
function toggleTheme() {
  document.body.classList.toggle('cmd-theme');
  const isCmd = document.body.classList.contains('cmd-theme');
  document.getElementById('themeBtn').textContent = isCmd ? 'Theme: CMD Black' : 'Theme: PowerShell';
}
async function login() {
  const token = document.getElementById('tok').value;
  try {
    const r = await fetch('/api/auth', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({token, client_id:'ps-web-monitor'})
    });
    const d = await r.json();
    if (r.ok && d.session_token) {
      sessionToken = d.session_token;
      localStorage.setItem('rfe_token', sessionToken);
      document.getElementById('authState').className = 'tag ONLINE';
      document.getElementById('authState').textContent = 'SESSION ' + d.session_id;
      if (selectedFileId) inspectVisualFlow(selectedFileId, selectedStripeIdx, true);
    } else {
      document.getElementById('authState').className = 'tag OFFLINE';
      document.getElementById('authState').textContent = d.error || 'AUTH FAILED';
    }
  } catch (e) {}
}
async function uploadFile() {
  const inp = document.getElementById('fileInput');
  if (!inp.files.length) return alert('Choose a file first');
  const f = inp.files[0];
  const msgEl = document.getElementById('uploadMsg');
  msgEl.textContent = 'Uploading & running DACA -> SHA-256 -> GF(256) RS -> PASPA/ARMA...';
  const r = await fetch('/api/upload', {method:'POST', headers:hdrs({'X-Filename':f.name}), body:f});
  const d = await r.json();
  if (r.ok) {
    msgEl.textContent = 'DONE: ' + d.file_id + ' [' + d.status + '] (' + d.stripes_count + ' stripes)';
    selectedStripeIdx = 0;
    simulatedDrops.clear();
    await refresh();
    await inspectVisualFlow(d.file_id, 0, true);
  } else {
    msgEl.textContent = 'ERROR: ' + (d.error || r.status);
  }
}
async function repairFile(fid) {
  const r = await fetch('/api/repair/' + encodeURIComponent(fid), {method:'POST', headers:hdrs()});
  const d = await r.json();
  await refresh();
  await inspectVisualFlow(fid, selectedStripeIdx, true);
  alert((d.status || '') + '\n' + (d.report || d.error || ''));
}
)HTML";

const char* DASH_PART_5 = R"HTML(
function toggleSimDrop(idx) {
  if (simulatedDrops.has(idx)) simulatedDrops.delete(idx);
  else simulatedDrops.add(idx);
  if (lastInspectData) renderInspector(lastInspectData);
}
function probeSelectedStripe() {
  if (!selectedFileId) return;
  inspectVisualFlow(selectedFileId, selectedStripeIdx, true);
}
async function inspectVisualFlow(fid, stripeIdx = 0, probeLive = true) {
  selectedFileId = fid;
  selectedStripeIdx = stripeIdx;
  try {
    const url = '/api/inspect/' + encodeURIComponent(fid) + '?stripe=' + stripeIdx + (probeLive && sessionToken ? '&probe=1' : '&probe=0');
    const r = await fetch(url, {headers:hdrs()});
    if (!r.ok) return;
    const d = await r.json();
    lastInspectData = d;
    renderPipeline(d);
    renderPeerRack();
    renderInspector(d);
    renderHreraFlow();
    renderQuickCmds(fid);
  } catch (e) {}
}

function renderPipeline(d) {
  const el = document.getElementById('pipelineFlow');
  const lbl = document.getElementById('pipeFileLabel');
  if (!d) {
    lbl.textContent = 'Cluster Idle';
    el.innerHTML = '<div class="sub">Upload a file or select a file row below to trace every pipeline module live.</div>';
    return;
  }
  lbl.textContent = esc(d.file_id) + ' (' + esc(d.filename) + ')';
  const st = d.status || 'STORED';
  const isDeg = (st === 'DEGRADED' || st === 'RECOVERY_PENDING');
  const isBad = (st === 'UNRECOVERABLE');
  const replCnt = d.arma_hot_replicas || 0;
  const peersOn = lastStatus.peers_online || 0;
  const peersTot = lastStatus.peers_total || 0;
  const steps = [
    {n:'[1] UPLOAD API', title:'Session & Stage', cls:'ok', txt: fmtBytes(d.size)+' staged centrally -> ID '+esc(d.file_id)},
    {n:'[2] DACA ENGINE', title:'Adaptive Chunk', cls:'ok', txt: fmtBytes(d.daca.shard_size)+' chunks x '+d.daca.stripes_total+' stripe(s)'},
    {n:'[3] EVP_SHA256', title:'Crypto Hash', cls:'ok', txt: 'File '+esc((d.sha256||'').slice(0,10))+'.. + '+d.shards_count+' shard hashes'},
    {n:'[4] GF(256) RS', title:'Parity Encode', cls:'ok', txt: 'RS('+d.daca.n+','+d.daca.k+') poly 0x11D -> '+d.daca.m+' parity/stripe'},
    {n:'[5] PASPA + ARMA', title:'Smart Placement', cls: replCnt>0?'ok':'ok', txt: 'Score-ranked placement; '+replCnt+' ARMA hot replica(s)'},
    {n:'[6] HPFD PEERS', title:'Cluster Health', cls: peersOn<peersTot?(peersOn>=d.daca.k?'warn':'bad'):'ok', txt: peersOn+'/'+peersTot+' peers ONLINE'},
    {n:'[7] HRERA 9-STEP', title:'Recovery Engine', cls: isBad?'bad':(isDeg?'warn':'ok'), txt: isDeg?'DEGRADED / repairing':(isBad?'< k shards!':'All shards healthy')},
    {n:'[8] MGFRA', title:'Rebuild Verify', cls: peersOn>=d.daca.k?'ok':'bad', txt: peersOn>=d.daca.k?'Sequence rebuild ready':'Blocked (< k='+d.daca.k+')'}
  ];
  el.innerHTML = steps.map(s =>
    '<div class="pipe-box '+s.cls+'"><div class="pipe-step">'+s.n+'</div><div class="pipe-name">'+s.title+'</div><div class="pipe-detail">'+s.txt+'</div></div>'
  ).join('');
}

function renderPeerRack() {
  const rack = document.getElementById('peerRack');
  if (!lastPeers.length) { rack.innerHTML = '<div class="sub">No active storage peers registered yet.</div>'; return; }
  const shardsByPeer = {};
  if (lastInspectData && lastInspectData.all_shards) {
    for (const sh of lastInspectData.all_shards) {
      for (let i = 0; i < (sh.peers || []).length; ++i) {
        const pid = sh.peers[i];
        if (!shardsByPeer[pid]) shardsByPeer[pid] = [];
        shardsByPeer[pid].push({
          chunk_id: sh.chunk_id,
          type: sh.shard_type,
          status: sh.status,
          isRepl: i > 0,
          hasRepl: sh.peers.length > 1
        });
      }
    }
  }
  rack.innerHTML = lastPeers.map(p => {
    const busy = p.workload && p.workload.is_busy && p.health === 'ONLINE';
    const cls = busy ? 'BUSY' : p.health;
    const held = shardsByPeer[p.id] || [];
    const chips = held.map(s => {
      const ccls = (cls === 'OFFLINE' || s.status !== 'HEALTHY') ? 'bad' : (s.type === 'PARITY' ? 'parity' : (s.hasRepl ? 'repl' : ''));
      const rtag = s.isRepl ? ' *REPL' : (s.hasRepl ? ' *PRI' : '');
      return '<span class="shard-chip '+ccls+'">'+esc(s.chunk_id.replace(/^F\d+-/, ''))+rtag+'</span>';
    }).join('') || '<span class="sub">standby (ready for PASPA/HRERA)</span>';
    const pct = Math.min(100, Math.max(8, Math.round(100 - (p.workload.active_transfers / Math.max(1, p.workload.transfer_capacity)) * 100)));
    return '<div class="peer-node '+cls+'">'+
      '<div class="row" style="justify-content:space-between"><b>'+esc(p.id)+'</b><span class="tag '+cls+'">'+cls+'</span></div>'+
      '<div class="sub">'+esc(p.address)+':'+p.port+' &bull; Free: '+fmtBytes(p.workload.available_storage_bytes)+'</div>'+
      '<div class="disk-bar"><div class="disk-fill" style="width:'+pct+'%"></div></div>'+
      '<div>'+chips+'</div></div>';
  }).join('');
}

function renderInspector(d) {
  const titleEl = document.getElementById('inspTitle');
  const pillsEl = document.getElementById('stripePills');
  const bodyEl = document.getElementById('inspBody');
  titleEl.textContent = d.file_id + ' — ' + d.filename + ' (' + fmtBytes(d.size) + ')';

  if (!d.stripes || d.stripes.length === 0) {
    pillsEl.innerHTML = '';
    bodyEl.innerHTML = '<div class="matrix-box"><span class="tag STORED">STORED CENTRALLY</span> File is staged in central storage awaiting &ge; ' + d.daca.k + ' ONLINE peers for DACA/RS distribution.</div>';
    return;
  }

  // Stripe pills (cap displayed pills cleanly if huge file)
  const totalSt = d.stripes.length;
  let pillsHtml = '';
  const maxShow = Math.min(totalSt, 12);
  for (let i = 0; i < maxShow; ++i) {
    const active = (i === d.inspected_stripe_index) ? ' active-pill' : '';
    pillsHtml += '<button class="sec'+active+'" onclick="inspectVisualFlow(\''+esc(d.file_id)+'\','+i+',true)">'+esc(d.stripes[i].stripe_id)+'</button>';
  }
  if (totalSt > maxShow) pillsHtml += '<span class="sub">... ('+totalSt+' total)</span>';
  pillsEl.innerHTML = pillsHtml;

  // 1. DACA Chunking Slicer HTML
  const dataShardsAll = (d.all_shards || []).filter(s => s.shard_type === 'DATA');
  let offsetCursor = 0;
  let dacaSegs = dataShardsAll.slice(0, 16).map(s => {
    const origLen = Number(s.original_data_length || s.size || 0);
    const padLen = Number(s.padding_length || 0);
    const stOff = offsetCursor;
    const edOff = origLen > 0 ? (offsetCursor + origLen - 1) : offsetCursor;
    offsetCursor += origLen;
    return '<div class="daca-seg'+(padLen>0?' pad':'')+'">'+
      '<div><b>'+esc(s.chunk_id)+'</b> <span class="sub">['+s.stripe_id+']</span></div>'+
      '<div class="sub">Bytes: '+stOff+'..'+edOff+' ('+origLen+' B)</div>'+
      '<div class="sub">Zero-Pad: '+padLen+' B &bull; Total: '+s.size+' B</div>'+
      '<div class="sub">SHA256: '+esc((s.sha256||'').slice(0,12))+'..</div></div>';
  }).join('');
  if (dataShardsAll.length > 16) dacaSegs += '<div class="daca-seg"><b>+ '+(dataShardsAll.length-16)+' more shards...</b></div>';

  // 2. Encoding Matrix HTML (Real GF(256) matrix from ReedSolomon)
  const k = d.daca.k, m = d.daca.m, n = d.daca.n;
  const mat = d.encoding_matrix || [];
  let matRows = '';
  for (let r = 0; r < n; ++r) {
    const label = r < k ? ('D'+r) : ('P'+(r-k));
    const coeffs = [];
    for (let c = 0; c < k; ++c) coeffs.push(hex2(mat[r*k+c] || 0));
    const eq = coeffs.map((cf, c) => cf+'&middot;D'+c).join(' &oplus; ');
    matRows += '<div><span class="tag '+(r<k?'INFO':'PARITY')+'">'+label+'</span> = [ '+coeffs.join('  ')+' ]  &rArr;  '+eq+'</div>';
  }

  // 3. Probed Stripe Live Hex + Parity Proof + Interactive/Real Erasure Recovery
  const st = d.inspected_stripe;
  const shards = st ? (st.shards || []) : [];
  let hexHtml = '';
  let presentLive = [];
  for (let i = 0; i < shards.length; ++i) {
    const s = shards[i];
    const simDropped = simulatedDrops.has(i);
    const realLive = !!s.live_reachable && !!s.verified_sha256;
    const usable = realLive && !simDropped;
    presentLive.push(usable);
    const roleTag = s.shard_type === 'PARITY' ? 'PARITY' : 'DATA';
    const peersTxt = (s.peers || []).map(p => esc(p.id)+'['+esc(p.health)+']').join(', ') || 'none';
    let stateBadge = !s.probed
      ? '<span class="tag INFO">NOT PROBED</span>'
      : (simDropped ? '<span class="tag OFFLINE">SIM DROPPED</span>'
         : (usable ? '<span class="tag ONLINE">LIVE VERIFIED ('+esc(s.served_by||'')+')</span>'
                   : '<span class="tag OFFLINE">'+(s.sha256_mismatch?'BIT-ROT CORRUPT':'OFFLINE / MISSING')+'</span>'));
    const dropBtn = s.probed
      ? ' <button class="sec" style="padding:1px 6px;font-size:10px" onclick="toggleSimDrop('+i+')">'+(simDropped?'Restore Sim':'Drop Peer Sim')+'</button>'
      : '';
    const hexSlice = usable ? esc(s.sample_hex || '') : (s.probed ? '-- -- -- -- (unavailable)' : 'click Probe Live Peers');
    const ascSlice = usable ? esc(s.sample_ascii || '') : '';
    const replBadge = (s.peers || []).length > 1 ? ' <span class="tag ARMA">ARMA REPL</span>' : '';
    hexHtml += '<div class="hex-row'+(usable?'':' dead')+'">'+
      '<div><b>'+esc(s.chunk_id.replace(/^F\d+-/, ''))+'</b> <span class="tag '+(roleTag==='PARITY'?'PARITY':'INFO')+'">'+roleTag+'</span>'+replBadge+'<div class="sub">@ '+peersTxt+'</div></div>'+
      '<div><code>'+hexSlice+'</code>'+(ascSlice?' <span class="sub">| "'+ascSlice+'"</span>':'')+'</div>'+
      '<div>'+stateBadge+'</div>'+
      '<div>'+dropBtn+'</div></div>';
  }

  // Parity equation verification on real bytes + live/simulated RS decode
  let mathHtml = '';
  if (st && st.probed) {
    const sampleCount = Math.min(8, shards[0] && shards[0].sample_bytes ? shards[0].sample_bytes.length : 0);
    if (d.parity_check && d.parity_check.checked) {
      mathHtml += '<div style="margin-bottom:8px"><span class="tag '+(d.parity_check.verified?'ONLINE':'OFFLINE')+'">'+
        (d.parity_check.verified ? 'REAL CLUSTER PARITY VERIFIED (GF(256))' : 'PARITY MISMATCH DETECTED!')+'</span> '+
        '<span class="sub">'+esc(d.parity_check.equation)+'</span></div>';
    }

    // Live C++ RS Decode Proof OR Interactive GF(256) Dry-Run when shards dropped
    const usableCount = presentLive.filter(Boolean).length;
    const anySimulated = simulatedDrops.size > 0;
    if (!anySimulated && st.live_recovery_proof && st.live_recovery_proof.performed) {
      const rp = st.live_recovery_proof;
      mathHtml += '<div class="matrix-box" style="border-color:var(--ok)">'+
        '<div><span class="tag ONLINE">LIVE CLUSTER RS RECONSTRUCTION SUCCEEDED</span> Surviving used: <b>'+esc((rp.surviving_sources||[]).join(' + '))+'</b></div>'+
        (rp.recovered || []).map(r =>
          '<div style="margin-top:4px">Reconstructed <b>'+esc(r.chunk_id)+'</b>: <code>'+esc(r.sample_hex)+'</code> | "'+esc(r.sample_ascii)+'" '+
          '<span class="tag '+(r.sha256_match?'ONLINE':'OFFLINE')+'">'+(r.sha256_match?'SHA-256 MATCH':'HASH MISMATCH')+'</span></div>'
        ).join('') + '</div>';
    } else if (shards.length === n) {
      const chosenRows = [];
      for (let i = 0; i < n && chosenRows.length < k; ++i) {
        if (presentLive[i] && (shards[i].sample_bytes || []).length > 0) chosenRows.push(i);
      }
      const missingIdx = [];
      for (let i = 0; i < n; ++i) if (!presentLive[i]) missingIdx.push(i);
      if (missingIdx.length > 0 && chosenRows.length >= k) {
        const sub = new Array(k * k);
        for (let r = 0; r < k; ++r) for (let c = 0; c < k; ++c) sub[r*k+c] = mat[chosenRows[r]*k+c];
        const inv = gfInvert(sub, k);
        if (inv) {
          const bLen = Math.min(12, shards[chosenRows[0]].sample_bytes.length);
          const recData = [];
          for (let dCol = 0; dCol < k; ++dCol) {
            const arr = [];
            for (let b = 0; b < bLen; ++b) {
              let val = 0;
              for (let r = 0; r < k; ++r) val ^= gfMul(inv[dCol*k+r], shards[chosenRows[r]].sample_bytes[b]);
              arr.push(val);
            }
            recData.push(arr);
          }
          const recRows = [];
          for (const mi of missingIdx) {
            let recBytes = [];
            let formula = '';
            if (mi < k) {
              recBytes = recData[mi];
              formula = Array.from({length:k}, (_,r) => hex2(inv[mi*k+r])+'&middot;'+shards[chosenRows[r]].chunk_id.split('-').pop()).join(' &oplus; ');
            } else {
              for (let b = 0; b < bLen; ++b) {
                let v = 0;
                for (let c = 0; c < k; ++c) v ^= gfMul(mat[mi*k+c], recData[c][b]);
                recBytes.push(v);
              }
              formula = Array.from({length:k}, (_,c) => hex2(mat[mi*k+c])+'&middot;D'+c).join(' &oplus; ');
            }
            const origBytes = shards[mi].sample_bytes || [];
            const matchOk = origBytes.length >= bLen && recBytes.every((v, idx) => v === origBytes[idx]);
            recRows.push('<div>Recovered <b>'+esc(shards[mi].chunk_id)+'</b> = '+formula+
              ' &rArr; <code>'+recBytes.map(hexByte).join(' ')+'</code> | "'+esc(recBytes.map(x=>(x>=32&&x<=126)?String.fromCharCode(x):'.').join(''))+'" '+
              (matchOk ? '<span class="tag ONLINE">BYTE-IDENTICAL</span>' : '')+'</div>');
          }
          if (recRows.length) {
            mathHtml += '<div class="matrix-box" style="border-color:var(--cyan)">'+
              '<div><span class="tag INFO">'+(anySimulated?'INTERACTIVE GF(256) RECOVERY ON LIVE BYTES':'LIVE GF(256) RECOVERY PROOF')+'</span> Surviving: <b>'+
              esc(chosenRows.map(r=>shards[r].chunk_id.split('-').pop()).join(' + '))+'</b></div>'+
              recRows.join('')+'</div>';
          }
        }
      } else if (missingIdx.length > 0 && usableCount < k && shards.some(s => s.probed)) {
        mathHtml += '<div class="matrix-box" style="border-color:var(--bad)">'+
          '<span class="tag OFFLINE">UNRECOVERABLE / RECOVERY_PENDING</span> Only '+usableCount+' of '+n+' shards available (requires &ge; k='+k+' shards). Download safely refused until peers return!</div>';
      }
    }
  }

  bodyEl.innerHTML =
    '<div style="display:grid;gap:12px">'+
      '<div class="matrix-box">'+
        '<div style="margin-bottom:6px"><b>[DACA] Dynamic Adaptive Chunking Decision:</b> <span class="tag INFO">'+esc(d.daca.tier_rule)+'</span> &bull; Chunk Size: <b>'+fmtBytes(d.daca.shard_size)+'</b> &bull; Overhead: <b>'+Number(d.daca.overhead_ratio||1).toFixed(2)+'x</b> &bull; Total Pad: <b>'+d.daca.total_padding_bytes+' B</b></div>'+
        '<div class="daca-strip">'+dacaSegs+'</div>'+
      '</div>'+
      '<div class="grid-2">'+
        '<div class="matrix-box"><div style="margin-bottom:6px"><b>[GF(256) Matrix] Systematic Cauchy Encoding (poly 0x11D):</b></div>'+matRows+'</div>'+
        '<div class="matrix-box"><div style="margin-bottom:6px"><b>[Stripe '+(st?esc(st.stripe_id):'')+'] Live Peer Shards (Hex / ASCII &amp; Recovery Controls):</b></div>'+hexHtml+'</div>'+
      '</div>'+
      mathHtml +
    '</div>';
}
)HTML";

const char* DASH_PART_6 = R"HTML(
function renderHreraFlow() {
  const box = document.getElementById('hreraFlow');
  const badge = document.getElementById('hreraBadge');
  const ev = lastEvents.slice(-25);
  const hasRepl = ev.some(e => e.category === 'REPAIR_REPLICA_OK');
  const hasRs   = ev.some(e => e.category === 'REPAIR_RS_OK');
  const hasCorrupt = ev.some(e => e.category === 'SHARD_CORRUPT');
  const hasOff  = lastPeers.some(p => p.health === 'OFFLINE');
  const hasPend = lastCatalog.some(f => f.status === 'RECOVERY_PENDING' || f.status === 'DEGRADED');

  badge.className = 'tag ' + (hasPend ? 'WARN' : (hasRs || hasRepl ? 'ONLINE' : 'INFO'));
  badge.textContent = hasPend ? 'ACTIVE RECOVERY' : (hasRs ? 'RECENT: RS REPAIR' : (hasRepl ? 'RECENT: REPLICA COPY' : 'ARMED & HEALTHY'));

  const nodes = [
    {s:'STEP 1', t:'Peer Timeout / Hash Mismatch', c:(hasOff||hasCorrupt)?'lit-warn':'', d:'HPFD offline or REPORT_SHARD_ISSUE'},
    {s:'STEP 2', t:'Mark Shard Unavailable', c:(hasOff||hasCorrupt||hasPend)?'lit-warn':'', d:'Update manifest metadata status'},
    {s:'STEP 3', t:'Replica Available on Alt Peer?', c:hasRepl?'lit-cyan':(hasRs?'lit-warn':''), d:'Check ARMA secondary peers'},
    {s:'STEP 4 (YES)', t:'Fetch Replica (COPY_SHARD)', c:hasRepl?'lit-ok':'', d:'Direct P2P copy + SHA-256 verify'},
    {s:'STEP 5 (NO)', t:'Collect >= k Stripe Shards', c:hasRs?'lit-cyan':(hasPend?'lit-bad':''), d:'Gather valid shards from ONLINE peers'},
    {s:'STEP 6', t:'Enough Shards (>= k)?', c:hasRs?'lit-ok':(hasPend?'lit-bad':''), d:hasPend?'< k -> RECOVERY_PENDING backoff':'k surviving shards ready'},
    {s:'STEP 7 (YES)', t:'GF(256) RS Decode (REPAIR_SHARD)', c:hasRs?'lit-ok':'', d:'Reconstruct & verify SHA-256 on peer'},
    {s:'STEP 8-9', t:'Commit Disk & Update Metadata', c:(hasRepl||hasRs)?'lit-ok':'', d:'Atomic .tmp -> .chunk, status AVAILABLE'}
  ];
  box.innerHTML = nodes.map(n =>
    '<div class="fnode '+n.c+'"><div><b>'+n.s+'</b>: '+n.t+'</div><div class="sub">'+n.d+'</div></div>'
  ).join('');
}

function renderQuickCmds(fid) {
  const el = document.getElementById('cmdQuickStrip');
  el.innerHTML = '<span class="sub">CLI:</span> '+
    '<code class="cmd">rfe peers</code> '+
    '<code class="cmd">rfe info '+esc(fid)+'</code> '+
    '<code class="cmd">rfe download '+esc(fid)+'</code> '+
    '<code class="cmd">rfe repair '+esc(fid)+'</code>';
}

async function refresh() {
  try {
    const [st, pr, fl, ev] = await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/peers').then(r=>r.json()),
      fetch('/api/files').then(r=>r.json()),
      fetch('/api/events').then(r=>r.json())
    ]);
    lastStatus = st;
    lastPeers = pr.peers || [];
    lastCatalog = fl.files || [];
    lastEvents = ev.events || [];
    document.getElementById('liveClock').textContent = 'last sync: ' + new Date().toLocaleTimeString();

    const ch = st.cluster_health || 'HEALTHY';
    document.getElementById('summary').innerHTML =
      '<div class="kpi"><div class="sub">CLUSTER HEALTH</div><div class="val"><span class="tag '+ch+'">'+ch+'</span></div></div>'+
      '<div class="kpi"><div class="sub">PEERS ONLINE</div><div class="val">'+(st.peers_online||0)+' / '+(st.peers_total||0)+'</div></div>'+
      '<div class="kpi"><div class="sub">RS CODING POLICY</div><div class="val">RS('+(st.rs_k+st.rs_m)+','+st.rs_k+')</div></div>'+
      '<div class="kpi"><div class="sub">ARMA HOT REPLICAS</div><div class="val">'+(st.adaptive_replicas ? ('ON (k_rep='+(st.hot_replica_count||1)+')') : 'OFF')+'</div></div>'+
      '<div class="kpi"><div class="sub">TRACKED FILES</div><div class="val">'+(st.files_total||0)+'</div></div>';

    document.getElementById('peersBody').innerHTML = lastPeers.map(p => {
      const h = (p.workload && p.workload.is_busy && p.health==='ONLINE') ? 'BUSY' : p.health;
      return '<tr><td><b>'+esc(p.id)+'</b></td><td>'+esc(p.address)+':'+p.port+'</td><td><span class="tag '+h+'">'+h+'</span></td>'+
        '<td>'+fmtBytes(p.workload.available_storage_bytes)+'</td><td>'+p.workload.active_transfers+'/'+p.workload.transfer_capacity+'</td>'+
        '<td>'+(p.last_heartbeat_ms_ago>=0 ? p.last_heartbeat_ms_ago+' ms ago' : '-')+'</td></tr>';
    }).join('') || '<tr><td colspan="6" class="sub">No peers registered</td></tr>';

    document.getElementById('filesBody').innerHTML = lastCatalog.map(f => {
      const sel = (f.file_id === selectedFileId) ? ' active-pill' : '';
      return '<tr><td><b>'+esc(f.file_id)+'</b></td><td>'+esc(f.filename)+'</td><td>'+fmtBytes(f.size)+'</td>'+
        '<td>RS('+(f.k+f.m)+','+f.k+')</td><td>'+fmtBytes(f.shard_size||0)+'</td><td>'+f.stripes_count+'</td>'+
        '<td><span class="tag '+f.status+'">'+f.status+'</span></td>'+
        '<td class="row"><button class="sec'+sel+'" onclick="inspectVisualFlow(\''+esc(f.file_id)+'\',0,true)">Visualize Internals</button>'+
        '<button class="warn" onclick="repairFile(\''+esc(f.file_id)+'\')">Repair</button>'+
        '<a href="/api/download/'+esc(f.file_id)+'?token='+encodeURIComponent(sessionToken)+'"><button class="sec">Download</button></a></td></tr>';
    }).join('') || '<tr><td colspan="8" class="sub">No files uploaded yet</td></tr>';

    const tail = lastEvents.slice(-25).reverse();
    document.getElementById('eventsBox').innerHTML = tail.map(e => {
      const cat = e.category || 'EVENT';
      const cls = cat.includes('OFFLINE')||cat.includes('CORRUPT')||cat.includes('UNAVAILABLE') ? 'OFFLINE'
                : (cat.includes('REPAIR')||cat.includes('DISTRIBUTED') ? 'ONLINE'
                : (cat.includes('SUSPECT') ? 'SUSPECT' : 'INFO'));
      return '<span class="sub">['+esc(e.timestamp)+']</span> <span class="tag '+cls+'">'+esc(cat)+'</span> '+esc(e.message)+
        (e.details ? ' <span class="sub">'+esc(JSON.stringify(e.details))+'</span>' : '');
    }).join('\n') || 'PS C:\\RFE\\CLUSTER> no events recorded yet.';

    const healthSig = lastPeers.map(p=>p.id+':'+p.health).join(',') + '|' + lastCatalog.map(f=>f.file_id+':'+f.status).join(',');
    const sigChanged = (healthSig !== lastHealthSig);
    lastHealthSig = healthSig;

    if (!selectedFileId && lastCatalog.length > 0) {
      inspectVisualFlow(lastCatalog[0].file_id, 0, true);
    } else if (selectedFileId) {
      const alreadyProbed = lastInspectData && lastInspectData.file_id === selectedFileId &&
                            lastInspectData.inspected_stripe_index === selectedStripeIdx &&
                            lastInspectData.inspected_stripe && lastInspectData.inspected_stripe.probed;
      if (sigChanged || !alreadyProbed) {
        inspectVisualFlow(selectedFileId, selectedStripeIdx, true);
      } else {
        renderPeerRack();
        renderHreraFlow();
      }
    } else {
      renderPeerRack();
      renderHreraFlow();
      renderPipeline(null);
    }
  } catch (e) {}
}

if (sessionToken) {
  document.getElementById('authState').className = 'tag ONLINE';
  document.getElementById('authState').textContent = 'SESSION LOADED';
} else { login(); }
refresh();
setInterval(refresh, 1500);
</script>
</body>
</html>)HTML";

std::string build_dashboard_html() {
    std::string out;
    out.reserve(32000);
    out += DASH_PART_1;
    out += DASH_PART_2;
    out += DASH_PART_3;
    out += DASH_PART_4;
    out += DASH_PART_5;
    out += DASH_PART_6;
    return out;
}

}  // namespace

HttpServer::HttpServer(CoordinatorService& coordinator, std::string bind_address, uint16_t port)
    : coord_(coordinator), bind_address_(std::move(bind_address)), port_(port) {}

HttpServer::~HttpServer() { stop(); }

bool HttpServer::start() {
    if (port_ == 0) return true;
    if (!sock_init()) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (::inet_pton(AF_INET, bind_address_.c_str(), &addr.sin_addr) != 1) return false;

    for (int attempt = 0; attempt < 20; ++attempt) {
        socket_t fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd == INVALID_SOCK) return false;
#ifndef _WIN32
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));
#endif
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 &&
            ::listen(fd, 16) == 0) {
            std::lock_guard<std::mutex> lk(listen_mu_);
            listen_fd_ = fd;
            running_.store(true);
            accept_thread_ = std::thread(&HttpServer::accept_loop, this);
            return true;
        }
        const int e = sock_last_error();
        sock_close(fd);
        if (!sock_err_addr_in_use(e)) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

void HttpServer::close_listener() {
    std::lock_guard<std::mutex> lk(listen_mu_);
    if (listen_fd_ != INVALID_SOCK) {
        sock_close(listen_fd_);
        listen_fd_ = INVALID_SOCK;
    }
}

void HttpServer::stop() {
    const bool was = running_.exchange(false);
    if (was) {
        socket_t w = ::socket(AF_INET, SOCK_STREAM, 0);
        if (w != INVALID_SOCK) {
            sockaddr_in a{};
            a.sin_family = AF_INET;
            a.sin_port = htons(port_);
            const char* host = (bind_address_ == "0.0.0.0") ? "127.0.0.1" : bind_address_.c_str();
            ::inet_pton(AF_INET, host, &a.sin_addr);
            ::connect(w, reinterpret_cast<sockaddr*>(&a), sizeof(a));
            sock_close(w);
        }
    }
    if (accept_thread_.joinable()) accept_thread_.join();
    close_listener();
    for (int i = 0; i < 200 && active_.load() > 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void HttpServer::accept_loop() {
    socket_t lfd = INVALID_SOCK;
    {
        std::lock_guard<std::mutex> lk(listen_mu_);
        lfd = listen_fd_;
    }
    while (running_.load() && lfd != INVALID_SOCK) {
        sockaddr_in cli{};
        socklen_t len = sizeof(cli);
        socket_t c = ::accept(lfd, reinterpret_cast<sockaddr*>(&cli), &len);
        if (c == INVALID_SOCK) {
            if (!running_.load()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;
        }
        if (!running_.load()) {
            sock_close(c);
            break;
        }
        ++active_;
        std::thread([this, c]() {
            handle_client(c);
            --active_;
        }).detach();
    }
}

void HttpServer::handle_client(socket_t client_fd) {
    proto::tune_socket(client_fd);
    proto::set_timeouts(client_fd, 15000, 15000);

    std::string raw_hdr;
    char ch = 0;
    while (raw_hdr.size() < 65536) {
        proto::RecvStatus st = proto::recv_all(client_fd, &ch, 1);
        if (st != proto::RecvStatus::OK) {
            sock_close(client_fd);
            return;
        }
        raw_hdr.push_back(ch);
        if (raw_hdr.size() >= 4 && raw_hdr.compare(raw_hdr.size() - 4, 4, "\r\n\r\n") == 0) {
            break;
        }
    }

    size_t line_end = raw_hdr.find("\r\n");
    if (line_end == std::string::npos) {
        sock_close(client_fd);
        return;
    }
    std::istringstream req_line(raw_hdr.substr(0, line_end));
    std::string method, target, proto_ver;
    req_line >> method >> target >> proto_ver;

    std::map<std::string, std::string> headers;
    size_t pos = line_end + 2;
    while (pos < raw_hdr.size()) {
        size_t next = raw_hdr.find("\r\n", pos);
        if (next == std::string::npos || next == pos) break;
        std::string line = raw_hdr.substr(pos, next - pos);
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            headers[to_lower(trim(line.substr(0, colon)))] = trim(line.substr(colon + 1));
        }
        pos = next + 2;
    }

    if (headers.count("expect") && to_lower(headers["expect"]) == "100-continue") {
        const std::string cont = "HTTP/1.1 100 Continue\r\n\r\n";
        proto::send_all(client_fd, cont.data(), cont.size());
    }

    size_t content_len = 0;
    if (headers.count("content-length")) {
        try { content_len = static_cast<size_t>(std::stoull(headers["content-length"])); } catch (...) {}
    }
    if (content_len > 64ULL * 1024ULL * 1024ULL) {
        json::Json err = json::Json::object();
        err["error"] = "payload exceeds 64 MB HTTP limit (use rfe upload for larger files)";
        send_json(client_fd, 413, "Payload Too Large", err);
        sock_close(client_fd);
        return;
    }

    std::vector<uint8_t> body(content_len);
    if (content_len > 0) {
        if (proto::recv_all(client_fd, body.data(), content_len) != proto::RecvStatus::OK) {
            sock_close(client_fd);
            return;
        }
    }

    std::string path = target;
    std::string query;
    size_t qpos = target.find('?');
    if (qpos != std::string::npos) {
        path = target.substr(0, qpos);
        query = target.substr(qpos + 1);
    }

    if (method == "OPTIONS") {
        send_http_response(client_fd, 204, "No Content", "text/plain", "");
        sock_close(client_fd);
        return;
    }

    std::string presented_token;
    if (headers.count("authorization")) {
        const std::string& auth = headers["authorization"];
        if (auth.size() > 7 && to_lower(auth.substr(0, 7)) == "bearer ") {
            presented_token = trim(auth.substr(7));
        }
    }
    if (presented_token.empty() && headers.count("x-auth-token")) {
        presented_token = headers["x-auth-token"];
    }
    if (presented_token.empty() && !query.empty()) {
        presented_token = get_query_param(query, "token");
    }

    auto require_auth = [&]() -> bool {
        if (!coord_.require_auth()) return true;
        if (coord_.sessions().validate_token(presented_token, nullptr, true)) return true;
        json::Json err = json::Json::object();
        err["error"] = "unauthorized: missing or invalid session/auth token";
        send_json(client_fd, 401, "Unauthorized", err);
        return false;
    };

    if (method == "GET" && path == "/") {
        send_http_response(client_fd, 200, "OK", "text/html; charset=utf-8", build_dashboard_html());
        sock_close(client_fd);
        return;
    }

    if (method == "POST" && path == "/api/auth") {
        std::string token, client_id = "http-client";
        try {
            auto j = json::Json::parse(std::string(body.begin(), body.end()));
            if (j.contains("token")) token = j["token"].as_string();
            if (j.contains("client_id")) client_id = j["client_id"].as_string();
        } catch (...) {}
        if (token.empty()) token = presented_token;

        ClientSession cs;
        std::string err;
        if (!coord_.sessions().authenticate(client_id, token, cs, err)) {
            json::Json ej = json::Json::object();
            ej["error"] = err;
            send_json(client_fd, 401, "Unauthorized", ej);
            sock_close(client_fd);
            return;
        }
        json::Json ok = json::Json::object();
        ok["status"] = "OK";
        ok["session_id"] = cs.session_id;
        ok["session_token"] = cs.session_token;
        ok["client_id"] = cs.client_id;
        send_json(client_fd, 200, "OK", ok);
        sock_close(client_fd);
        return;
    }

    if (method == "GET" && path == "/api/status") {
        coord_.registry().evaluate_health(coord_.heartbeat_timeout_ms(), coord_.failure_threshold());
        auto peers = coord_.registry().get_all_peers();
        size_t online = 0, offline = 0, suspect = 0;
        for (const auto& p : peers) {
            if (p.state == PeerHealth::ONLINE) ++online;
            else if (p.state == PeerHealth::SUSPECT) ++suspect;
            else ++offline;
        }
        auto files = coord_.catalog().list_files();
        json::Json j = json::Json::object();
        j["cluster_health"] = (offline > 0) ? "DEGRADED" : "HEALTHY";
        j["peers_total"] = static_cast<long long>(peers.size());
        j["peers_online"] = static_cast<long long>(online);
        j["peers_suspect"] = static_cast<long long>(suspect);
        j["peers_offline"] = static_cast<long long>(offline);
        j["files_total"] = static_cast<long long>(files.size());
        j["rs_k"] = static_cast<long long>(coord_.rs_k());
        j["rs_m"] = static_cast<long long>(coord_.rs_m());
        j["adaptive_replicas"] = coord_.adaptive_replicas();
        j["hot_replica_count"] = static_cast<long long>(coord_.hot_replica_count());
        j["active_sessions"] = static_cast<long long>(coord_.sessions().active_session_count());
        send_json(client_fd, 200, "OK", j);
        sock_close(client_fd);
        return;
    }

    if (method == "GET" && path == "/api/peers") {
        coord_.registry().evaluate_health(coord_.heartbeat_timeout_ms(), coord_.failure_threshold());
        json::Json j = json::Json::object();
        j["peers"] = coord_.registry().to_json();
        send_json(client_fd, 200, "OK", j);
        sock_close(client_fd);
        return;
    }

    if (method == "GET" && path == "/api/files") {
        auto files = coord_.catalog().list_files();
        json::Json arr = json::Json::array();
        for (const auto& f : files) arr.push_back(f.to_json());
        json::Json j = json::Json::object();
        j["files"] = arr;
        send_json(client_fd, 200, "OK", j);
        sock_close(client_fd);
        return;
    }

    if (method == "GET" && path.rfind("/api/files/", 0) == 0) {
        const std::string fid = url_decode(path.substr(11));
        FileRecord rec;
        if (!coord_.catalog().get_file(fid, rec)) {
            json::Json ej = json::Json::object();
            ej["error"] = "file not found: " + fid;
            send_json(client_fd, 404, "Not Found", ej);
            sock_close(client_fd);
            return;
        }
        send_json(client_fd, 200, "OK", rec.to_json());
        sock_close(client_fd);
        return;
    }

    if (method == "GET" && path.rfind("/api/inspect/", 0) == 0) {
        const std::string fid = url_decode(path.substr(13));
        const bool probe = get_query_param(query, "probe") == "1";
        if (probe && !require_auth()) {
            sock_close(client_fd);
            return;
        }

        FileRecord rec;
        if (!coord_.catalog().get_file(fid, rec)) {
            json::Json ej = json::Json::object();
            ej["error"] = "file not found: " + fid;
            send_json(client_fd, 404, "Not Found", ej);
            sock_close(client_fd);
            return;
        }

        coord_.registry().evaluate_health(coord_.heartbeat_timeout_ms(), coord_.failure_threshold());
        std::map<std::string, PeerInfo> peer_map;
        for (const auto& p : coord_.registry().get_all_peers()) peer_map[p.id] = p;

        const size_t k = rec.k > 0 ? rec.k : coord_.rs_k();
        const size_t m = rec.m > 0 ? rec.m : coord_.rs_m();
        const size_t n = k + m;
        ReedSolomon rs(k, m);

        // Group shards by stripe sorted strictly by numeric min_seq
        struct StripeBucket {
            std::string stripe_id;
            uint64_t min_seq = std::numeric_limits<uint64_t>::max();
            std::vector<ShardMeta> shards;
        };
        std::map<std::string, size_t> sidx;
        std::vector<StripeBucket> stripes;
        uint64_t total_orig = 0, total_pad = 0, total_parity = 0;
        size_t arma_hot_replicas = 0;
        json::Json all_shards_json = json::Json::array();

        for (const auto& sh : rec.shards) {
            all_shards_json.push_back(sh.to_json());
            if (sh.shard_type == "PARITY") total_parity += sh.size;
            else {
                total_orig += sh.original_data_length;
                total_pad += sh.padding_length;
            }
            if (sh.peers.size() > 1) ++arma_hot_replicas;
            auto it = sidx.find(sh.stripe_id);
            if (it == sidx.end()) {
                sidx[sh.stripe_id] = stripes.size();
                stripes.push_back({sh.stripe_id, sh.sequence, {sh}});
            } else {
                stripes[it->second].min_seq = std::min(stripes[it->second].min_seq, sh.sequence);
                stripes[it->second].shards.push_back(sh);
            }
        }
        std::sort(stripes.begin(), stripes.end(), [](const StripeBucket& a, const StripeBucket& b) {
            if (a.min_seq != b.min_seq) return a.min_seq < b.min_seq;
            return a.stripe_id < b.stripe_id;
        });

        size_t req_stripe = 0;
        const std::string sparam = get_query_param(query, "stripe");
        if (!sparam.empty()) {
            try { req_stripe = static_cast<size_t>(std::stoull(sparam)); } catch (...) {}
        }
        if (!stripes.empty() && req_stripe >= stripes.size()) req_stripe = stripes.size() - 1;

        json::Json stripes_summary = json::Json::array();
        for (const auto& sb : stripes) {
            json::Json s = json::Json::object();
            s["stripe_id"] = sb.stripe_id;
            s["shards"] = static_cast<long long>(sb.shards.size());
            stripes_summary.push_back(s);
        }

        json::Json enc_matrix = json::Json::array();
        for (uint8_t v : rs.encoding_matrix()) enc_matrix.push_back(static_cast<long long>(v));

        json::Json insp_stripe = json::Json::object();
        json::Json parity_check = json::Json::object();
        parity_check["checked"] = false;

        if (!stripes.empty()) {
            auto& bucket = stripes[req_stripe];
            // Sort shards inside stripe: DATA D0..Dk-1 then PARITY P0..Pm-1
            auto slot_index = [&](const ShardMeta& sm) -> int {
                size_t p = sm.chunk_id.rfind('-');
                std::string tail = (p != std::string::npos) ? sm.chunk_id.substr(p + 1) : sm.chunk_id;
                if (!tail.empty() && tail[0] == 'D') {
                    try { return std::stoi(tail.substr(1)); } catch (...) {}
                } else if (!tail.empty() && tail[0] == 'P') {
                    try { return static_cast<int>(k) + std::stoi(tail.substr(1)); } catch (...) {}
                }
                return static_cast<int>(sm.sequence);
            };
            std::sort(bucket.shards.begin(), bucket.shards.end(), [&](const ShardMeta& a, const ShardMeta& b) {
                return slot_index(a) < slot_index(b);
            });

            std::vector<std::vector<uint8_t>> live_bufs(n);
            std::vector<bool> live_ok(n, false);
            json::Json sh_arr = json::Json::array();

            for (size_t i = 0; i < bucket.shards.size() && i < n; ++i) {
                const ShardMeta& sm = bucket.shards[i];
                json::Json sj = sm.to_json();
                sj["probed"] = probe;
                json::Json peers_detail = json::Json::array();
                bool any_verified = false;
                bool any_mismatch = false;
                std::string served_by;
                std::vector<uint8_t> verified_payload;

                for (const auto& pid : sm.peers) {
                    json::Json pj = json::Json::object();
                    pj["id"] = pid;
                    auto pit = peer_map.find(pid);
                    const bool online = (pit != peer_map.end() && pit->second.state == PeerHealth::ONLINE);
                    pj["health"] = (pit != peer_map.end()) ? peer_health_str(pit->second.state) : "UNKNOWN";
                    if (probe && online && !any_verified) {
                        std::vector<uint8_t> pl;
                        if (fetch_chunk_from_peer(pit->second, sm.chunk_id, pl)) {
                            const bool sha_ok = (pl.size() == sm.size &&
                                                 ChunkManager::sha256_of_bytes(pl) == sm.sha256);
                            pj["sha_ok"] = sha_ok;
                            if (sha_ok) {
                                any_verified = true;
                                served_by = pid;
                                verified_payload = std::move(pl);
                            } else {
                                any_mismatch = true;
                            }
                        }
                    }
                    peers_detail.push_back(pj);
                }

                sj["peers"] = peers_detail;
                sj["live_reachable"] = any_verified;
                sj["verified_sha256"] = any_verified;
                sj["sha256_mismatch"] = (!any_verified && any_mismatch);
                sj["served_by"] = served_by;

                if (any_verified) {
                    live_bufs[i] = verified_payload;
                    live_ok[i] = true;
                    sj["sample_hex"] = bytes_to_hex(verified_payload, 12);
                    sj["sample_ascii"] = bytes_to_ascii(verified_payload, 12);
                    json::Json sb = json::Json::array();
                    const size_t nbytes = std::min<size_t>(12, verified_payload.size());
                    for (size_t b = 0; b < nbytes; ++b) sb.push_back(static_cast<long long>(verified_payload[b]));
                    sj["sample_bytes"] = sb;
                } else {
                    sj["sample_bytes"] = json::Json::array();
                }
                sh_arr.push_back(sj);
            }

            if (probe) {
                // Verify real GF(256) parity equation if all D0..Dk-1 and P0 are live
                bool all_data_live = (bucket.shards.size() >= k + 1);
                for (size_t d = 0; d < k; ++d) if (!live_ok[d]) all_data_live = false;
                if (all_data_live && live_ok[k] && rec.shard_size > 0) {
                    const auto& gf = GF256::instance();
                    const auto& mat = rs.encoding_matrix();
                    const size_t check_len = std::min<size_t>(16, live_bufs[0].size());
                    bool match = true;
                    for (size_t b = 0; b < check_len; ++b) {
                        uint8_t acc = 0;
                        for (size_t d = 0; d < k; ++d) {
                            acc ^= gf.mul(mat[k * k + d], live_bufs[d][b]);
                        }
                        if (acc != live_bufs[k][b]) { match = false; break; }
                    }
                    std::ostringstream eq;
                    eq << "b[0]: ";
                    for (size_t d = 0; d < k; ++d) {
                        if (d > 0) eq << " ^ ";
                        eq << "0x" << std::uppercase << std::hex << std::setfill('0') << std::setw(2)
                           << static_cast<unsigned>(mat[k * k + d]) << "*0x" << std::setw(2)
                           << static_cast<unsigned>(live_bufs[d][0]);
                    }
                    eq << " = 0x" << std::setw(2) << static_cast<unsigned>(live_bufs[k][0])
                       << (match ? " (MATCHES P0 on Peer!)" : " (MISMATCH!)");
                    parity_check["checked"] = true;
                    parity_check["verified"] = match;
                    parity_check["equation"] = eq.str();
                }

                // If any shard in this stripe is missing/corrupt on cluster right now and >= k live shards exist, run real rs.decode()!
                size_t live_cnt = 0;
                for (size_t i = 0; i < n; ++i) if (live_ok[i]) ++live_cnt;
                if (live_cnt < n && live_cnt >= k && rec.shard_size > 0 && bucket.shards.size() == n) {
                    std::vector<std::vector<uint8_t>> work = live_bufs;
                    for (size_t i = 0; i < n; ++i) {
                        if (!live_ok[i]) work[i].assign(rec.shard_size, 0);
                    }
                    std::vector<uint8_t*> ptrs(n);
                    for (size_t i = 0; i < n; ++i) ptrs[i] = work[i].data();
                    if (rs.decode(ptrs, live_ok, rec.shard_size)) {
                        json::Json lr = json::Json::object();
                        lr["performed"] = true;
                        json::Json srcs = json::Json::array();
                        json::Json rec_arr = json::Json::array();
                        for (size_t i = 0; i < n; ++i) {
                            if (live_ok[i]) {
                                srcs.push_back(bucket.shards[i].chunk_id + " @ " +
                                               (bucket.shards[i].peers.empty() ? "?" : bucket.shards[i].peers.front()));
                            } else {
                                json::Json ri = json::Json::object();
                                ri["chunk_id"] = bucket.shards[i].chunk_id;
                                ri["sha256_match"] = (ChunkManager::sha256_of_bytes(work[i]) == bucket.shards[i].sha256);
                                ri["sample_hex"] = bytes_to_hex(work[i], 12);
                                ri["sample_ascii"] = bytes_to_ascii(work[i], 12);
                                rec_arr.push_back(ri);
                            }
                        }
                        lr["surviving_sources"] = srcs;
                        lr["recovered"] = rec_arr;
                        insp_stripe["live_recovery_proof"] = lr;
                    }
                }
            }

            insp_stripe["stripe_id"] = bucket.stripe_id;
            insp_stripe["probed"] = probe;
            insp_stripe["shards"] = sh_arr;
        }

        const uint64_t total_stored = total_orig + total_pad + total_parity;
        json::Json daca = json::Json::object();
        daca["shard_size"] = static_cast<long long>(rec.shard_size);
        daca["k"] = static_cast<long long>(k);
        daca["m"] = static_cast<long long>(m);
        daca["n"] = static_cast<long long>(n);
        daca["stripes_total"] = static_cast<long long>(stripes.size());
        daca["tier_rule"] = daca_tier_description(rec.size, rec.shard_size);
        daca["total_original_bytes"] = static_cast<long long>(total_orig);
        daca["total_padding_bytes"] = static_cast<long long>(total_pad);
        daca["total_parity_bytes"] = static_cast<long long>(total_parity);
        daca["overhead_ratio"] = rec.size > 0 ? static_cast<double>(total_stored) / static_cast<double>(rec.size) : 1.0;

        json::Json out = json::Json::object();
        out["file_id"] = rec.file_id;
        out["filename"] = rec.filename;
        out["size"] = static_cast<long long>(rec.size);
        out["sha256"] = rec.sha256;
        out["status"] = file_status_str(rec.status);
        out["shards_count"] = static_cast<long long>(rec.shards.size());
        out["arma_hot_replicas"] = static_cast<long long>(arma_hot_replicas);
        out["daca"] = daca;
        out["encoding_matrix"] = enc_matrix;
        out["stripes"] = stripes_summary;
        out["all_shards"] = all_shards_json;
        out["inspected_stripe_index"] = static_cast<long long>(req_stripe);
        out["inspected_stripe"] = insp_stripe;
        out["parity_check"] = parity_check;
        send_json(client_fd, 200, "OK", out);
        sock_close(client_fd);
        return;
    }

    if (method == "GET" && path == "/api/events") {
        send_json(client_fd, 200, "OK", coord_.read_events());
        sock_close(client_fd);
        return;
    }

    if (method == "POST" && path == "/api/upload") {
        if (!require_auth()) { sock_close(client_fd); return; }

        std::string filename = "upload.bin";
        std::vector<uint8_t> file_bytes;
        if (headers.count("content-type") &&
            to_lower(headers["content-type"]).find("multipart/form-data") != std::string::npos) {
            if (!parse_multipart_file(headers["content-type"], body, filename, file_bytes)) {
                json::Json ej = json::Json::object();
                ej["error"] = "invalid multipart upload payload";
                send_json(client_fd, 400, "Bad Request", ej);
                sock_close(client_fd);
                return;
            }
        } else {
            if (headers.count("x-filename") && !headers["x-filename"].empty()) {
                filename = fs::path(headers["x-filename"]).filename().string();
            } else if (!query.empty()) {
                std::string qf = get_query_param(query, "filename");
                if (!qf.empty()) filename = fs::path(qf).filename().string();
            }
            file_bytes = std::move(body);
        }

        FileRecord rec;
        std::string err;
        if (!coord_.catalog().stage_bytes(filename, file_bytes, rec, err)) {
            json::Json ej = json::Json::object();
            ej["error"] = err;
            send_json(client_fd, 500, "Internal Server Error", ej);
            sock_close(client_fd);
            return;
        }

        coord_.process_uploaded_file(rec.file_id, rec);
        send_json(client_fd, 200, "OK", rec.to_json());
        sock_close(client_fd);
        return;
    }

    if (method == "POST" && path.rfind("/api/repair/", 0) == 0) {
        if (!require_auth()) { sock_close(client_fd); return; }
        const std::string fid = url_decode(path.substr(12));
        std::string report;
        bool ok = coord_.repair_file(fid, report);
        json::Json j = json::Json::object();
        j["status"] = ok ? "OK" : "PENDING_OR_FAILED";
        j["report"] = report;
        send_json(client_fd, 200, "OK", j);
        sock_close(client_fd);
        return;
    }

    if (method == "GET" && path.rfind("/api/download/", 0) == 0) {
        if (!require_auth()) { sock_close(client_fd); return; }
        const std::string fid = url_decode(path.substr(14));
        FileRecord rec;
        if (!coord_.catalog().get_file(fid, rec) || !fs::exists(rec.central_path)) {
            json::Json ej = json::Json::object();
            ej["error"] = "file or central backup not found: " + fid;
            send_json(client_fd, 404, "Not Found", ej);
            sock_close(client_fd);
            return;
        }
        std::ifstream in(rec.central_path, std::ios::binary);
        std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        send_http_response(client_fd, 200, "OK", "application/octet-stream", data,
                           "Content-Disposition: attachment; filename=\"" + rec.filename + "\"\r\n");
        sock_close(client_fd);
        return;
    }

    json::Json ej = json::Json::object();
    ej["error"] = "not found";
    send_json(client_fd, 404, "Not Found", ej);
    sock_close(client_fd);
}
