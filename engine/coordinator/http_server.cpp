// ============================================================================
// http_server.cpp - Native HTTP/1.1 Upload API & Live Cluster Console
// ============================================================================
#include "http_server.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <vector>

#include "../communication/protocol.hpp"
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

const char* DASHBOARD_HTML = R"HTML(<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Resilient Multi-Peer File Storage & Recovery</title>
<style>
  :root { --bg:#0f172a; --card:#1e293b; --border:#334155; --text:#e2e8f0; --muted:#94a3b8;
          --ok:#22c55e; --warn:#f59e0b; --bad:#ef4444; --accent:#38bdf8; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--text); }
  header { padding:16px 24px; background:#020617; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px; }
  h1 { margin:0; font-size:18px; font-weight:700; color:var(--accent); }
  .sub { font-size:12px; color:var(--muted); }
  .wrap { max-width:1240px; margin:0 auto; padding:20px; display:grid; gap:16px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:12px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:16px; }
  .card h2 { margin:0 0 12px; font-size:15px; color:#f8fafc; display:flex; justify-content:space-between; align-items:center; }
  .stat { font-size:24px; font-weight:700; }
  .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  input, button { padding:8px 12px; border-radius:6px; border:1px solid var(--border); background:#0f172a; color:var(--text); font-size:13px; }
  button { background:#0284c7; border-color:#0284c7; color:white; cursor:pointer; font-weight:600; }
  button:hover { background:#0369a1; }
  button.sec { background:#334155; border-color:#475569; }
  button.warn { background:#d97706; border-color:#d97706; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }
  th { color:var(--muted); font-weight:600; font-size:11px; text-transform:uppercase; }
  .badge { padding:2px 8px; border-radius:999px; font-size:11px; font-weight:700; display:inline-block; }
  .ONLINE,.AVAILABLE,.HEALTHY { background:rgba(34,197,94,.18); color:var(--ok); }
  .SUSPECT,.DEGRADED,.BUSY,.RECOVERING,.RECOVERY_PENDING { background:rgba(245,158,11,.18); color:var(--warn); }
  .OFFLINE,.UNAVAILABLE,.CORRUPT,.UNRECOVERABLE { background:rgba(239,68,68,.18); color:var(--bad); }
  .STORED { background:rgba(56,189,248,.18); color:var(--accent); }
  .shards { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
  .shard { padding:5px 8px; border-radius:6px; border:1px solid var(--border); background:#0f172a; font-size:11px; }
  pre { background:#020617; padding:10px; border-radius:6px; font-size:12px; max-height:220px; overflow:auto; margin:0; color:#cbd5e1; }
</style>
</head>
<body>
<header>
  <div>
    <h1>Resilient Multi-Peer File Storage &amp; Recovery</h1>
    <div class="sub">Central Coordination Server &bull; Systematic RS Erasure Coding &bull; SHA-256 Integrity &bull; P2P Durability Repair</div>
  </div>
  <div class="row">
    <input id="tok" type="password" placeholder="Cluster Auth Token" value="rfe-demo-token" style="width:170px">
    <button onclick="login()">Authenticate</button>
    <span id="authState" class="badge STORED">Not Authenticated</span>
  </div>
</header>
<div class="wrap">
  <div class="grid" id="summary"></div>
  <div class="card">
    <h2>1. Upload File (HTTP Upload API &rarr; Chunk &rarr; SHA-256 &rarr; RS Encode &rarr; Distribute)</h2>
    <div class="row">
      <input type="file" id="fileInput">
      <button onclick="uploadFile()">Upload &amp; Distribute</button>
      <span id="uploadMsg" class="sub"></span>
    </div>
  </div>
  <div class="card">
    <h2>2. Participating Storage Peers (Heartbeat Monitor)</h2>
    <table><thead><tr><th>Peer ID</th><th>Endpoint</th><th>Health</th><th>Free Storage</th><th>Transfers</th><th>Last Heartbeat</th></tr></thead>
    <tbody id="peersBody"></tbody></table>
  </div>
  <div class="card">
    <h2>3. File Catalogue &amp; Stripe Map</h2>
    <table><thead><tr><th>File ID</th><th>Filename</th><th>Size</th><th>Policy</th><th>Stripes</th><th>Status</th><th>Actions</th></tr></thead>
    <tbody id="filesBody"></tbody></table>
    <div id="manifestBox" style="margin-top:12px"></div>
  </div>
  <div class="card">
    <h2>4. Recovery &amp; Audit Events</h2>
    <pre id="eventsBox">Loading...</pre>
  </div>
</div>
<script>
let sessionToken = localStorage.getItem('rfe_token') || '';
function fmtBytes(b) {
  if (b >= 1073741824) return (b/1073741824).toFixed(2)+' GB';
  if (b >= 1048576) return (b/1048576).toFixed(2)+' MB';
  if (b >= 1024) return (b/1024).toFixed(2)+' KB';
  return b+' B';
}
function hdrs(extra={}) {
  return sessionToken ? Object.assign({'Authorization':'Bearer '+sessionToken}, extra) : extra;
}
async function login() {
  const token = document.getElementById('tok').value;
  const r = await fetch('/api/auth', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({token, client_id:'web-console'})});
  const d = await r.json();
  if (r.ok && d.session_token) {
    sessionToken = d.session_token;
    localStorage.setItem('rfe_token', sessionToken);
    document.getElementById('authState').className = 'badge ONLINE';
    document.getElementById('authState').textContent = 'Session ' + d.session_id;
  } else {
    document.getElementById('authState').className = 'badge OFFLINE';
    document.getElementById('authState').textContent = d.error || 'Auth Failed';
  }
}
async function uploadFile() {
  const inp = document.getElementById('fileInput');
  if (!inp.files.length) return alert('Choose a file first');
  const f = inp.files[0];
  document.getElementById('uploadMsg').textContent = 'Uploading & distributing...';
  const r = await fetch('/api/upload', {method:'POST', headers:hdrs({'X-Filename':f.name}), body:f});
  const d = await r.json();
  if (r.ok) {
    document.getElementById('uploadMsg').textContent = 'Uploaded ' + d.file_id + ' (' + d.status + ', ' + d.stripes_count + ' stripes)';
    refresh();
    inspectFile(d.file_id);
  } else {
    document.getElementById('uploadMsg').textContent = 'Error: ' + (d.error || r.status);
  }
}
async function inspectFile(fid) {
  const r = await fetch('/api/files/' + encodeURIComponent(fid), {headers:hdrs()});
  if (!r.ok) return;
  const f = await r.json();
  let html = '<div class="sub"><b>'+f.file_id+' ('+f.filename+')</b> &bull; SHA-256: <code>'+f.sha256+'</code> &bull; Shard Size: '+fmtBytes(f.shard_size)+'</div><div class="shards">';
  for (const s of (f.shards || [])) {
    const st = s.status || 'HEALTHY';
    const peers = (s.peers || []).join(',') || 'none';
    html += '<div class="shard"><div><b>'+s.chunk_id+'</b> <span class="badge '+st+'">'+st+'</span></div><div class="sub">Peer: '+peers+' &bull; '+s.shard_type+'</div></div>';
  }
  html += '</div>';
  document.getElementById('manifestBox').innerHTML = html;
}
async function repairFile(fid) {
  const r = await fetch('/api/repair/' + encodeURIComponent(fid), {method:'POST', headers:hdrs()});
  const d = await r.json();
  alert((d.status || '') + '\n' + (d.report || d.error || ''));
  refresh();
  inspectFile(fid);
}
async function refresh() {
  try {
    const [st, pr, fl, ev] = await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/peers').then(r=>r.json()),
      fetch('/api/files').then(r=>r.json()),
      fetch('/api/events').then(r=>r.json())
    ]);
    document.getElementById('summary').innerHTML =
      '<div class="card"><div class="sub">Cluster Health</div><div class="stat">'+(st.cluster_health||'HEALTHY')+'</div></div>'+
      '<div class="card"><div class="sub">Peers Online</div><div class="stat">'+(st.peers_online||0)+' / '+(st.peers_total||0)+'</div></div>'+
      '<div class="card"><div class="sub">RS Coding Policy</div><div class="stat">RS('+(st.rs_k+st.rs_m)+','+st.rs_k+')</div></div>'+
      '<div class="card"><div class="sub">Tracked Files</div><div class="stat">'+(st.files_total||0)+'</div></div>';
    document.getElementById('peersBody').innerHTML = (pr.peers||[]).map(p => {
      const h = p.workload && p.workload.is_busy && p.health==='ONLINE' ? 'BUSY' : p.health;
      return '<tr><td><b>'+p.id+'</b></td><td>'+p.address+':'+p.port+'</td><td><span class="badge '+h+'">'+h+'</span></td>'+
        '<td>'+fmtBytes(p.workload.available_storage_bytes)+'</td><td>'+p.workload.active_transfers+'/'+p.workload.transfer_capacity+'</td>'+
        '<td>'+(p.last_heartbeat_ms_ago>=0 ? p.last_heartbeat_ms_ago+' ms ago' : '-')+'</td></tr>';
    }).join('') || '<tr><td colspan="6" class="sub">No peers registered</td></tr>';
    document.getElementById('filesBody').innerHTML = (fl.files||[]).map(f =>
      '<tr><td><b>'+f.file_id+'</b></td><td>'+f.filename+'</td><td>'+fmtBytes(f.size)+'</td>'+
      '<td>RS('+(f.k+f.m)+','+f.k+')</td><td>'+f.stripes_count+'</td><td><span class="badge '+f.status+'">'+f.status+'</span></td>'+
      '<td class="row"><button class="sec" onclick="inspectFile(\''+f.file_id+'\')">Shards</button>'+
      '<button class="warn" onclick="repairFile(\''+f.file_id+'\')">Repair</button>'+
      '<a href="/api/download/'+f.file_id+'?token='+encodeURIComponent(sessionToken)+'"><button class="sec">Download</button></a></td></tr>'
    ).join('') || '<tr><td colspan="7" class="sub">No files uploaded</td></tr>';
    const events = (ev.events||[]).slice(-15).reverse();
    document.getElementById('eventsBox').textContent = events.map(e => '['+e.timestamp+'] '+e.category+': '+e.message).join('\n') || 'No events yet.';
  } catch (e) {}
}
if (sessionToken) {
  document.getElementById('authState').className = 'badge ONLINE';
  document.getElementById('authState').textContent = 'Token Loaded';
} else { login(); }
refresh();
setInterval(refresh, 1500);
</script>
</body>
</html>)HTML";

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

    // Extract Bearer / X-Auth-Token / ?token=
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
        send_http_response(client_fd, 200, "OK", "text/html; charset=utf-8", DASHBOARD_HTML);
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
