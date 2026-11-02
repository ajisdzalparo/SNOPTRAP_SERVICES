package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for mobile browser & app access
	},
}

type Client struct {
	conn   *websocket.Conn
	roomID string
	isHost bool // true = SnoopTrap laptop, false = phone viewer
	send   chan []byte
}

type Room struct {
	host      *Client
	viewers   map[*Client]bool
	lastFrame []byte
	mu        sync.RWMutex
}

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.RWMutex
)

// viewerHTML is the mobile/desktop live surveillance player. It uses PeerJS (WebRTC P2P):
// the viewer calls the laptop's PeerJS peer (id = room) via the public PeerJS cloud broker
// + STUN (and TURN if configured), and the laptop answers with its live camera + mic track.
// WebRTC negotiates Opus audio natively, so sound works on iPhone Safari (unlike raw webm).
// The viewer keeps retrying the call every few seconds so it connects as soon as the laptop
// peer registers. Keep in sync with the laptop peer in SNOOPTRAP frontend/src/App.tsx.
const viewerHTML = `<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>SnoopTrap // Remote Surveillance Feed</title>
  <script src="https://unpkg.com/peerjs@1.5.4/dist/peerjs.min.js"></script>
  <style>
    * { margin:0; padding:0; box-sizing:border-box; }
    body { background:#050505; color:#fff; font-family:-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; min-height:100vh; display:flex; flex-direction:column; align-items:center; justify-content:center; padding:12px; }
    .card { width:100%; max-width:640px; background:#0a0a0a; border:2px solid #ff003c; border-radius:10px; overflow:hidden; box-shadow:0 0 35px rgba(255,0,60,0.35); }
    .header { padding:14px 16px; background:#111; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #222; }
    .brand { display:flex; align-items:center; gap:8px; font-weight:900; letter-spacing:1px; color:#ff003c; font-size:14px; }
    .dot { width:10px; height:10px; background:#ff003c; border-radius:50%; display:inline-block; animation:pulse 1s infinite; }
    @keyframes pulse { 0%{opacity:1;transform:scale(1);} 50%{opacity:0.3;transform:scale(0.85);} 100%{opacity:1;transform:scale(1);} }
    .badge { font-size:11px; font-weight:700; padding:3px 8px; border-radius:4px; background:#1a1a1a; color:#00ff41; border:1px solid #333; }
    .video-container { position:relative; width:100%; aspect-ratio:4/3; background:#000; display:flex; align-items:center; justify-content:center; }
    video { width:100%; height:100%; object-fit:cover; background:#000; }
    .overlay-text { position:absolute; bottom:12px; left:12px; background:rgba(0,0,0,0.7); padding:4px 8px; font-size:11px; border-radius:4px; border:1px solid #333; }
    .status-bar { padding:12px 16px; font-size:12px; color:#888; text-align:center; border-top:1px solid #222; background:#0a0a0a; }
    .controls { padding:12px 16px; display:flex; gap:8px; background:#111; border-top:1px solid #222; }
    button { flex:1; padding:12px; border:none; border-radius:6px; font-weight:700; font-size:12px; cursor:pointer; text-transform:uppercase; transition:all 0.2s; }
    .btn-audio { background:#ff003c; color:#fff; }
    .btn-audio:active { transform:scale(0.98); opacity:0.8; }
  </style>
</head>
<body>
  <div class="card">
    <div class="header">
      <div class="brand">
        <span class="dot"></span>
        <span>SNOOPTRAP REMOTE SURVEILLANCE</span>
      </div>
      <div id="liveBadge" class="badge">CONNECTING...</div>
    </div>

    <div class="video-container">
      <video id="remoteVideo" autoplay playsinline controls muted></video>
      <div class="overlay-text" id="fpsText">CAM: READY</div>
    </div>

    <div class="status-bar" id="statusText">
      Menghubungkan ke kamera & mic laptop Anda via WebRTC P2P...
    </div>

    <div class="controls">
      <button class="btn-audio" onclick="unmuteAudio()">🔊 AKTIFKAN SUARA MIC LAPTOP</button>
    </div>
  </div>

  <script>
    const params = new URLSearchParams(window.location.search);
    const room = params.get('room');
    const statusText = document.getElementById('statusText');
    const liveBadge = document.getElementById('liveBadge');
    const remoteVideo = document.getElementById('remoteVideo');
    const fpsText = document.getElementById('fpsText');

    function unmuteAudio() {
      remoteVideo.muted = false;
      remoteVideo.volume = 1.0;
      remoteVideo.play().then(() => {
        statusText.innerText = "Audio aktif (mendengarkan ruangan laptop).";
      }).catch(e => {
        statusText.innerText = "Audio tap error: " + e;
      });
    }

    if (!room) {
      statusText.innerText = "Error: Room ID tidak ditemukan pada tautan ini.";
      liveBadge.innerText = "ROOM MISSING";
      liveBadge.style.color = "#ff003c";
    } else {
      // ICE servers: Google STUN for NAT discovery. Add your TURN server here if P2P
      // fails on strict/mobile NAT, e.g.
      //   { urls: 'turn:YOUR_VPS_IP:3478', username: 'user', credential: 'pass' }
      // Injected server-side from env (STUN + TURN relay if TURN_URL is set). TURN creds
      // live in the VPS .env, never in source.
      const iceServers = __ICE_SERVERS__;

      let peer = null;
      let connected = false;
      let retryTimer = null;

      function scheduleRetry() {
        if (connected) return;
        clearTimeout(retryTimer);
        retryTimer = setTimeout(() => {
          if (peer && peer.open && !connected) placeCall();
        }, 3000);
      }

      function placeCall() {
        liveBadge.innerText = "CALLING...";
        liveBadge.style.color = "#ffbb00";
        statusText.innerText = "Memanggil kamera laptop (" + room + ")...";

        const call = peer.call(room, new MediaStream());
        if (!call) { scheduleRetry(); return; }

        call.on('stream', (stream) => {
          connected = true;
          clearTimeout(retryTimer);
          remoteVideo.srcObject = stream;
          remoteVideo.play().catch(()=>{});
          liveBadge.innerText = "LIVE";
          liveBadge.style.color = "#00ff41";
          fpsText.innerText = "LIVE | tap 🔊 utk suara";
          statusText.innerText = "Siaran langsung aktif! Tekan 🔊 AKTIFKAN SUARA untuk mendengar.";
        });

        call.on('close', () => {
          connected = false;
          liveBadge.innerText = "RECONNECTING";
          liveBadge.style.color = "#ff003c";
          statusText.innerText = "Feed terputus / laptop disarm. Mencoba ulang...";
          scheduleRetry();
        });

        call.on('error', () => { scheduleRetry(); });
      }

      function startPeer() {
        peer = new Peer({ config: { iceServers: iceServers } });

        peer.on('open', () => {
          statusText.innerText = "Tersambung ke broker. Memanggil kamera laptop...";
          placeCall();
        });

        peer.on('error', (err) => {
          if (err.type === 'peer-unavailable') {
            liveBadge.innerText = "MENUNGGU LAPTOP";
            liveBadge.style.color = "#ffbb00";
            statusText.innerText = "Laptop belum siaran. Kirim /stream di Telegram & jalankan SnoopTrap. Mencoba lagi...";
            scheduleRetry();
          } else if (err.type === 'network' || err.type === 'server-error' || err.type === 'socket-error') {
            statusText.innerText = "Koneksi broker bermasalah, menyambung ulang...";
            setTimeout(() => { try { peer.destroy(); } catch(e){} startPeer(); }, 4000);
          } else {
            statusText.innerText = "PeerJS: " + err.type;
          }
        });

        peer.on('disconnected', () => { try { peer.reconnect(); } catch(e){} });
      }

      startPeer();
    }
  </script>
</body>
</html>`

// iceServersJSON builds the WebRTC ICE server list: Google STUN always, plus the VPS TURN
// relay when TURN_URL/TURN_USER are set in the environment (.env). Credentials come from
// env, never from source. TURN is what makes media flow when direct P2P fails behind NAT.
func iceServersJSON() string {
	servers := []map[string]interface{}{
		{"urls": "stun:stun.l.google.com:19302"},
		{"urls": "stun:stun1.l.google.com:19302"},
	}
	turnURL := os.Getenv("TURN_URL")
	turnUser := os.Getenv("TURN_USER")
	turnPass := os.Getenv("TURN_PASS")
	if turnURL != "" && turnUser != "" {
		servers = append(servers,
			map[string]interface{}{"urls": turnURL + "?transport=udp", "username": turnUser, "credential": turnPass},
			map[string]interface{}{"urls": turnURL + "?transport=tcp", "username": turnUser, "credential": turnPass},
		)
	}
	b, _ := json.Marshal(servers)
	return string(b)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// 1. Web Viewer Endpoint: Serves HTML5 live surveillance player for mobile & desktop.
	// ICE servers (STUN + optional TURN from env) are injected into the page per request.
	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		html := strings.Replace(viewerHTML, "__ICE_SERVERS__", iceServersJSON(), 1)
		fmt.Fprint(w, html)
	})

	// 2. Health check endpoint (for VPS monitoring & uptime checks)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "online",
			"time":   time.Now().Format(time.RFC3339),
			"memory": "ultra-light (< 15MB)",
		})
	})

	// 3. Legacy WebSocket relay endpoint (kept for backward compatibility / LAN fallback).
	// The PeerJS viewer above does NOT use this; media is peer-to-peer via the PeerJS cloud.
	http.HandleFunc("/ws", handleWebSocket)

	log.Printf("[SNOOPTRAP RELAY] Server running on port %s", port)
	log.Printf("[SNOOPTRAP RELAY] Ready for global WebRTC (PeerJS) live surveillance streaming.")
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	roomID := r.URL.Query().Get("room")
	role := r.URL.Query().Get("role") // "host" or "viewer"

	if roomID == "" {
		return
	}

	roomsMu.Lock()
	room, exists := rooms[roomID]
	if !exists {
		room = &Room{
			viewers: make(map[*Client]bool),
		}
		rooms[roomID] = room
	}
	roomsMu.Unlock()

	client := &Client{
		conn:   conn,
		roomID: roomID,
		isHost: role == "host",
		send:   make(chan []byte, 32),
	}

	room.mu.Lock()
	if client.isHost {
		room.host = client
	} else {
		room.viewers[client] = true
		if len(room.lastFrame) > 0 {
			_ = client.conn.WriteMessage(websocket.TextMessage, room.lastFrame)
		}
	}
	room.mu.Unlock()

	defer func() {
		room.mu.Lock()
		if client.isHost {
			room.host = nil
		} else {
			delete(room.viewers, client)
		}
		room.mu.Unlock()
	}()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		}
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}

		// Relay signal between host and viewers
		if client.isHost {
			if bytes.Contains(msg, []byte(`"type":"frame"`)) || bytes.Contains(msg, []byte(`"type": "frame"`)) {
				room.mu.Lock()
				room.lastFrame = msg
				room.mu.Unlock()
			}

			// Broadcast signal from host laptop to all connected phone viewers
			room.mu.RLock()
			for viewer := range room.viewers {
				_ = viewer.conn.WriteMessage(websocket.TextMessage, msg)
			}
			room.mu.RUnlock()
		} else {
			// Forward signal from viewer to host laptop
			room.mu.RLock()
			if room.host != nil {
				_ = room.host.conn.WriteMessage(websocket.TextMessage, msg)
			}
			room.mu.RUnlock()
		}
	}
}
