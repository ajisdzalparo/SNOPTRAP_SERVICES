package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
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
	host    *Client
	viewers map[*Client]bool
	mu      sync.RWMutex
}

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.RWMutex
)

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
      <video id="remoteVideo" autoplay playsinline controls></video>
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
      remoteVideo.play().then(() => {
        statusText.innerText = "Audio diaktifkan (Mendengarkan ruangan laptop)";
      }).catch(e => {
        statusText.innerText = "Audio tap error: " + e;
      });
    }

    if (!room) {
      statusText.innerText = "Error: Room ID tidak ditemukan pada tautan ini.";
      liveBadge.innerText = "ROOM MISSING";
      liveBadge.style.color = "#ff003c";
    } else {
      const peer = new Peer({
        config: {
          iceServers: [
            { urls: 'stun:stun.l.google.com:19302' },
            { urls: 'stun:stun1.l.google.com:19302' },
            { urls: 'stun:stun2.l.google.com:19302' },
            { urls: 'stun:stun3.l.google.com:19302' },
            { urls: 'stun:stun4.l.google.com:19302' }
          ]
        }
      });

      peer.on('open', (id) => {
        statusText.innerText = "Tersambung ke Relay. Memanggil kamera laptop (" + room + ")...";
        liveBadge.innerText = "CALLING...";

        // Call the laptop's PeerJS room
        const call = peer.call(room, new MediaStream());

        call.on('stream', (stream) => {
          remoteVideo.srcObject = stream;
          remoteVideo.play().catch(()=>{});
          liveBadge.innerText = "LIVE (30 FPS)";
          liveBadge.style.color = "#00ff41";
          fpsText.innerText = "FPS: 30 | AUDIO: LIVE";
          statusText.innerText = "Siaran langsung aktif! Kamera dan mikrofon laptop sedang menyiarkan siaran real-time.";
        });

        call.on('close', () => {
          liveBadge.innerText = "DISCONNECTED";
          liveBadge.style.color = "#ff003c";
          statusText.innerText = "Panggilan live feed selesai / laptop disarm.";
        });

        call.on('error', (err) => {
          statusText.innerText = "Koneksi terputus: " + err;
        });
      });

      peer.on('error', (err) => {
        if (err.type === 'peer-unavailable') {
          statusText.innerText = "Laptop sedang offline atau feed belum dimulai. Kirim /stream di Telegram untuk memicu feed.";
        } else {
          statusText.innerText = "PeerJS Notice: " + err.type + " - " + err.message;
        }
      });
    }
  </script>
</body>
</html>`

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// 1. Web Viewer Endpoint: Serves HTML5 live surveillance player for mobile & desktop
	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, viewerHTML)
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

	// 3. Signaling Relay Endpoint (WebSocket)
	http.HandleFunc("/ws", handleWebSocket)

	log.Printf("[SNOOPTRAP RELAY] Server running on port %s", port)
	log.Printf("[SNOOPTRAP RELAY] Ready for global WebRTC live surveillance streaming.")
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

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}

		// Relay signal between host and viewers
		room.mu.RLock()
		if client.isHost {
			// Broadcast signal from host laptop to all connected phone viewers
			for viewer := range room.viewers {
				_ = viewer.conn.WriteMessage(websocket.TextMessage, msg)
			}
		} else if room.host != nil {
			// Forward signal from viewer to host laptop
			_ = room.host.conn.WriteMessage(websocket.TextMessage, msg)
		}
		room.mu.RUnlock()
	}
}

