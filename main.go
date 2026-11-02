package main

import (
	"bytes"
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
	host      *Client
	viewers   map[*Client]bool
	lastFrame []byte
	mu        sync.RWMutex
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
  <title>SnoopTrap // Live Surveillance & Intercom</title>
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
    button { flex:1; padding:12px; border:none; border-radius:6px; font-weight:700; font-size:12px; cursor:pointer; text-transform:uppercase; transition:all 0.2s; user-select:none; -webkit-user-select:none; }
    .btn-audio { background:#ff003c; color:#fff; }
    .btn-audio:active { transform:scale(0.98); opacity:0.8; }
    .btn-talk { background:#0055ff; color:#fff; }
    .btn-talk.talking { background:#00ff41; color:#000; box-shadow:0 0 15px #00ff41; }
  </style>
</head>
<body>
  <div class="card">
    <div class="header">
      <div class="brand">
        <span class="dot"></span>
        <span>SNOOPTRAP LIVE INTERCOM</span>
      </div>
      <div id="liveBadge" class="badge">CONNECTING...</div>
    </div>

    <div class="video-container">
      <img id="remoteVideo" src="" alt="Live Surveillance Feed" style="width:100%;height:100%;object-fit:cover;display:none;" />
      <div id="callingPlaceholder" style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:#888;font-size:12px;gap:8px;">
        <span class="dot"></span>
        <span id="placeholderText">Menghubungkan ke live stream laptop...</span>
      </div>
      <div class="overlay-text" id="fpsText">CAM: READY</div>
    </div>

    <div class="status-bar" id="statusText">
      Menghubungkan ke relay server WebSocket...
    </div>

    <div class="controls">
      <button id="btnListen" class="btn-audio" onclick="toggleListen()">🔊 DENGARKAN LAPTOP</button>
      <button id="btnTalk" class="btn-talk" onmousedown="startTalk()" onmouseup="stopTalk()" ontouchstart="startTalk(event)" ontouchend="stopTalk(event)">🎙️ TAHAN BICARA</button>
    </div>
  </div>

  <script>
    const params = new URLSearchParams(window.location.search);
    const room = params.get('room');
    const statusText = document.getElementById('statusText');
    const liveBadge = document.getElementById('liveBadge');
    const remoteVideo = document.getElementById('remoteVideo');
    const fpsText = document.getElementById('fpsText');
    const callingPlaceholder = document.getElementById('callingPlaceholder');
    const placeholderText = document.getElementById('placeholderText');

    let ws = null;
    let isListening = false;
    let talkStream = null;
    const PCM_RATE = 16000;

    // Playback of laptop PCM (raw samples via Web Audio -> no codec, works on iPhone Safari)
    let playCtx = null;
    let playNextTime = 0;
    // Capture of phone mic as PCM for talk-back
    let talkCtx = null;
    let talkNode = null;

    function b64ToFloat32(b64) {
      const bin = atob(b64);
      const len = bin.length;
      const bytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) bytes[i] = bin.charCodeAt(i);
      const pcm = new Int16Array(bytes.buffer, 0, Math.floor(len / 2));
      const f32 = new Float32Array(pcm.length);
      for (let i = 0; i < pcm.length; i++) f32[i] = pcm[i] / 32768;
      return f32;
    }

    function int16ToB64(pcm) {
      const bytes = new Uint8Array(pcm.buffer, pcm.byteOffset, pcm.byteLength);
      let bin = '';
      const CHUNK = 0x8000;
      for (let i = 0; i < bytes.length; i += CHUNK) {
        bin += String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK));
      }
      return btoa(bin);
    }

    function playPcm(b64, rate) {
      if (!playCtx) return;
      const f32 = b64ToFloat32(b64);
      if (f32.length === 0) return;
      const buf = playCtx.createBuffer(1, f32.length, rate || PCM_RATE);
      buf.getChannelData(0).set(f32);
      const src = playCtx.createBufferSource();
      src.buffer = buf;
      src.connect(playCtx.destination);
      const now = playCtx.currentTime;
      if (playNextTime < now) playNextTime = now + 0.08;
      src.start(playNextTime);
      playNextTime += buf.duration;
    }

    function toggleListen() {
      isListening = !isListening;
      const btn = document.getElementById('btnListen');
      if (isListening) {
        // iPhone Safari needs the AudioContext created/resumed inside a user tap
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        if (!playCtx) playCtx = new AudioCtx();
        if (playCtx.resume) playCtx.resume();
        playNextTime = 0;
        btn.innerText = "🔇 BISUKAN SUARA";
        btn.style.background = "#222";
        statusText.innerText = "Mendengarkan suara mikrofon laptop...";
      } else {
        btn.innerText = "🔊 DENGARKAN LAPTOP";
        btn.style.background = "#ff003c";
        statusText.innerText = "Suara laptop dibisukan.";
      }
    }

    async function startTalk(e) {
      if (e) e.preventDefault();
      const btn = document.getElementById('btnTalk');
      btn.classList.add('talking');
      btn.innerText = "🔴 SEDANG BICARA...";
      statusText.innerText = "Suara Anda sedang diputar di speaker laptop!";

      try {
        if (!talkStream) {
          talkStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        }
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        if (!talkCtx) talkCtx = new AudioCtx();
        if (talkCtx.resume) talkCtx.resume();
        const srcRate = talkCtx.sampleRate;
        const source = talkCtx.createMediaStreamSource(talkStream);
        talkNode = talkCtx.createScriptProcessor(4096, 1, 1);
        talkNode.onaudioprocess = (ev) => {
          if (!ws || ws.readyState !== WebSocket.OPEN) return;
          const input = ev.inputBuffer.getChannelData(0);
          const ratio = srcRate / PCM_RATE;
          const outLen = Math.floor(input.length / ratio);
          const pcm = new Int16Array(outLen);
          for (let i = 0; i < outLen; i++) {
            const s = input[Math.floor(i * ratio)];
            pcm[i] = Math.max(-32768, Math.min(32767, s * 32767));
          }
          ws.send(JSON.stringify({ type: 'intercom_speak', pcm: int16ToB64(pcm), rate: PCM_RATE }));
        };
        source.connect(talkNode);
        const mute = talkCtx.createGain();
        mute.gain.value = 0;
        talkNode.connect(mute);
        mute.connect(talkCtx.destination);
      } catch (err) {
        btn.classList.remove('talking');
        btn.innerText = "🎙️ TAHAN BICARA";
        statusText.innerText = "Izinkan akses mic HP untuk berbicara ke laptop.";
      }
    }

    function stopTalk(e) {
      if (e) e.preventDefault();
      const btn = document.getElementById('btnTalk');
      btn.classList.remove('talking');
      btn.innerText = "🎙️ TAHAN BICARA";
      statusText.innerText = "Pesan suara terkirim.";

      if (talkNode) {
        try { talkNode.disconnect(); } catch (e) {}
        talkNode.onaudioprocess = null;
        talkNode = null;
      }
    }

    if (!room) {
      statusText.innerText = "Error: Room ID tidak ditemukan pada tautan ini.";
      liveBadge.innerText = "ROOM MISSING";
      liveBadge.style.color = "#ff003c";
    } else {
      let frameCount = 0;
      let lastFpsTime = Date.now();

      function connectWS() {
        const loc = window.location;
        const proto = loc.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = proto + '//' + loc.host + '/ws?room=' + encodeURIComponent(room) + '&role=viewer';

        statusText.innerText = "Menghubungkan ke Relay WebSocket...";
        liveBadge.innerText = "CONNECTING...";

        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
          placeholderText.innerText = "Relay terhubung. Menunggu feed kamera laptop...";
          statusText.innerText = "Terhubung ke Relay. Menunggu feed kamera laptop...";
          liveBadge.innerText = "WAITING FEED";
          liveBadge.style.color = "#ffbb00";
        };

        ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            if (msg.type === 'frame' && msg.data) {
              remoteVideo.style.display = 'block';
              callingPlaceholder.style.display = 'none';
              remoteVideo.src = msg.data;

              frameCount++;
              const now = Date.now();
              if (now - lastFpsTime >= 1000) {
                fpsText.innerText = 'FPS: ' + frameCount + ' | LIVE FEED';
                frameCount = 0;
                lastFpsTime = now;
              }

              liveBadge.innerText = "LIVE (ONLINE)";
              liveBadge.style.color = "#00ff41";
              statusText.innerText = "Siaran langsung aktif! Kamera laptop sedang menyiarkan secara real-time.";
            } else if (msg.type === 'audio_out' && isListening && msg.pcm) {
              playPcm(msg.pcm, msg.rate || PCM_RATE);
            }
          } catch (err) {}
        };

        ws.onerror = (err) => {
          statusText.innerText = "Relay WebSocket error.";
        };

        ws.onclose = () => {
          liveBadge.innerText = "RECONNECTING";
          liveBadge.style.color = "#ff003c";
          statusText.innerText = "Koneksi terputus. Mencoba ulang dalam 2 detik...";
          setTimeout(connectWS, 2000);
        };
      }

      connectWS();
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

