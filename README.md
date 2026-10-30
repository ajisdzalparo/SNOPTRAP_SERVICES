# SnoopTrap Stream Relay 📡🚀

> Standalone WebRTC Relay & Surveillance Stream Player for [SnoopTrap](https://github.com/ajisdzalparo/SNOOPTRAP).
> Designed for instant deployment on any Linux VPS (Ubuntu, Debian, CentOS, Alpine).
> **Resource footprint:** < 15MB RAM, ~0% idle CPU, image size < 20MB.

---

## ⚡ Deployment di VPS (Cepat & Mudah)

### Opsi 1: Docker Compose (Direkomendasikan)

```bash
# 1. Clone repositori ini di VPS
git clone https://github.com/ajisdzalparo/snooptrap-relay.git
cd snooptrap-relay

# 2. Jalankan container
docker compose up -d --build
```

### Opsi 2: Docker Biasa

```bash
docker build -t snooptrap-stream .
docker run -d --name snooptrap-stream -p 8080:8080 --restart always snooptrap-stream
```

### Opsi 3: Binary Native (Tanpa Docker)

```bash
# Butuh Go 1.21+ terinstal di VPS
go build -o relay-service .
PORT=8080 ./relay-service
```

---

## ⚙️ Cara Menghubungkan ke SnoopTrap Laptop

1. Buka aplikasi **SnoopTrap** di laptop Anda.
2. Masuk ke tab **Settings (Config)**.
3. Pada bagian **VPS / RELAY SERVER FOR GLOBAL STREAMING**:
   - **SERVER IP / HOST**: Masukkan IP Publik VPS Anda (contoh: `103.123.45.67` atau `stream.domainanda.com`).
   - **PORT**: `8080` (atau port yang Anda petakan).
4. Klik **Save Configuration**.
5. Selesai! Link `/stream` Telegram sekarang dapat diakses dari browser HP di mana pun melalui koneksi internet global.
