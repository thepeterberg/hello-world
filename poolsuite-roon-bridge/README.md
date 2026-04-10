# Poolsuite → Roon Bridge

Stream curated [Poolsuite FM](https://poolsuite.net/) tracks (sourced from SoundCloud) to your [Roon](https://roonlabs.com/) home audio system as a local internet radio station.

## How It Works

```
Poolsuite API  →  yt-dlp (resolve SoundCloud)  →  ffmpeg (transcode)  →  HTTP MP3 stream  →  Roon
```

1. **Fetches playlists** from Poolsuite's API (`api.poolsidefm.workers.dev`), which returns curated track lists with SoundCloud IDs
2. **Resolves audio streams** using `yt-dlp` to get direct audio URLs from SoundCloud
3. **Transcodes to MP3** via `ffmpeg` at a constant bitrate (default 192kbps)
4. **Serves an HTTP stream** at `http://YOUR_IP:8489/stream` that acts as a local internet radio station
5. **Roon connects** to this URL as a custom Live Radio station and plays it on any zone

## Prerequisites

- **Python 3.11+**
- **ffmpeg** (with libmp3lame)
- **yt-dlp**

### Install on macOS

```bash
brew install ffmpeg python
pip install yt-dlp
```

### Install on Ubuntu/Debian

```bash
sudo apt update && sudo apt install -y ffmpeg python3 python3-pip
pip3 install yt-dlp
```

### Install on Arch Linux

```bash
sudo pacman -S ffmpeg python python-pip yt-dlp
```

## Setup

```bash
# Clone and enter the project
cd poolsuite-roon-bridge

# Install Python dependencies
pip install -r requirements.txt

# (Optional) Copy and edit config
cp config.example.json config.json
```

## Usage

```bash
# Start with defaults (shuffled, port 8489)
python main.py

# Use a config file
python main.py --config config.json

# Custom port
python main.py --port 9000

# Filter to a specific Poolsuite playlist
python main.py --playlist "Poolsuite FM"

# Play in order (no shuffle)
python main.py --no-shuffle

# Verbose logging
python main.py -v
```

On startup you'll see:

```
============================================================
  Poolsuite -> Roon Bridge
============================================================

  Stream URL:  http://YOUR_LOCAL_IP:8489/stream
  Status:      http://YOUR_LOCAL_IP:8489/status
  Web UI:      http://YOUR_LOCAL_IP:8489/

  Add the stream URL as a Live Radio station in Roon:
    Roon > My Live Radio > + > paste the stream URL

============================================================
```

## Adding to Roon

1. Find your machine's local IP (e.g. `192.168.1.100`)
2. Open **Roon** on any client
3. Go to **My Live Radio** (in the sidebar under "Library")
4. Click **+ Add Station**
5. Paste the stream URL: `http://192.168.1.100:8489/stream`
6. Name it **"Poolsuite FM"**
7. Play it on any Roon zone

Roon treats this exactly like any internet radio station — you get full zone grouping, volume control, and DSP.

## Running as a Service

To keep it running in the background on a Linux server (e.g. your Roon Core machine):

### systemd

```bash
sudo tee /etc/systemd/system/poolsuite-roon.service << 'EOF'
[Unit]
Description=Poolsuite Roon Bridge
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/poolsuite-roon-bridge
ExecStart=/usr/bin/python3 main.py --config config.json
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now poolsuite-roon.service
```

### Docker

```bash
docker run -d \
  --name poolsuite-roon \
  --restart unless-stopped \
  -p 8489:8489 \
  -v $(pwd)/config.json:/app/config.json \
  python:3.12-slim \
  bash -c "apt-get update && apt-get install -y ffmpeg && pip install -r /app/requirements.txt yt-dlp && cd /app && python main.py"
```

(Mount the project directory appropriately.)

## Configuration

`config.json` options:

| Key                  | Default                                    | Description                                       |
| -------------------- | ------------------------------------------ | ------------------------------------------------- |
| `host`               | `"0.0.0.0"`                                | Bind address                                      |
| `port`               | `8489`                                     | HTTP server port                                  |
| `bitrate`            | `"192k"`                                   | MP3 output bitrate                                |
| `crossfade_seconds`  | `3`                                        | Silence gap between tracks                        |
| `shuffle`            | `true`                                     | Randomize track order                             |
| `playlist_filter`    | `null`                                     | Only play tracks from playlists matching this name |
| `poolsuite_api`      | `"https://api.poolsidefm.workers.dev"`     | Poolsuite API base URL                            |

## Endpoints

| Path          | Description                                  |
| ------------- | -------------------------------------------- |
| `/`           | Web UI with now-playing info and audio player |
| `/stream`     | MP3 audio stream (add this to Roon)          |
| `/stream.mp3` | Alias for `/stream`                          |
| `/status`     | JSON status (now playing, listeners, uptime) |

## Alternative Approaches

If this bridge approach doesn't suit your setup, here are other ways to get Poolsuite audio into Roon:

### AirPlay Routing
- Play Poolsuite in a browser on your Mac
- Use **Rogue Amoeba SoundSource** or **BlackHole** (virtual audio device) to route system audio
- Roon can receive AirPlay — but this is fragile and ties up a computer

### SoundCloud Playlists Directly
- Poolsuite maintains official playlists on SoundCloud:
  - [Poolsuite FM Official Playlist](https://soundcloud.com/poolsuite/sets/poolsuite-fm-official-playlist)
  - [Official Playlist Pt. II](https://soundcloud.com/poolsuite/sets/poolsuite-fm-official-playlist-two)
  - [Poolsuite Mixtapes](https://soundcloud.com/poolsuite/sets/poolsuite-mixtapes)
- Use `yt-dlp` to download these playlists as FLAC/MP3 files, then add to your Roon library

### Roon Extension (Advanced)
- Build a Node.js [Roon Extension](https://github.com/RoonLabs/node-roon-api) that provides a browseable Poolsuite source
- More complex but gives tighter Roon integration

## Troubleshooting

**"yt-dlp not found"** — Install with `pip install yt-dlp`. Keep it updated: `pip install -U yt-dlp`

**"ffmpeg not found"** — Install via your package manager (brew, apt, etc.)

**Tracks skipping or failing** — SoundCloud URLs can expire. The bridge re-resolves each track before playing. Run with `-v` for detailed logs.

**Roon won't connect** — Ensure the bridge host is reachable from your Roon Core's network. Test by opening `http://HOST:8489/` in a browser.

**No tracks found** — The Poolsuite API may be down or changed. Check `http://HOST:8489/status` and logs.

## License

MIT — for personal/educational use. Respect SoundCloud's and Poolsuite's terms of service.
