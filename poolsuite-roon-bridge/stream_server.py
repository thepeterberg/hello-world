from __future__ import annotations

"""HTTP streaming server that serves a continuous MP3 stream.

Listeners (including Roon) connect to /stream and receive a
never-ending MP3 audio stream, like an internet radio station.
ICY metadata is optionally injected for track titles.
"""

import asyncio
import logging
import socket
import time

from aiohttp import web

logger = logging.getLogger(__name__)

# Chunk size for streaming to clients (8KB)
CHUNK_SIZE = 8192

# ICY metadata interval (bytes between metadata blocks)
ICY_METAINT = 16000


class RadioServer:
    """A simple internet radio server that broadcasts an MP3 stream.

    The server maintains a ring buffer of recent audio data so that
    new listeners get audio immediately. Tracks are fed in by the
    orchestrator via `push_audio()` and `set_now_playing()`.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8489):
        self.host = host
        self.port = port
        self._listeners: list[asyncio.Queue] = []
        self._now_playing: str = "Poolsuite FM"
        self._current_channel: str = "All"
        self._available_channels: list[str] = []
        self._running = False
        self._skip_event: asyncio.Event = asyncio.Event()
        self._prev_event: asyncio.Event = asyncio.Event()
        self._channel_change_event: asyncio.Event = asyncio.Event()
        self._pending_channel: str | None = None
        self._app = web.Application()
        self._app.router.add_get("/stream", self._handle_stream)
        self._app.router.add_get("/stream.mp3", self._handle_stream)
        self._app.router.add_get("/status", self._handle_status)
        self._app.router.add_post("/skip", self._handle_skip)
        self._app.router.add_get("/skip", self._handle_skip)
        self._app.router.add_post("/prev", self._handle_prev)
        self._app.router.add_get("/prev", self._handle_prev)
        self._app.router.add_get("/channel", self._handle_channel)
        self._app.router.add_get("/", self._handle_index)
        self._started_at = time.time()
        self._tracks_played = 0
        self._history: list[tuple[str, float]] = []  # (title, timestamp)

    @staticmethod
    def _detect_local_ip() -> str:
        """Detect the machine's local network IP address."""
        try:
            # Connect to a public DNS to determine which interface is used
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    @property
    def local_ip(self) -> str:
        if not hasattr(self, "_local_ip"):
            self._local_ip = self._detect_local_ip()
        return self._local_ip

    @property
    def stream_url(self) -> str:
        return f"http://{self.local_ip}:{self.port}/stream"

    def set_now_playing(self, title: str, soundcloud_url: str | None = None) -> None:
        self._now_playing = title
        self._now_playing_url = soundcloud_url
        self._tracks_played += 1
        self._history.insert(0, (title, time.time(), soundcloud_url))
        # Keep last 100 tracks
        self._history = self._history[:100]
        logger.info("Now playing: %s", title)

    async def push_audio(self, data: bytes) -> None:
        """Push audio data to all connected listeners."""
        dead = []
        for i, queue in enumerate(self._listeners):
            try:
                queue.put_nowait(data)
            except asyncio.QueueFull:
                dead.append(i)
        # Clean up disconnected/slow listeners
        for i in reversed(dead):
            self._listeners.pop(i)

    async def push_eof(self) -> None:
        """Signal end-of-track to all listeners (they just keep listening)."""
        # No-op for continuous stream; tracks blend together
        pass

    @staticmethod
    def _build_icy_metadata(title: str) -> bytes:
        """Build an ICY metadata block for the given title.

        ICY format: 1 byte length prefix (actual length / 16, rounded up),
        followed by the metadata string padded with null bytes to a multiple of 16.
        """
        text = f"StreamTitle='{title}';".encode("utf-8")
        # Length byte = ceil(len(text) / 16)
        length = (len(text) + 15) // 16
        # Pad to length * 16 bytes
        padded = text.ljust(length * 16, b"\x00")
        return bytes([length]) + padded

    async def _handle_stream(self, request: web.Request) -> web.StreamResponse:
        """Handle a listener connecting to the MP3 stream."""
        # Check if client supports ICY metadata
        icy_requested = request.headers.get("Icy-MetaData", "") == "1"

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "audio/mpeg",
                "Cache-Control": "no-cache, no-store",
                "Connection": "keep-alive",
                "icy-name": "Poolsuite FM via Roon Bridge",
                "icy-genre": "Synthwave / Funk / Disco / Poolside Vibes",
                "icy-br": "192",
                "icy-sr": "44100",
                "icy-pub": "0",
            },
        )
        if icy_requested:
            response.headers["icy-metaint"] = str(ICY_METAINT)

        await response.prepare(request)

        queue: asyncio.Queue = asyncio.Queue(maxsize=256)
        self._listeners.append(queue)
        peer = request.remote
        logger.info("Listener connected: %s (icy=%s, total: %d)", peer, icy_requested, len(self._listeners))

        try:
            if not icy_requested:
                # Simple path: no metadata injection needed
                while True:
                    chunk = await queue.get()
                    await response.write(chunk)
            else:
                # ICY path: inject metadata every ICY_METAINT bytes
                bytes_since_meta = 0
                while True:
                    chunk = await queue.get()
                    pos = 0
                    while pos < len(chunk):
                        # How many bytes until next metadata insertion?
                        remaining = ICY_METAINT - bytes_since_meta
                        to_send = min(remaining, len(chunk) - pos)
                        await response.write(chunk[pos:pos + to_send])
                        bytes_since_meta += to_send
                        pos += to_send

                        if bytes_since_meta >= ICY_METAINT:
                            # Insert ICY metadata block
                            meta = self._build_icy_metadata(self._now_playing)
                            await response.write(meta)
                            bytes_since_meta = 0
        except (ConnectionResetError, ConnectionAbortedError, asyncio.CancelledError):
            pass
        finally:
            if queue in self._listeners:
                self._listeners.remove(queue)
            logger.info("Listener disconnected: %s (total: %d)", peer, len(self._listeners))

        return response

    @property
    def skip_event(self) -> asyncio.Event:
        """Event that is set when a skip is requested. The playback loop
        should check/await this and clear it after advancing."""
        return self._skip_event

    @property
    def prev_event(self) -> asyncio.Event:
        """Event that is set when a previous track is requested."""
        return self._prev_event

    @property
    def channel_change_event(self) -> asyncio.Event:
        """Event set when a channel change is requested."""
        return self._channel_change_event

    @property
    def pending_channel(self) -> str | None:
        """The channel name requested via the web UI, or None."""
        return self._pending_channel

    def set_available_channels(self, channels: list[str]) -> None:
        self._available_channels = channels

    def set_current_channel(self, name: str) -> None:
        self._current_channel = name

    async def _handle_skip(self, request: web.Request) -> web.Response:
        """Handle a skip request — advance to the next track."""
        logger.info("Skip requested")
        self._skip_event.set()
        if "text/html" in request.headers.get("Accept", ""):
            raise web.HTTPFound("/")
        return web.json_response({"status": "skipping", "was_playing": self._now_playing})

    async def _handle_prev(self, request: web.Request) -> web.Response:
        """Handle a previous track request — go back to the prior track."""
        logger.info("Previous track requested")
        self._prev_event.set()
        self._skip_event.set()  # Stop the current track
        if "text/html" in request.headers.get("Accept", ""):
            raise web.HTTPFound("/")
        return web.json_response({"status": "going_back", "was_playing": self._now_playing})

    async def _handle_channel(self, request: web.Request) -> web.Response:
        """Handle a channel change request."""
        name = request.query.get("name", "").strip()
        if not name:
            return web.json_response(
                {"channels": self._available_channels, "current": self._current_channel}
            )
        logger.info("Channel change requested: %s", name)
        self._pending_channel = name if name != "All" else None
        self._channel_change_event.set()
        self._skip_event.set()  # Also skip current track to switch faster
        if "text/html" in request.headers.get("Accept", ""):
            raise web.HTTPFound("/")
        return web.json_response({"status": "switching", "channel": name})

    async def _handle_status(self, request: web.Request) -> web.Response:
        """Return JSON status of the radio server."""
        return web.json_response({
            "status": "streaming" if self._running else "stopped",
            "now_playing": self._now_playing,
            "listeners": len(self._listeners),
            "tracks_played": self._tracks_played,
            "uptime_seconds": int(time.time() - self._started_at),
            "stream_url": self.stream_url,
        })

    def _render_history(self) -> str:
        """Render the play history as HTML rows."""
        if not self._history:
            return '<div class="history-empty">No tracks played yet</div>'
        now = time.time()
        rows = []
        for i, entry in enumerate(self._history):
            title, ts = entry[0], entry[1]
            sc_url = entry[2] if len(entry) > 2 else None
            ago = int(now - ts)
            if ago < 60:
                time_str = "just now" if ago < 10 else f"{ago}s ago"
            elif ago < 3600:
                time_str = f"{ago // 60}m ago"
            else:
                time_str = f"{ago // 3600}h {(ago % 3600) // 60}m ago"
            label = "NOW" if i == 0 else str(i)
            # Split "Artist - Title" if possible
            if " - " in title:
                artist, track = title.split(" - ", 1)
                text = f"<strong>{artist}</strong> &mdash; {track}"
            else:
                text = f"<strong>{title}</strong>"
            if sc_url:
                display = (
                    f'<a href="{sc_url}" target="_blank" '
                    f'style="color: #e0d68a; text-decoration: none; '
                    f'border-bottom: 1px solid rgba(224,214,138,0.2);">{text}</a>'
                )
            else:
                display = text
            rows.append(
                f'<div class="history-row">'
                f'<span class="track-num">{label}</span>'
                f'<span class="track-title">{display}</span>'
                f'<span class="track-time">{time_str}</span>'
                f'</div>'
            )
        return "\n".join(rows)

    async def _handle_index(self, request: web.Request) -> web.Response:
        """Simple landing page."""
        ip = self.local_ip
        base = f"http://{ip}:{self.port}"

        channel_buttons = ""
        all_channels = ["All"] + self._available_channels
        for ch in all_channels:
            is_current = (ch == self._current_channel)
            channel_buttons += (
                f'<a href="/channel?name={ch}" class="ch-btn'
                f'{" ch-active" if is_current else ""}">{ch}</a>\n'
            )

        now_playing_link = (
            f'<a href="{self._now_playing_url}" target="_blank" class="np-title">'
            f'{self._now_playing}</a>'
            if getattr(self, "_now_playing_url", None)
            else f'<span class="np-title">{self._now_playing}</span>'
        )

        html = f"""<!DOCTYPE html>
<html>
<head>
  <title>POOLSUITE FM &mdash; Roon Bridge</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet">
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: 'DM Sans', -apple-system, sans-serif;
      background: linear-gradient(170deg, #0a1628 0%, #162447 40%, #1f4068 70%, #1b3a5c 100%);
      color: #f0e6d3;
      min-height: 100vh;
    }}
    .container {{ max-width: 520px; margin: 0 auto; padding: 2em 1.5em; }}

    /* Header */
    .header {{ text-align: center; margin-bottom: 2em; }}
    .logo {{ font-family: 'Space Mono', monospace; font-size: 0.7em; letter-spacing: 0.35em; text-transform: uppercase; color: rgba(240,230,211,0.5); margin-bottom: 0.5em; }}
    .title {{ font-family: 'Space Mono', monospace; font-size: 2em; font-weight: 700; background: linear-gradient(135deg, #ffd700, #ff8c42, #ff6b6b); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; line-height: 1.2; }}
    .tagline {{ font-size: 0.8em; color: rgba(240,230,211,0.4); margin-top: 0.4em; font-family: 'Space Mono', monospace; }}

    /* Now Playing Card */
    .np-card {{
      background: linear-gradient(135deg, rgba(255,215,0,0.12), rgba(255,140,66,0.08));
      border: 1px solid rgba(255,215,0,0.2);
      border-radius: 12px;
      padding: 1.5em;
      margin-bottom: 1.5em;
      position: relative;
      overflow: hidden;
    }}
    .np-card::before {{
      content: '';
      position: absolute;
      top: -50%; left: -50%;
      width: 200%; height: 200%;
      background: radial-gradient(ellipse at 30% 50%, rgba(255,215,0,0.06) 0%, transparent 60%);
      pointer-events: none;
    }}
    .np-label {{ font-family: 'Space Mono', monospace; font-size: 0.65em; letter-spacing: 0.2em; text-transform: uppercase; color: #ffd700; margin-bottom: 0.6em; }}
    .np-title {{ font-size: 1.3em; font-weight: 700; color: #f0e6d3; text-decoration: none; display: block; line-height: 1.3; }}
    a.np-title:hover {{ color: #ffd700; }}
    .np-meta {{ font-size: 0.75em; color: rgba(240,230,211,0.45); margin-top: 0.6em; font-family: 'Space Mono', monospace; }}

    /* Transport */
    .transport {{ display: flex; justify-content: center; gap: 0.8em; margin-bottom: 1.5em; }}
    .t-btn {{
      display: flex; align-items: center; justify-content: center;
      width: 48px; height: 48px;
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 50%;
      color: #f0e6d3;
      text-decoration: none;
      font-size: 1.2em;
      transition: all 0.15s;
    }}
    .t-btn:hover {{ background: rgba(255,215,0,0.2); border-color: rgba(255,215,0,0.4); color: #ffd700; }}

    /* Section */
    .section {{ margin-bottom: 1.5em; }}
    .section-label {{
      font-family: 'Space Mono', monospace;
      font-size: 0.6em;
      letter-spacing: 0.2em;
      text-transform: uppercase;
      color: rgba(240,230,211,0.35);
      margin-bottom: 0.8em;
    }}

    /* Channels */
    .channels {{ display: flex; flex-wrap: wrap; gap: 0.4em; }}
    .ch-btn {{
      font-family: 'Space Mono', monospace;
      font-size: 0.7em;
      padding: 0.5em 1em;
      background: rgba(255,255,255,0.05);
      border: 1px solid rgba(255,255,255,0.1);
      border-radius: 20px;
      color: rgba(240,230,211,0.7);
      text-decoration: none;
      transition: all 0.15s;
    }}
    .ch-btn:hover {{ background: rgba(255,215,0,0.15); border-color: rgba(255,215,0,0.3); color: #ffd700; }}
    .ch-active {{ background: rgba(255,215,0,0.2); border-color: #ffd700; color: #ffd700; }}

    /* Audio player */
    audio {{ width: 100%; margin-top: 0.5em; border-radius: 8px; }}

    /* Roon URL */
    .roon-box {{
      display: flex; align-items: center;
      background: rgba(0,0,0,0.25);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 8px;
      padding: 0.7em 1em;
      gap: 0.8em;
      margin-top: 0.5em;
    }}
    .roon-box code {{ flex: 1; font-family: 'Space Mono', monospace; font-size: 0.8em; color: #ffd700; word-break: break-all; }}
    .roon-box .label {{ font-size: 0.7em; color: rgba(240,230,211,0.35); white-space: nowrap; }}
    .copy-btn {{
      font-family: 'Space Mono', monospace;
      background: rgba(255,215,0,0.15);
      border: 1px solid rgba(255,215,0,0.3);
      color: #ffd700;
      padding: 0.35em 0.8em;
      border-radius: 4px;
      cursor: pointer;
      font-size: 0.7em;
      letter-spacing: 0.05em;
      transition: all 0.15s;
    }}
    .copy-btn:hover {{ background: rgba(255,215,0,0.3); }}
    .copy-btn.copied {{ background: rgba(255,215,0,0.4); }}

    /* Divider */
    .divider {{ border: none; border-top: 1px solid rgba(255,255,255,0.06); margin: 1.5em 0; }}

    /* History */
    .history {{
      background: rgba(0,0,0,0.2);
      border: 1px solid rgba(255,255,255,0.06);
      border-radius: 10px;
      max-height: 420px;
      overflow-y: auto;
    }}
    .history::-webkit-scrollbar {{ width: 4px; }}
    .history::-webkit-scrollbar-thumb {{ background: rgba(255,215,0,0.2); border-radius: 2px; }}
    .history-row {{
      display: flex; align-items: center;
      padding: 0.7em 1em;
      border-bottom: 1px solid rgba(255,255,255,0.04);
      gap: 0.8em;
      transition: background 0.1s;
    }}
    .history-row:hover {{ background: rgba(255,255,255,0.03); }}
    .history-row:last-child {{ border-bottom: none; }}
    .history-row:first-child {{ background: rgba(255,215,0,0.06); }}
    .history-row .track-num {{
      font-family: 'Space Mono', monospace;
      font-size: 0.65em;
      color: rgba(240,230,211,0.25);
      min-width: 2.5em;
      text-align: right;
    }}
    .history-row:first-child .track-num {{ color: #ffd700; font-weight: 700; }}
    .history-row .track-title {{ flex: 1; font-size: 0.85em; line-height: 1.3; }}
    .history-row .track-title a {{ color: #f0e6d3; text-decoration: none; }}
    .history-row .track-title a:hover {{ color: #ffd700; }}
    .history-row .track-title strong {{ color: rgba(255,215,0,0.85); font-weight: 600; }}
    .history-row .track-time {{
      font-family: 'Space Mono', monospace;
      color: rgba(240,230,211,0.25);
      font-size: 0.65em;
      white-space: nowrap;
    }}
    .history-empty {{ padding: 2em; text-align: center; color: rgba(240,230,211,0.25); font-size: 0.85em; }}

    /* Footer */
    .footer {{ text-align: center; margin-top: 2em; font-size: 0.7em; color: rgba(240,230,211,0.2); font-family: 'Space Mono', monospace; }}
    .footer a {{ color: rgba(255,215,0,0.4); text-decoration: none; }}
    .footer a:hover {{ color: #ffd700; }}
  </style>
  <script>
    function copyUrl(btn, url) {{
      navigator.clipboard.writeText(url).then(() => {{
        btn.textContent = 'COPIED';
        btn.classList.add('copied');
        setTimeout(() => {{ btn.textContent = 'COPY'; btn.classList.remove('copied'); }}, 2000);
      }});
    }}
  </script>
</head>
<body>
<div class="container">

  <div class="header">
    <div class="logo">Poolsuite FM</div>
    <div class="title">Roon Bridge</div>
    <div class="tagline">{ip} &middot; port {self.port}</div>
  </div>

  <div class="np-card">
    <div class="np-label">Now Playing</div>
    {now_playing_link}
    <div class="np-meta">{self._current_channel} &middot; {len(self._listeners)} listener{"s" if len(self._listeners) != 1 else ""} &middot; {self._tracks_played} played</div>
  </div>

  <div class="transport">
    <a href="/prev" class="t-btn" title="Previous">&laquo;</a>
    <a href="/skip" class="t-btn" title="Next">&raquo;</a>
  </div>

  <div class="section">
    <div class="section-label">Channels</div>
    <div class="channels">{channel_buttons}</div>
  </div>

  <div class="section">
    <div class="section-label">Listen in Browser</div>
    <audio controls src="/stream"></audio>
  </div>

  <hr class="divider">

  <div class="section">
    <div class="section-label">Add to Roon</div>
    <div class="roon-box">
      <code>{base}/stream</code>
      <button class="copy-btn" onclick="copyUrl(this, '{base}/stream')">COPY</button>
    </div>
  </div>

  <div class="section">
    <div class="section-label">Endpoints</div>
    <div class="roon-box">
      <code>{base}/stream</code>
      <span class="label">MP3</span>
      <button class="copy-btn" onclick="copyUrl(this, '{base}/stream')">COPY</button>
    </div>
    <div class="roon-box" style="margin-top:0.4em;">
      <code>{base}/status</code>
      <span class="label">JSON</span>
      <button class="copy-btn" onclick="copyUrl(this, '{base}/status')">COPY</button>
    </div>
    <div class="roon-box" style="margin-top:0.4em;">
      <code>{base}/skip</code>
      <span class="label">Skip</span>
      <button class="copy-btn" onclick="copyUrl(this, '{base}/skip')">COPY</button>
    </div>
    <div class="roon-box" style="margin-top:0.4em;">
      <code>{base}/prev</code>
      <span class="label">Prev</span>
      <button class="copy-btn" onclick="copyUrl(this, '{base}/prev')">COPY</button>
    </div>
  </div>

  <hr class="divider">

  <div class="section">
    <div class="section-label">Play History</div>
    <div class="history">{self._render_history()}</div>
  </div>

  <div class="footer">
    <a href="https://poolsuite.net" target="_blank">poolsuite.net</a> &middot;
    <a href="https://github.com/thepeterberg/poolsuite-roon-streaming" target="_blank">github</a>
  </div>

</div>
</body>
</html>"""
        return web.Response(text=html, content_type="text/html")

    async def start(self) -> web.AppRunner:
        """Start the HTTP server."""
        self._running = True
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        logger.info("Radio server listening on http://%s:%d/stream", self.host, self.port)
        return runner

    async def stop(self, runner: web.AppRunner) -> None:
        """Stop the HTTP server."""
        self._running = False
        await runner.cleanup()
