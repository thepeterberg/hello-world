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

    def set_now_playing(self, title: str) -> None:
        self._now_playing = title
        self._tracks_played += 1
        self._history.insert(0, (title, time.time()))
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
        for i, (title, ts) in enumerate(self._history):
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
                display = f"<strong>{artist}</strong> &mdash; {track}"
            else:
                display = f"<strong>{title}</strong>"
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
            bg = "#64dfdf" if is_current else "rgba(224,214,138,0.15)"
            color = "#1a1a2e" if is_current else "#e0d68a"
            border = "2px solid #64dfdf" if is_current else "2px solid rgba(224,214,138,0.3)"
            channel_buttons += (
                f'<a href="/channel?name={ch}" style="display: inline-block; '
                f'padding: 0.5em 1.2em; margin: 0.3em; background: {bg}; color: {color}; '
                f'text-decoration: none; font-weight: bold; border: {border}; '
                f'border-radius: 4px;">{ch}</a>\n'
            )

        html = f"""<!DOCTYPE html>
<html>
<head>
  <title>Poolsuite Roon Bridge</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ font-family: 'SF Mono', 'Menlo', 'Monaco', monospace; background: #1a1a2e; color: #e0d68a; padding: 2em; max-width: 700px; margin: 0 auto; }}
    h1 {{ margin-bottom: 0.3em; }}
    .subtitle {{ color: #64dfdf; margin-top: 0; font-size: 0.85em; }}
    hr {{ border: none; border-top: 1px solid rgba(224,214,138,0.2); margin: 1.5em 0; }}
    .now-playing {{ font-size: 1.3em; margin: 0.5em 0; }}
    .meta {{ color: rgba(224,214,138,0.6); font-size: 0.85em; }}
    .btn {{ display: inline-block; padding: 0.7em 1.8em; background: #e0d68a; color: #1a1a2e; text-decoration: none; font-weight: bold; font-size: 1em; border-radius: 4px; margin: 0.3em; }}
    .btn:hover {{ background: #64dfdf; }}
    .roon-url {{ display: flex; align-items: center; background: rgba(255,255,255,0.05); border: 1px solid rgba(224,214,138,0.2); border-radius: 6px; padding: 0.6em 1em; margin: 0.5em 0; gap: 0.8em; }}
    .roon-url code {{ flex: 1; color: #64dfdf; word-break: break-all; font-size: 0.95em; }}
    .copy-btn {{ background: none; border: 1px solid rgba(224,214,138,0.4); color: #e0d68a; padding: 0.4em 0.8em; border-radius: 4px; cursor: pointer; font-family: inherit; font-size: 0.85em; white-space: nowrap; }}
    .copy-btn:hover {{ background: rgba(224,214,138,0.15); }}
    .copy-btn.copied {{ border-color: #64dfdf; color: #64dfdf; }}
    audio {{ width: 100%; margin-top: 1em; }}
    .section-label {{ color: rgba(224,214,138,0.5); text-transform: uppercase; font-size: 0.75em; letter-spacing: 0.1em; margin-bottom: 0.5em; }}
    .history {{ background: rgba(255,255,255,0.03); border: 1px solid rgba(224,214,138,0.15); border-radius: 6px; max-height: 400px; overflow-y: auto; }}
    .history-row {{ display: flex; align-items: center; padding: 0.6em 1em; border-bottom: 1px solid rgba(224,214,138,0.08); gap: 1em; }}
    .history-row:last-child {{ border-bottom: none; }}
    .history-row:first-child {{ background: rgba(100,223,223,0.08); }}
    .history-row .track-title {{ flex: 1; font-size: 0.9em; }}
    .history-row .track-title strong {{ color: #64dfdf; }}
    .history-row .track-time {{ color: rgba(224,214,138,0.4); font-size: 0.8em; white-space: nowrap; }}
    .history-row .track-num {{ color: rgba(224,214,138,0.3); font-size: 0.75em; min-width: 1.5em; text-align: right; }}
    .history-empty {{ padding: 2em; text-align: center; color: rgba(224,214,138,0.3); }}
  </style>
  <script>
    function copyUrl(btn, url) {{
      navigator.clipboard.writeText(url).then(() => {{
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => {{ btn.textContent = 'Copy'; btn.classList.remove('copied'); }}, 2000);
      }});
    }}
  </script>
</head>
<body>
  <h1>Poolsuite &rarr; Roon</h1>
  <p class="subtitle">Local bridge &middot; {ip}</p>

  <div class="now-playing">Now Playing: <strong>{self._now_playing}</strong></div>
  <p class="meta">Channel: {self._current_channel} &middot; {len(self._listeners)} listener{"s" if len(self._listeners) != 1 else ""} &middot; {self._tracks_played} tracks played</p>

  <div style="margin: 1.2em 0;">
    <a href="/prev" class="btn">&laquo; Previous</a>
    <a href="/skip" class="btn">Next &raquo;</a>
  </div>

  <hr>
  <p class="section-label">Channels</p>
  <div style="margin: 0.3em 0 1em 0;">{channel_buttons}</div>

  <hr>
  <p class="section-label">Add to Roon &mdash; Live Radio</p>

  <div class="roon-url">
    <code>{base}/stream</code>
    <button class="copy-btn" onclick="copyUrl(this, '{base}/stream')">Copy</button>
  </div>

  <hr>
  <p class="section-label">Endpoints</p>

  <div class="roon-url">
    <code>{base}/stream</code>
    <span class="meta" style="white-space:nowrap;">MP3 stream</span>
    <button class="copy-btn" onclick="copyUrl(this, '{base}/stream')">Copy</button>
  </div>
  <div class="roon-url">
    <code>{base}/status</code>
    <span class="meta" style="white-space:nowrap;">JSON status</span>
    <button class="copy-btn" onclick="copyUrl(this, '{base}/status')">Copy</button>
  </div>
  <div class="roon-url">
    <code>{base}/skip</code>
    <span class="meta" style="white-space:nowrap;">Skip track</span>
    <button class="copy-btn" onclick="copyUrl(this, '{base}/skip')">Copy</button>
  </div>
  <div class="roon-url">
    <code>{base}/prev</code>
    <span class="meta" style="white-space:nowrap;">Previous track</span>
    <button class="copy-btn" onclick="copyUrl(this, '{base}/prev')">Copy</button>
  </div>

  <hr>
  <p class="section-label">Listen in browser</p>
  <audio controls src="/stream"></audio>

  <hr>
  <p class="section-label">Play History</p>
  <div class="history">{self._render_history()}</div>
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
