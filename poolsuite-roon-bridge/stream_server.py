from __future__ import annotations

"""HTTP streaming server that serves a continuous MP3 stream.

Listeners (including Roon) connect to /stream and receive a
never-ending MP3 audio stream, like an internet radio station.
ICY metadata is optionally injected for track titles.
"""

import asyncio
import logging
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
        self._channel_change_event: asyncio.Event = asyncio.Event()
        self._pending_channel: str | None = None
        self._app = web.Application()
        self._app.router.add_get("/stream", self._handle_stream)
        self._app.router.add_get("/stream.mp3", self._handle_stream)
        self._app.router.add_get("/status", self._handle_status)
        self._app.router.add_post("/skip", self._handle_skip)
        self._app.router.add_get("/skip", self._handle_skip)
        self._app.router.add_get("/channel", self._handle_channel)
        self._app.router.add_get("/", self._handle_index)
        self._started_at = time.time()
        self._tracks_played = 0

    @property
    def stream_url(self) -> str:
        return f"http://{self.host}:{self.port}/stream"

    def set_now_playing(self, title: str) -> None:
        self._now_playing = title
        self._tracks_played += 1
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

    async def _handle_index(self, request: web.Request) -> web.Response:
        """Simple landing page."""
        channel_buttons = ""
        all_channels = ["All"] + self._available_channels
        for ch in all_channels:
            is_current = (ch == self._current_channel)
            bg = "#64dfdf" if is_current else "#e0d68a"
            color = "#1a1a2e"
            border = "3px solid #64dfdf" if is_current else "3px solid transparent"
            channel_buttons += (
                f'<a href="/channel?name={ch}" style="display: inline-block; '
                f'padding: 0.5em 1em; margin: 0.3em; background: {bg}; color: {color}; '
                f'text-decoration: none; font-weight: bold; border: {border};">{ch}</a>\n'
            )

        html = f"""<!DOCTYPE html>
<html>
<head><title>Poolsuite Roon Bridge</title></head>
<body style="font-family: monospace; background: #1a1a2e; color: #e0d68a; padding: 2em;">
  <h1>🌴 Poolsuite → Roon Bridge</h1>
  <p>Now Playing: <strong>{self._now_playing}</strong></p>
  <p>Channel: <strong>{self._current_channel}</strong></p>
  <p>Listeners: {len(self._listeners)} | Tracks played: {self._tracks_played}</p>
  <hr>
  <h3>Channels</h3>
  <div style="margin: 0.5em 0 1.5em 0;">{channel_buttons}</div>
  <hr>
  <div style="margin: 1em 0;">
    <a href="/skip" style="display: inline-block; padding: 0.8em 2em; background: #e0d68a; color: #1a1a2e; text-decoration: none; font-weight: bold; font-size: 1.1em;">Skip Track &raquo;</a>
  </div>
  <hr>
  <p>Stream URL: <a href="/stream" style="color: #64dfdf;">{self.stream_url}</a></p>
  <p>Add <code>{self.stream_url}</code> as a Live Radio station in Roon.</p>
  <audio controls src="/stream" style="width: 100%; margin-top: 1em;">
    Your browser does not support the audio element.
  </audio>
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
