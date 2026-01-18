import asyncio
from datetime import datetime, timedelta
import time
from typing import Awaitable, Callable, Final, NoReturn, Self, cast

from quart import Response

from nexus_tuner.config import Config
from nexus_tuner.stream import StreamManager
from nexus_tuner.utils import (MPEGTS_CHUNK_READ_TIMEOUT, MPEGTS_CHUNK_SIZE, LogicalChannelId, MPEGTSHealthImpl, ProcessInfoMutable, Label, Log, MPEGTSProcessInfo, ReaderId,
                               StopReason, StreamEngine, VideoKey, VideoName, VideoType, run_bg)

BUFFER_CLEANUP_INTERVAL: Final[int] = 5
BUFFER_SIZE_LIMIT: Final[int] = 100 * 1024 * 1024


class MPEGTSStream:
    """A class to manage MPEGTS streams, handling reading and buffering of data."""
    __slots__ = (
        'config', 'stream_manager', 'logical_channel_id', 'video_key', 'video_name',
        'stream_engine', 'recreate_stream', '_buffer', '_event', '_reader_positions', 
        '_total_bytes_read', '_started_at', '_cancelled', '_writer', '_cleaner',
    )

    streams: dict[VideoKey, Self] = {}
    
    def __init__(self, config: Config, stream_manager: StreamManager, logical_channel_id: LogicalChannelId, video_key: VideoKey, video_name: VideoName, stream_engine: StreamEngine, recreate_stream: Callable[[], Awaitable[Response | VideoKey]]) -> None:
        self.config: Config = config
        self.stream_manager: StreamManager = stream_manager
        self.logical_channel_id: LogicalChannelId = logical_channel_id
        self.video_key: VideoKey = video_key
        self.video_name: VideoName = video_name
        self.stream_engine: StreamEngine = stream_engine
        self.recreate_stream: Callable[[], Awaitable[Response | VideoKey]] = recreate_stream
        self._buffer: list[bytes] = []
        self._event: asyncio.Event = asyncio.Event()
        self._reader_positions: dict[ReaderId, int] = {}
        self._total_bytes_read: int = 0
        self._started_at: float | None = None
        self._cancelled: bool = False
        self._writer: asyncio.Task[NoReturn]
        self._cleaner: asyncio.Task[NoReturn]

    async def _initialize(self) -> None:
        """Initialize the stream by setting up the buffer and starting the writer task."""
        process_info = cast(MPEGTSProcessInfo | None, await self.stream_manager.get_process_info(self.video_key))
        if not process_info:
            raise ValueError(f"{self.video_name}: Internal error: MPEGTS process not found with '{self.video_key}'.")
        self._writer = asyncio.create_task(self._fill_buffer(process_info))
        self._cleaner = asyncio.create_task(self._cleanup())

    @classmethod
    async def register(cls, config: Config, stream_manager: StreamManager, logical_channel_id: LogicalChannelId, video_key: VideoKey, video_name: VideoName, stream_engine: StreamEngine, recreate_stream: Callable[[], Awaitable[Response | VideoKey]]) -> tuple[Self, ReaderId]:
        """Create a new stream or return an existing one."""
        if video_key not in cls.streams:  # If stream is cancelled, still choose it to prevent dual ownership
            instance = cls(config, stream_manager, logical_channel_id, video_key, video_name, stream_engine, recreate_stream)
            cls.streams[video_key] = instance
            Log.debug(Label.STREAM, f"{instance.video_name}: Created stream handler", (VideoType.MPEGTS, stream_engine))
            try:
                await instance._initialize()
            except BaseException:
                cls.streams.pop(video_key, None)
                raise
        else:
            instance = cls.streams[video_key]
            Log.debug(Label.STREAM, f"{instance.video_name}: Reusing existing stream handler", (VideoType.MPEGTS, stream_engine))
        return instance, instance._register()

    def _register(self) -> ReaderId:
        """Register a new reader for the stream and return its ID."""
        reader_id = ReaderId(max(self._reader_positions.keys(), default=0) + 1)
        self._reader_positions[reader_id] = 0
        Log.debug(Label.STREAM, f"{self.video_name}: Registering Client #{reader_id} - {len(self._reader_positions)} readers total.", (VideoType.MPEGTS, self.stream_engine))
        return reader_id

    def unregister(self, reader_id: ReaderId) -> None:
        """Unregister a reader from the stream, will stop the stream if no readers are left."""
        del self._reader_positions[reader_id]
        Log.debug(Label.STREAM, f"{self.video_name}: Unregistering Client #{reader_id} - {len(self._reader_positions)} readers left.", (VideoType.MPEGTS, self.stream_engine))
        if not len(self._reader_positions):
            self.shutdown()

    async def read(self, reader_id: ReaderId) -> bytes:
        """Read a chunk of data from the stream for the given reader ID, blocking until data is available.
        Raises:
            asyncio.CancelledError: If the stream is unrecoverable.
        """
        if self._reader_positions[reader_id] >= len(self._buffer):
            await self._event.wait()
        if self._cancelled:
            raise asyncio.CancelledError("stream has been shutdown.")
        buf = self._buffer[self._reader_positions[reader_id]]
        self._reader_positions[reader_id] += 1
        return buf

    async def _fill_buffer(self, process_info: MPEGTSProcessInfo) -> NoReturn:
        """Continuously read data from the stream and fill the buffer, notifying readers when new data is available."""
        try:
            while True:
                Log.debug(Label.STREAM, f"{self.video_name}: Marking as active.", (VideoType.MPEGTS, self.stream_engine))
                cast(ProcessInfoMutable, process_info)["is_mpegts_active"] = True
                if process_info["mpegts_health"]:
                    cast(MPEGTSHealthImpl, process_info["mpegts_health"])["stop_read"] = True
                    await process_info["mpegts_health"]["stopped"].wait()
                    total_elapsed = time.monotonic() - process_info["mpegts_health"]["started_at"]
                    bytes_read = sum(len(chunk) for chunk in process_info["mpegts_health"]["buffer"])
                    mbps = (bytes_read * 8) / 1_000_000 / total_elapsed if total_elapsed else 0
                    Log.debug(Label.STREAM, f"{self.video_name}: Initializing buffer with {len(process_info['mpegts_health']['buffer']):,} chunks ({bytes_read:,} bytes) over {total_elapsed:.3f}s ({mbps:.2f}mbps).", (VideoType.MPEGTS, self.stream_engine))
                    if not self._started_at:
                        self._started_at = process_info["mpegts_health"]["started_at"]
                    self._total_bytes_read += bytes_read
                    self._buffer.extend(process_info["mpegts_health"]["buffer"])
                    if process_info["mpegts_health"]["is_healthy"]:
                        self._event.set()
                        self._event = asyncio.Event()
                    else:
                        Log.error(Label.STREAM, f"{self.video_name}: Stream ended while transitioning ownership from health check to stream handler.", (VideoType.MPEGTS, self.stream_engine))
                    cast(ProcessInfoMutable, process_info)["mpegts_health"] = None
                else:
                    self._started_at = time.monotonic()
                    Log.debug(Label.STREAM, f"{self.video_name}: No health check data to initialize with.", (VideoType.MPEGTS, self.stream_engine))
                stdout: asyncio.StreamReader = cast(asyncio.StreamReader, process_info["process"].stdout)
                try:
                    while True:
                        chunk = await asyncio.wait_for(stdout.readexactly(MPEGTS_CHUNK_SIZE), timeout=MPEGTS_CHUNK_READ_TIMEOUT)
                        self._total_bytes_read += len(chunk)
                        if self._cancelled:
                            raise asyncio.CancelledError()  # Ensure we finish reading properly
                        if not chunk:
                            raise EOFError("End of stream reached")
                        self._buffer.append(chunk)
                        self._event.set()
                        self._event = asyncio.Event()
                except Exception as e:
                    if isinstance(e, asyncio.TimeoutError):
                        Log.error(Label.STREAM, f"{self.video_name}: Process read stalled, no new data received in {MPEGTS_CHUNK_READ_TIMEOUT}s.", (VideoType.MPEGTS, self.stream_engine))
                        if not process_info["stop_reason"]:
                            cast(ProcessInfoMutable, process_info)["stopped_at"] = datetime.now()
                            cast(ProcessInfoMutable, process_info)["stop_reason"] = StopReason.STALLED
                            Log.debug(Label.STREAM, f"{self.video_name}: Updated stopped timestamp with {StopReason.STALLED}.", (VideoType.MPEGTS, self.stream_engine))
                        await self.stream_manager.stop_process(self.video_key)
                        if self._cancelled:
                            raise asyncio.CancelledError()
                    else:
                        if self._cancelled:
                            raise asyncio.CancelledError()
                        Log.error(Label.STREAM, f"{self.video_name}: Error reading from stream - {e}", (VideoType.MPEGTS, self.stream_engine))
                    if not process_info["stop_reason"]:
                        cast(ProcessInfoMutable, process_info)["stopped_at"] = datetime.now()
                        cast(ProcessInfoMutable, process_info)["stop_reason"] = StopReason.ERROR
                        Log.debug(Label.STREAM, f"{self.video_name}: Updated stopped timestamp with {StopReason.ERROR}.", (VideoType.MPEGTS, self.stream_engine))
                    await self.stream_manager.stop_process(self.video_key)
                    res = await self.recreate_stream()
                    if isinstance(res, Response):
                        Log.error(Label.STREAM, f"{self.video_name}: Failed to recreate stream - {res.status}", (VideoType.MPEGTS, self.stream_engine))
                        raise
                    self.streams.pop(self.video_key, None)
                    self.video_key = res
                    self.streams[self.video_key] = self
                    process_info_res = cast(MPEGTSProcessInfo | None, await self.stream_manager.get_process_info(self.video_key))
                    if not process_info_res:
                        Log.error(Label.STREAM, f"Internal error: Process not found with key '{self.video_key}' after recreating stream.", (VideoType.MPEGTS, self.stream_engine))
                        raise
                    process_info = process_info_res
                    self.video_name = process_info['video_name']
                    self.stream_engine = process_info['stream_engine']
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            Log.error(Label.STREAM, f"{self.video_name}: Unexpected error - {e}", (VideoType.MPEGTS, self.stream_engine))
            raise
        finally:  # Don't stop early in case the user reconnects, let the timeout handle it
            async def bg_cleanup() -> None:
                Log.debug(Label.STREAM, f"{self.video_name}: Shutting down stream handler...", (VideoType.MPEGTS, self.stream_engine))
                self.shutdown()
                self._cleaner.cancel()
                async with self.stream_manager.stream_process_lock:
                    Log.debug(Label.STREAM, f"{self.video_name}: Marking as inactive.", (VideoType.MPEGTS, self.stream_engine))
                    cast(ProcessInfoMutable, process_info)["last_access"] = datetime.now() - timedelta(seconds=self.config.segment_prune_timeout)  # Eligible for pruning immediately
                    cast(ProcessInfoMutable, process_info)["is_mpegts_active"] = False
                    self.streams.pop(self.video_key, None)  # Make this atomic with is_mpegts_active
            run_bg(bg_cleanup())

    async def _cleanup(self) -> NoReturn:
        """Periodically clean up the buffer to prevent excessive memory usage."""
        max_entries = BUFFER_SIZE_LIMIT // MPEGTS_CHUNK_SIZE
        try:
            await self._event.wait()
            while True:
                await asyncio.sleep(BUFFER_CLEANUP_INTERVAL)

                # Drop the chunks all readers have sent and shift all their positions.
                min_index = min(self._reader_positions.values(), default=len(self._buffer))
                del self._buffer[:min_index]
                for reader_id in self._reader_positions:
                    self._reader_positions[reader_id] -= min_index

                if len(self._buffer) <= max_entries:
                    continue
                dropped = len(self._buffer) - max_entries
                Log.warn(Label.STREAM, f"{self.video_name}: Cleaning up buffer, {len(self._buffer)} entries ({sum(len(chunk) for chunk in self._buffer)} bytes), dropping {dropped} entries (will cause skips).", (VideoType.MPEGTS, self.stream_engine))
                for reader_id in self._reader_positions:
                    skip_msg = f"will cause skips" if dropped > self._reader_positions[reader_id] else "no skips"
                    new_pos = max(0, self._reader_positions[reader_id] - dropped)
                    Log.debug(Label.STREAM, f"--- Adjusting Client #{reader_id} position from {self._reader_positions[reader_id]} to {new_pos} ({skip_msg}).", (VideoType.MPEGTS, self.stream_engine))
                    self._reader_positions[reader_id] = new_pos
                del self._buffer[:dropped]
                Log.debug(Label.STREAM, f"{self.video_name}: Cleaned up buffer, new size at {len(self._buffer)} entries ({sum(len(chunk) for chunk in self._buffer)} bytes).", (VideoType.MPEGTS, self.stream_engine))
        finally:
            total_elapsed = time.monotonic() - self._started_at if self._started_at else 0
            mbps = (self._total_bytes_read * 8) / 1_000_000 / total_elapsed if total_elapsed else 0
            Log.debug(Label.STREAM, f"{self.video_name}: Read {self._total_bytes_read:,} bytes at {mbps:.2f}mbps for {total_elapsed:,.3f}s.", (VideoType.MPEGTS, self.stream_engine))

    def shutdown(self) -> None:
        """Cancel the stream, cleaning up resources and stopping the writer task."""
        self._cancelled = True  # Let the stdout reader finish gracefully in case we will reconnect
        self._event.set()  # Ensure any waiting readers are woken up
