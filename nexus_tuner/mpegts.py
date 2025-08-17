import asyncio
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Final, NoReturn, Self, cast

from quart import Response

from nexus_tuner.config import Config
from nexus_tuner.stream import StreamManager
from nexus_tuner.utils import MPEGTS_PACKET_SIZE, FFmpegProcessInfoMutable, Log, MPEGTSProcessInfo, ReaderId, VideoKey, VideoType, run_bg

BUFFER_CLEANUP_INTERVAL: Final[int] = 10
BUFFER_SIZE_LIMIT: Final[int] = 100 * 1024 * 1024
MPEGTS_CHUNK_SIZE: Final[int] = MPEGTS_PACKET_SIZE * 21


class MPEGTSStream:
    """A class to manage MPEGTS streams, handling reading and buffering of data."""
    __slots__ = (
        'config', 'stream_manager', 'video_key', 'recreate_stream',
        '_buffer', '_event', '_reader_positions', '_cancelled',
        '_writer', '_cleaner',
    )

    streams: dict[VideoKey, Self] = {}
    
    def __init__(self, config: Config, stream_manager: StreamManager, video_key: VideoKey, recreate_stream: Callable[[], Awaitable[Response | VideoKey]]) -> None:
        self.config: Config = config
        self.stream_manager: StreamManager = stream_manager
        self.video_key: VideoKey = video_key
        self.recreate_stream: Callable[[], Awaitable[Response | VideoKey]] = recreate_stream
        self._buffer: list[bytes] = []
        self._event: asyncio.Event = asyncio.Event()
        self._reader_positions: dict[ReaderId, int] = {}
        self._cancelled: bool = False
        self._writer: asyncio.Task[NoReturn]
        self._cleaner: asyncio.Task[NoReturn]

    async def _initialize(self) -> None:
        """Initialize the MPEGTS stream by setting up the buffer and starting the writer task."""
        process_info = cast(MPEGTSProcessInfo | None, await self.stream_manager.get_ffmpeg_process_info(self.video_key))
        if not process_info:
            raise ValueError(f"Internal error: MPEGTS FFmpeg process not found for '{self.video_key}'.")
        self._writer = asyncio.create_task(self._fill_buffer(process_info))
        self._cleaner = asyncio.create_task(self._cleanup())

    @classmethod
    async def register(cls, config: Config, stream_manager: StreamManager, video_key: VideoKey, recreate_stream: Callable[[], Awaitable[Response | VideoKey]]) -> tuple[Self, ReaderId]:
        """Create a new MPEGTS stream or return an existing one."""
        if video_key not in cls.streams:  # If stream is cancelled, still choose it to prevent dual ownership
            instance = cls(config, stream_manager, video_key, recreate_stream)
            cls.streams[video_key] = instance
            try:
                await instance._initialize()
            except:
                del cls.streams[video_key]
                raise
        else:
            Log.debug(VideoType.MPEGTS, f"Reusing existing MPEGTS stream for '{video_key}'")
            instance = cls.streams[video_key]
        return instance, instance._register()

    def _register(self) -> ReaderId:
        """Register a new reader for the MPEGTS stream and return its ID."""
        reader_id = ReaderId(max(self._reader_positions.keys(), default=-1) + 1)
        self._reader_positions[reader_id] = 0
        Log.debug(VideoType.MPEGTS, f"Registering Client #{reader_id} for MPEGTS stream '{self.video_key}': {len(self._reader_positions)} readers total.")
        return reader_id

    def unregister(self, reader_id: ReaderId) -> None:
        """Unregister a reader from the MPEGTS stream, will stop the stream if no readers are left."""
        del self._reader_positions[reader_id]
        Log.debug(VideoType.MPEGTS, f"Unregistering Client #{reader_id} from MPEGTS stream for '{self.video_key}': {len(self._reader_positions)} readers left.")
        if not len(self._reader_positions):
            self.shutdown()

    async def read(self, reader_id: ReaderId) -> bytes:
        """Read a chunk of data from the MPEGTS stream for the given reader ID, blocking until data is available.
        Raises:
            asyncio.CancelledError: If the stream is unrecoverable.
        """
        if self._reader_positions[reader_id] >= len(self._buffer):
            await self._event.wait()
        if self._cancelled:
            raise asyncio.CancelledError("MPEGTS stream has been cancelled.")
        buf = self._buffer[self._reader_positions[reader_id]]
        self._reader_positions[reader_id] += 1
        return buf

    async def _fill_buffer(self, process_info: MPEGTSProcessInfo) -> NoReturn:
        """Continuously read data from the MPEGTS stream and fill the buffer, notifying readers when new data is available."""
        try:
            while True:
                Log.debug(VideoType.MPEGTS, f"Marking MPEGTS stream for '{process_info['logical_channel_title']}' with key '{self.video_key}' as active.")
                cast(FFmpegProcessInfoMutable, process_info)["is_mpegts_active"] = True
                stdout: asyncio.StreamReader = cast(asyncio.StreamReader, process_info["process"].stdout)
                try:
                    while True:
                        chunk = await stdout.readexactly(MPEGTS_CHUNK_SIZE)
                        if self._cancelled:
                            raise asyncio.CancelledError()  # Ensure we finish reading properly
                        if not chunk:
                            raise EOFError("End of stream reached")
                        self._buffer.append(chunk)
                        self._event.set()
                        self._event = asyncio.Event()
                except Exception as e:
                    if self._cancelled:
                        raise asyncio.CancelledError()
                    Log.error(VideoType.MPEGTS, f"Error reading from MPEGTS stream for '{process_info['logical_channel_title']}' with key '{self.video_key}': {e}")
                    await self.stream_manager.stop_ffmpeg_process(self.video_key, process_info["logical_channel_title"])
                    res = await self.recreate_stream()
                    if isinstance(res, Response):
                        Log.error(VideoType.MPEGTS, f"Failed to recreate MPEGTS stream for '{process_info['logical_channel_title']}' with key '{self.video_key}': {res.status}")
                        raise
                    del self.streams[self.video_key]
                    self.video_key = res
                    self.streams[self.video_key] = self
                    process_info_res = cast(MPEGTSProcessInfo | None, await self.stream_manager.get_ffmpeg_process_info(self.video_key))
                    if not process_info_res:
                        Log.error(VideoType.MPEGTS, f"Internal error: MPEGTS FFmpeg process not found for logical channel '{process_info['logical_channel_title']}' with key '{self.video_key}' after recreating stream.")
                        raise
                    process_info = process_info_res
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            Log.error(VideoType.MPEGTS, f"Unexpected error in MPEGTS stream for '{self.video_key}': {e}")
            raise
        finally:  # Don't stop early incase the user reconnects, let the timeout handle it
            async def bg_cleanup() -> None:
                self.shutdown()
                async with self.stream_manager.stream_process_lock:
                    Log.debug(VideoType.MPEGTS, f"Marking MPEGTS stream for '{process_info['logical_channel_title']}' with key '{self.video_key}' as inactive.")
                    cast(FFmpegProcessInfoMutable, process_info)["last_access"] = datetime.now() - timedelta(seconds=self.config.segment_prune_timeout)  # Elligible for pruning immediately
                    cast(FFmpegProcessInfoMutable, process_info)["is_mpegts_active"] = False
                    del self.streams[self.video_key]  # Make this atomic with is_mpegts_active
            run_bg(bg_cleanup())

    async def _cleanup(self) -> NoReturn:
        """Periodically clean up the buffer to prevent excessive memory usage."""
        max_entries = BUFFER_SIZE_LIMIT // MPEGTS_CHUNK_SIZE
        while True:
            await asyncio.sleep(BUFFER_CLEANUP_INTERVAL)
            min_index = min(self._reader_positions.values(), default=len(self._buffer))
            self._buffer = self._buffer[min_index:]
            for reader_id in self._reader_positions:
                self._reader_positions[reader_id] -= min_index
            if len(self._buffer) > max_entries:
                dropped = len(self._buffer) - max_entries
                Log.debug(VideoType.MPEGTS, f"Cleaning up MPEGTS buffer for '{self.video_key}': {len(self._buffer)} entries, dropping {dropped} entries.")
                self._buffer = self._buffer[-max_entries:]
                for reader_id in self._reader_positions:
                    new_pos = max(0, self._reader_positions[reader_id] - dropped)
                    Log.debug(VideoType.MPEGTS, f"--- Adjusting Client #{reader_id} position from {self._reader_positions[reader_id]} to {new_pos}.")
                    self._reader_positions[reader_id] = new_pos

    def shutdown(self) -> None:
        """Cancel the MPEGTS stream, cleaning up resources and stopping the writer task."""
        Log.debug(VideoType.MPEGTS, f"Shutting down MPEGTS stream for '{self.video_key}'...")
        self._cancelled = True  # Let the stdout reader finish gracefully incase we will reconnect
        self._cleaner.cancel()
        self._event.set()  # Ensure any waiting readers are woken up
