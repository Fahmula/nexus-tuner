import asyncio
from datetime import datetime
from typing import Any, Awaitable, Callable, NoReturn, Self

from nexus_stream.config import Config, VideoKey, VideoType
from nexus_stream.create_stream import MPEGTS_PACKET_SIZE
from nexus_stream.stream import StreamManager

BUFFER_CLEANUP_INTERVAL = 10
BUFFER_SIZE_LIMIT = 100 * 1024 * 1024
MPEGTS_CHUNK_SIZE = MPEGTS_PACKET_SIZE * 21  # Size of a chunk in bytes


class MPEGTSStream:
    streams: dict[VideoKey, Self] = {}
    
    def __init__(self, config: Config, stream_manager: StreamManager, video_key: VideoKey, recreate_stream: Callable[[], Awaitable[None]]) -> None:
        self.config = config
        self.stream_manager = stream_manager
        self.video_key = video_key
        self.recreate_stream = recreate_stream
        self._buffer: list[bytes] = []
        self._event = asyncio.Event()
        self._reader_positions: dict[int, int] = {}
        self._cancelled = False
        self._writer: asyncio.Task[NoReturn]
        self._cleaner: asyncio.Task[NoReturn]

    async def _initialize(self) -> None:
        process_info = await self.stream_manager.get_ffmpeg_process_info(self.video_key)
        if not process_info:
            raise ValueError(f"Internal error: MPEGTS FFmpeg process not found for '{self.video_key}'.")
        self._writer = asyncio.create_task(self._fill_buffer(process_info))
        self._cleaner = asyncio.create_task(self._cleanup())

    @classmethod
    async def register(cls, config: Config, stream_manager: StreamManager, video_key: VideoKey, recreate_stream: Callable[[], Awaitable[None]]) -> tuple[Self, int]:
        if video_key not in cls.streams or cls.streams[video_key]._cancelled:
            instance = cls(config, stream_manager, video_key, recreate_stream)
            cls.streams[video_key] = instance
            await instance._initialize()
        else:
            instance = cls.streams[video_key]
        return instance, await instance._register()
        
    async def _register(self) -> int:
        reader_id = max(self._reader_positions.keys(), default=-1) + 1
        self._reader_positions[reader_id] = 0
        return reader_id

    async def unregister(self, reader_id: int) -> None:
        del self._reader_positions[reader_id]
        if not len(self._reader_positions):
            self.config.log_message(f"[{VideoType.MPEGTS}] Inactive MPEGTS stream for '{self.video_key}' as no readers are registered.")
            await self._cancel()

    async def read(self, reader_id: int) -> bytes:
        if self._reader_positions[reader_id] >= len(self._buffer):  
            await self._event.wait()
        if self._cancelled:
            raise asyncio.CancelledError("MPEGTS stream has been cancelled.")
        buf = self._buffer[self._reader_positions[reader_id]]
        self._reader_positions[reader_id] += 1
        return buf

    async def _fill_buffer(self, process_info: dict[str, Any]) -> NoReturn:
        try:
            while True:
                process_info["is_mpegts_active"] = True
                stdout: asyncio.StreamReader = process_info["process"].stdout
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
                    self.config.log_message(f"[{VideoType.MPEGTS}] Error reading from MPEGTS stream for '{process_info['logical_channel_name']}' with key '{self.video_key}': {e}", level="ERROR")
                    await self.stream_manager.stop_ffmpeg_process(self.video_key, process_info["logical_channel_name"])
                    await self.recreate_stream()
                    process_info_res = await self.stream_manager.get_ffmpeg_process_info(self.video_key)
                    if not process_info_res:
                        self.config.log_message(f"[{VideoType.MPEGTS}] Internal error: MPEGTS FFmpeg process not found for logical channel '{process_info['logical_channel_name']}' with key '{self.video_key}' after recreating stream.", level="ERROR")
                        raise
                    process_info = process_info_res
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            self.config.log_message(f"[{VideoType.MPEGTS}] Unexpected error in MPEGTS stream for '{self.video_key}': {e}", level="ERROR")
            raise
        finally:  # Don't stop early incase the user reconnects, let the timeout handle it
            await self._cancel()
            async with self.stream_manager.stream_process_lock:
                process_info["last_access"] = datetime.now()
                process_info["is_mpegts_active"] = False

    async def _cleanup(self) -> NoReturn:
        max_entries = BUFFER_SIZE_LIMIT // MPEGTS_CHUNK_SIZE
        while True:
            await asyncio.sleep(BUFFER_CLEANUP_INTERVAL)
            min_index = min(self._reader_positions.values(), default=len(self._buffer))
            self._buffer = self._buffer[min_index:]
            for reader_id in self._reader_positions:
                self._reader_positions[reader_id] -= min_index
            if len(self._buffer) > max_entries:
                dropped = len(self._buffer) - max_entries
                self._buffer = self._buffer[-max_entries:]
                for reader_id in self._reader_positions:
                    self._reader_positions[reader_id] = max(0, self._reader_positions[reader_id] - dropped)

    async def _cancel(self) -> None:
        self.streams.pop(self.video_key, None)
        self._cancelled = True  # Let the stdout reader finish gracefully incase we will reconnect
        self._cleaner.cancel()
        self._event.set()  # Ensure any waiting readers are woken up
