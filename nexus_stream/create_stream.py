import asyncio
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Coroutine

# Refactor Note: Replaced threading and concurrent.futures with asyncio for native async concurrency.
# Refactor Note: Replaced shutil with aioshutil for non-blocking file system operations.
# Refactor Note: Imported aiofiles for async file I/O, especially for FFmpeg logs.
import aioshutil
import aiofiles
import aiofiles.os

# Refactor Note: All dependent modules are now the async versions.
# Feature Add: Imported new generalized types and constants.
from nexus_stream.config import CREATE_STREAM_DEADLINE, NEW_DEADLINE_NON_BEST, Config, VideoKey, VideoName, VideoType
from nexus_stream.quality_monitor import QualityMonitor
from nexus_stream.slots import ProviderName, ProviderSlots
# Feature Add: Renamed HLSStreamManager to the more generic StreamManager.
from nexus_stream.stream import ChannelHandler, StreamManager


CREATE_STREAM_POLL_INTERVAL = 0.01
# Feature Add: Constants for MPEG-TS stream validation.
MPEGTS_PACKET_SIZE = 188       # Size of a single MPEG-TS packet in bytes
MPEGTS_PACKETS_PER_CHUNK = 21  # Number of packets to read at once in the MPEG-TS stream


# --- Helper functions (remain synchronous as they are pure CPU-BOUND) ---

def create_video_key(logical_channel_id: str, source_service_id: str, video_type: VideoType) -> VideoKey:
    """Generates a unique key for the stream."""
    return VideoKey(f"{video_type}_{logical_channel_id}_{source_service_id}")


def create_video_name(logical_channel_name: str, source_service_id: str, video_type: VideoType) -> VideoName:
    """Generates a unique name for the stream."""
    return VideoName(f"[{video_type}] {logical_channel_name} - {source_service_id}")


# Refactor Note: This function is now async to perform non-blocking directory creation.
async def create_hls_ffmpeg_command(stream_manager: StreamManager, config: Config, input_url: str, video_key: VideoKey) -> tuple[list[str], Path]:
    """Constructs the FFmpeg command list and creates the necessary HLS directory asynchronously."""
    channel_hls_dir = stream_manager.hls_base_dir / config.get_fs_safe_alphanum(video_key)
    # Refactor Note: Replaced blocking Path.mkdir with aiofiles.os.makedirs for non-blocking I/O.
    await aiofiles.os.makedirs(channel_hls_dir, exist_ok=True)
    
    playlist_path = channel_hls_dir / "playlist.m3u8"
    segment_filename = channel_hls_dir / "segment_%05d.ts"
    
    command = [
        config.ffmpeg_path,
        "-hide_banner", "-loglevel", "info",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4",
        "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
        "-user_agent", "NexusStream/1.0 (FFMPEG-HLS)",
        "-i", input_url,
        "-codec", "copy",
        "-map", "0:v:0?", "-map", "0:a:0?",
        "-f", "hls",
        "-hls_time", str(config.hls_segment_duration),
        "-hls_list_size", str(config.hls_playlist_length),
        "-hls_flags", "delete_segments+omit_endlist+program_date_time",
        "-hls_segment_filename", str(segment_filename),
        str(playlist_path)
    ]
    return command, channel_hls_dir


def create_mpegts_ffmpeg_command(config: Config, input_url: str) -> list[str]:
    """Constructs the FFmpeg command list to output a continuous MPEG-TS stream."""
    command = [
        config.ffmpeg_path,
        "-hide_banner", "-loglevel", "info",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4",
        "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
        "-user_agent", "NexusStream/1.0 (FFMPEG-MPEGTS)",
        "-i", input_url,
        "-c:v", "copy",
        "-c:a", "libmp3lame",
        "-map", "0:v:0?", "-map", "0:a:0?",
        "-f", "mpegts",
        "pipe:1"
    ]
    return command


def sort_sources(sources: list[dict[str, Any]], quality_scores: dict[str, dict[str, float]], *, reverse: bool) -> dict[str, int]:
    """Sorts sources based on priority and quality. (Sync - CPU-bound pure function)"""
    prev_score = None
    curr_priority = -1
    source_scores = sorted(((source["priority"],
                             -quality_scores.get(source["source_service_id"], {}).get("total_score", 0),
                             -quality_scores.get(source["source_service_id"], {}).get("uptime", 0),
                             source["source_service_id"]),
                    source["source_service_id"]) for source in sources)
    source_priorities: dict[str, int] = {}
    for score, source_service_id in source_scores:
        if score != prev_score:
            prev_score = score
            curr_priority += 1
        source_priorities[source_service_id] = curr_priority
    sources.sort(key=lambda x: source_priorities[x["source_service_id"]], reverse=reverse)
    return source_priorities


class CreateStream:
    """
    An asyncio-native class to acquire and use resources for creating a stream.
    The original threaded logic is preserved using asyncio tasks and synchronization primitives.
    """
    def __init__(self, config: Config, handler: ChannelHandler, stream_manager: StreamManager, quality_monitor: QualityMonitor, logical_channel_id: str, logical_channel_name: str, video_type: VideoType, input_sources: list[dict[str, Any]] | None = None) -> None:
        # Refactor Note: The __init__ method is now lightweight and synchronous.
        # All async operations and task creation are moved to the `_initialize_and_start` method.
        self.config = config
        self.handler = handler
        self.stream_manager = stream_manager
        self.quality_monitor = quality_monitor
        self.logical_channel_id = logical_channel_id
        self.logical_channel_name = logical_channel_name
        self.video_type = video_type
        
        self._res: VideoKey | tuple[int, str] = (500, f"[{video_type}] Stream not created yet")
        # Refactor Note: Replaced threading.Lock with asyncio.Lock for coroutine-safe state management.
        self._mutex = asyncio.Lock()
        # Refactor Note: Replaced a blocking mutex on result() with an asyncio.Event.
        # This allows `result()` to `await` completion without blocking the event loop.
        self._result_event = asyncio.Event()

        self._sources: list[dict[str, Any]] = []
        self._quality_scores: dict[str, dict[str, float]] = {}
        self._remaining_priorities: dict[str, int] = {}
        self._input_sources = input_sources

        self._results: list[tuple[VideoKey, dict[str, Any]]] = []
        self._selected = False
        self._active_video_keys: list[VideoKey] = []
        self._source_quality_messages: dict[str, str] = {}
        self._video_names: dict[VideoKey, VideoName] = {}
        self._deadline: float = 0.0 
        # Refactor Note: Separate lists for supervisor and worker tasks.
        self._worker_tasks: list[asyncio.Task] = []
        self._supervisor_task: asyncio.Task | None = None

    @classmethod
    async def create(cls, *args, **kwargs) -> "CreateStream":
        """
        Asynchronously creates and initializes the stream creation process.
        This factory pattern is idiomatic for async classes that need to perform
        async operations during initialization.
        """
        instance = cls(*args, **kwargs)
        await instance._initialize_and_start()
        return instance

    async def _initialize_and_start(self) -> None:
        """Performs async setup and launches all processing tasks."""
        loop = asyncio.get_running_loop()
        self._deadline = loop.time() + CREATE_STREAM_DEADLINE
        self._sources = self.handler.get_sources_for_client_facing_channel(self.logical_channel_id) if self._input_sources is None else deepcopy(self._input_sources)
        if not self._sources:
            self._res = (404, f"[{self.video_type}] Logical channel '{self.logical_channel_name}' ({self.logical_channel_id}) not found or has no sources.")
            self._result_event.set()
            return

        self._quality_scores = await self.quality_monitor.get_quality_scores()
        self._remaining_priorities = sort_sources(self._sources, self._quality_scores, reverse=True)

        all_provider_sources: dict[ProviderName, list[dict[str, Any]]] = {}
        for source in self._sources:
            all_provider_sources.setdefault(ProviderName(source["provider_alias"]), []).append(source)

        # Refactor Note: Replaced threading.Thread with asyncio.create_task to run background coroutines.
        self._supervisor_task = asyncio.create_task(self._process_results())

        for provider_alias, provider_sources in all_provider_sources.items():
            task = asyncio.create_task(self._create_provider_stream(provider_alias, provider_sources))
            self._worker_tasks.append(task)


    async def result(self) -> VideoKey | tuple[int, str]:
        """Awaits the result of the stream creation process without blocking the event loop."""
        await self._result_event.wait()
        return self._res

    # Refactor Note: All internal state accessors are now async to use the asyncio.Lock.
    async def _get_deadline(self) -> float:
        async with self._mutex:
            return self._deadline

    async def _get_selected(self) -> bool:
        async with self._mutex:
            return self._selected

    async def _set_deadline(self, source: dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        async with self._mutex:
            if self._remaining_priorities[source["source_service_id"]] <= min(self._remaining_priorities.values()):
                new_deadline = loop.time()
            else:
                # Feature Add: Use configured deadline constant.
                new_deadline = loop.time() + NEW_DEADLINE_NON_BEST
            self._deadline = min(self._deadline, new_deadline)

    async def _set_result(self, video_key: VideoKey, source: dict[str, Any]) -> bool:
        async with self._mutex:
            if self._selected:
                return False
            self._results.append((video_key, source))
            return True

    async def _remove_active_video_key(self, video_key: VideoKey) -> None:
        async with self._mutex:
            if video_key in self._active_video_keys:
                self._active_video_keys.remove(video_key)

    async def _pop_source(self, provider_sources: list[dict[str, Any]], current_source: dict[str, Any] | None) -> dict[str, Any] | None:
        async with self._mutex:
            if current_source:
                self._remaining_priorities.pop(current_source["source_service_id"], None)
            if provider_sources:
                return provider_sources.pop()
            return None

    async def _create_provider_stream(self, provider_alias: ProviderName, provider_sources: list[dict[str, Any]]) -> None:
        """Creates streams for a provider by launching concurrent worker tasks."""
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            self.config.log_message(f"{self.logical_channel_name}: Provider '{provider_alias}' does not exist.", level="CRITICAL")
            return

        status = await self.handler.get_provider_stream_status()
        max_streams = status[provider_alias]["max"]
        
        # Refactor Note: Replaced ThreadPoolExecutor with a list of asyncio.Tasks and asyncio.gather.
        # This achieves the same goal of running a fixed number of concurrent workers non-blockingly.
        worker_tasks = [
            asyncio.create_task(self._provider_worker_task(provider_alias, provider_slots, provider_sources))
            for _ in range(max_streams)
        ]
        await asyncio.gather(*worker_tasks, return_exceptions=False)

    async def _provider_worker_task(self, provider_alias: ProviderName, provider_slots: ProviderSlots, provider_sources: list[dict[str, Any]]) -> None:
        """Tries sources for a provider until a stream is created or sources are exhausted."""
        source = await self._pop_source(provider_sources, None)
        if not source:
            return
        
        video_key = create_video_key(self.logical_channel_id, source["source_service_id"], self.video_type)
        
        loop = asyncio.get_running_loop()
        while loop.time() < await self._get_deadline():
            if await self._get_selected():
                return

            # Refactor Note: The original logic of checking for an existing stream is preserved using an async lock.
            async with self.stream_manager.stream_process_lock:
                if video_key in self.stream_manager.ffmpeg_processes and self.stream_manager.ffmpeg_processes[video_key]["process"].returncode is None:
                    if not self.stream_manager.ffmpeg_processes[video_key]["is_long_term"]:
                        # Release lock and sleep to allow other tasks to run
                        await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
                        continue

                    async with self._mutex:
                        if video_key not in self._active_video_keys:
                            self._active_video_keys.append(video_key)
                    
                    if await self._set_result(video_key, source):
                        await self._set_deadline(source)
                    return

            try:
                new_active_count = await provider_slots.acquire_user_slot()
            except asyncio.TimeoutError as e:
                self.config.log_message(f"Failed to acquire user slot: {e}", level="WARN")
                await self.stream_manager.prune_ffmpeg_processes()
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
                continue

            log_status_string = f"{new_active_count}/{provider_slots.get_total_slots()}"
            created_video_key = await self._create_stream(video_key, provider_alias, source, log_status_string)
            if created_video_key:
                if not await self._set_result(created_video_key, source):
                    # Another stream was selected while this one was starting; clean up.
                    await self.stream_manager.stop_ffmpeg_process(created_video_key, self._video_names[created_video_key])
                else:
                    await self._set_deadline(source)
                return # Worker's job is done, either successfully or because another stream was chosen.

            # If stream creation failed, get the next source and loop again.
            source = await self._pop_source(provider_sources, source)
            if not source:
                return
            video_key = create_video_key(self.logical_channel_id, source["source_service_id"], self.video_type)

    async def _check_mpegts_ffmpeg_health_async(self, video_name: VideoName, stream_info: str, stdout_reader: asyncio.StreamReader, is_healthy: list[bool | None]) -> None:
        """Tries to read from stdout, blocks until data is available or the stream ends."""
        error: Exception | str = "no data received or timed out"
        try:
            # Read the first packet to confirm the stream is flowing.
            if await stdout_reader.read(MPEGTS_PACKET_SIZE):
                is_healthy[0] = True
                return
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError) as e:
            error = e
        self.config.log_message(f"{video_name} {stream_info}: Error reading from FFmpeg MPEG-TS stream: {error}", level="error")
        is_healthy[0] = False

    async def _create_stream(self, video_key: VideoKey, provider_alias: ProviderName, source: dict[str, Any], log_status_string: str) -> VideoKey | None:
        """Creates a stream using FFmpeg via an asynchronous subprocess."""
        video_name = create_video_name(self.logical_channel_name, source["source_service_id"], self.video_type)
        self._video_names[video_key] = video_name
        quality_score = self._quality_scores.get(source["source_service_id"])
        score_msg = f"Score={quality_score['total_score']:.2f} | Uptime={quality_score['uptime']*100:.0f}%" if quality_score else "Score=Unknown | Uptime=Unknown"
        stream_info = f"[Priority={source['priority']} | {score_msg}]"
        self._source_quality_messages[video_key] = stream_info

        channel_hls_dir = None
        stderr_log_file = None
        process = None
        try:
            # Feature Add: Conditional command creation based on video type.
            if self.video_type == VideoType.HLS:
                command, channel_hls_dir = await create_hls_ffmpeg_command(self.stream_manager, self.config, source["actual_stream_url"], video_key)
            elif self.video_type == VideoType.MPEGTS:
                command = create_mpegts_ffmpeg_command(self.config, source["actual_stream_url"])
            else:
                raise ValueError(f"Unsupported video type: {self.video_type}")

            log_path = self.config.get_ffmpeg_log_path(video_name, self.video_type)
            stderr_log_file = await aiofiles.open(log_path, 'a', encoding='utf-8')

            async with self._mutex:
                if self._selected:
                    raise asyncio.CancelledError("Stream selection already occurred.")
                stdout_pipe = asyncio.subprocess.PIPE if self.video_type == VideoType.MPEGTS else asyncio.subprocess.DEVNULL
                process = await asyncio.create_subprocess_exec(*command, stdout=stdout_pipe, stderr=stderr_log_file)
                self._active_video_keys.append(video_key)

            async with self.stream_manager.stream_process_lock:
                self.stream_manager.ffmpeg_processes[video_key] = {
                    'process': process, 'is_long_term': False, 'is_preview': self.logical_channel_id.startswith("preview_"),
                    'video_type': self.video_type, 'provider_alias': provider_alias, 'logical_channel_id': self.logical_channel_id,
                    'source_service_id': source["source_service_id"], 'logical_channel_name': self.logical_channel_name,
                    'channel_hls_dir': channel_hls_dir, 'last_access': datetime.now(), 'is_mpegts_active': False,
                    'stderr_log_file_obj': stderr_log_file
                }
            
            self.config.log_message(f"{video_name} {stream_info}: Claimed a '{provider_alias}' slot and started FFmpeg (PID: {process.pid}) {{{provider_alias}:{log_status_string}}}.", level="INFO")

            is_healthy: list[bool | None] = [None]
            health_check_task = None
            if self.video_type == VideoType.MPEGTS:
                health_check_task = asyncio.create_task(self._check_mpegts_ffmpeg_health_async(video_name, stream_info, process.stdout, is_healthy))

            loop = asyncio.get_running_loop()
            end_time = loop.time() + self.config.ffmpeg_start_timeout

            while loop.time() < end_time:
                if await self._get_selected():
                    raise asyncio.CancelledError("Stream selected elsewhere.")
                if process.returncode is not None:
                    raise ChildProcessError(f"exited prematurely with code {process.returncode}")
                
                if self.video_type == VideoType.HLS:
                    if any(f.endswith('.ts') for f in await aiofiles.os.listdir(channel_hls_dir)):
                        self.config.log_message(f"{video_name} {stream_info}: FFmpeg stream is now healthy (PID: {process.pid})", level="INFO")
                        return video_key
                elif self.video_type == VideoType.MPEGTS:
                    res = is_healthy[0]
                    if res is True:
                        self.config.log_message(f"{video_name} {stream_info}: FFmpeg stream is now healthy (PID: {process.pid})", level="INFO")
                        return video_key
                    elif res is False:
                        raise ChildProcessError("MPEG-TS health check failed")
                else:
                    raise ValueError(f"Unsupported video type: {self.video_type}")

                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
            
            raise TimeoutError("timed out waiting for segments or process stability")

        except (ChildProcessError, TimeoutError, asyncio.CancelledError) as e:
            self.config.log_message(f"{video_name} {stream_info}: FFmpeg validation failed (PID: {process.pid if process else 'N/A'}): {e}. Cleaning up.", level="ERROR")
            if health_check_task and not health_check_task.done():
                health_check_task.cancel()
            await self._remove_active_video_key(video_key)
            await self.stream_manager.stop_ffmpeg_process(video_key, video_name)
            if stderr_log_file: await stderr_log_file.close()
            if channel_hls_dir and await aiofiles.os.path.exists(channel_hls_dir):
                await aioshutil.rmtree(channel_hls_dir, ignore_errors=True)
            return None

    async def _process_results(self) -> None:
        """
        Supervisor task that actively monitors the deadline to select a stream as
        quickly as possible, and then robustly cleans up all related resources.
        """
        try:
            loop = asyncio.get_running_loop()
            while loop.time()  < await self._get_deadline():
                await asyncio.sleep(0.02)

            async with self._mutex:
                if self._selected:
                    return
                self._selected = True

                if not self._results:
                    self._res = (503, f"[{self.video_type}] {self.logical_channel_name}: Failed to start {self.video_type} stream from any source.")
                    return

                # 1. Select the winner.
                video_key, source = min(self._results, key=lambda x: self._remaining_priorities[x[1]["source_service_id"]])

                # 2. Make the result available to the client IMMEDIATELY.
                self._res = video_key
                self._result_event.set()

                # 3. Promote the winner to a long-term stream.
                await self.stream_manager.set_ffmpeg_process_long_term(video_key, True)

                # 4. Log the selection success.
                self.config.log_message(
                    f"{self._video_names[video_key]} {self._source_quality_messages[video_key]}: "
                    f"Selected as the best stream from {len(self._results)} tested and healthy sources "
                    f"(Total: {len(self._sources)} sources)",
                    level="INFO"
                )

                # 5. Identify losers and wait for their cleanup to complete.
                keys_to_stop = [k for k in self._active_video_keys if k != video_key]
                if keys_to_stop:
                    stop_tasks: list[Coroutine] = [
                        self.stream_manager.stop_ffmpeg_process(k, self._video_names.get(k, VideoName("Unknown")))
                        for k in keys_to_stop
                    ]
                    await asyncio.gather(*stop_tasks, return_exceptions=True)

        finally:
            # If the event wasn't set due to an early exit or error, ensure it's set now.
            if not self._result_event.is_set():
                self._result_event.set()