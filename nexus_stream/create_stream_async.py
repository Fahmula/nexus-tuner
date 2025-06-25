import asyncio
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, NewType, Coroutine

# Refactor Note: Replaced threading and concurrent.futures with asyncio for native async concurrency.
# Refactor Note: Replaced shutil with aioshutil for non-blocking file system operations.
# Refactor Note: Imported aiofiles for async file I/O, especially for FFmpeg logs.
import aioshutil
import aiofiles
import aiofiles.os

# Refactor Note: All dependent modules are now the async versions.
from config_async import Config
from quality_monitor_async import QualityMonitor
from slots_async import ProviderName, ProviderSlots
from stream_async import ChannelHandler, HLSStreamManager

HLSKey = NewType("HLSKey", str)
HLSName = NewType("HLSName", str)

CREATE_STREAM_POLL_INTERVAL = 0.01

# --- Helper functions (remain synchronous as they are pure) ---

def create_hls_key(logical_channel_id: str, source_service_id: str) -> HLSKey:
    """Generates a unique key for the HLS stream."""
    return HLSKey(f"{logical_channel_id}_{source_service_id}")

def create_hls_name(logical_channel_name: str, source_service_id: str) -> HLSName:
    """Generates a unique name for the HLS stream."""
    return HLSName(f"{logical_channel_name} - {source_service_id}")

# Refactor Note: This function is now async to perform non-blocking directory creation.
async def create_hls_ffmpeg_command(hls_manager: HLSStreamManager, config: Config, input_url: str, hls_key: HLSKey) -> tuple[list[str], Path]:
    """Constructs the FFmpeg command list and creates the necessary HLS directory asynchronously."""
    channel_hls_dir = hls_manager.hls_base_dir / config.get_fs_safe_alphanum(hls_key)
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


class CreateHLSStream:
    """
    An asyncio-native class to acquire and use resources for creating a stream.
    The original threaded logic is preserved using asyncio tasks and synchronization primitives.
    """
    def __init__(self, config: Config, handler: ChannelHandler, hls_manager: HLSStreamManager, quality_monitor: QualityMonitor, logical_channel_id: str, logical_channel_name: str, input_sources: list[dict[str, Any]] | None = None) -> None:
        # Refactor Note: The __init__ method is now lightweight and synchronous.
        # All async operations and task creation are moved to the `_initialize_and_start` method.
        self.config = config
        self.handler = handler
        self.hls_manager = hls_manager
        self.quality_monitor = quality_monitor
        self.logical_channel_id = logical_channel_id
        self.logical_channel_name = logical_channel_name
        
        self._res: HLSKey | tuple[int, str] = (500, "Stream not created yet")
        # Refactor Note: Replaced threading.Lock with asyncio.Lock for coroutine-safe state management.
        self._mutex = asyncio.Lock()
        # Refactor Note: Replaced a blocking mutex on result() with an asyncio.Event.
        # This allows `result()` to `await` completion without blocking the event loop.
        self._result_event = asyncio.Event()

        self._sources: list[dict[str, Any]] = []
        self._quality_scores: dict[str, dict[str, float]] = {}
        self._remaining_priorities: dict[str, int] = {}
        self._input_sources = input_sources

        self._results: list[tuple[HLSKey, dict[str, Any]]] = []
        self._selected = False
        self._active_hls_keys: list[HLSKey] = []
        self._source_quality_messages: dict[str, str] = {}
        self._hls_names: dict[HLSKey, HLSName] = {}
        self._deadline = time.monotonic() + 10
        # Refactor Note: Separate lists for supervisor and worker tasks.
        self._worker_tasks: list[asyncio.Task] = []
        self._supervisor_task: asyncio.Task | None = None

    @classmethod
    async def create(cls, *args, **kwargs) -> "CreateHLSStream":
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
        self._sources = self.handler.get_sources_for_client_facing_channel(self.logical_channel_id) if self._input_sources is None else deepcopy(self._input_sources)
        if not self._sources:
            self._res = (404, f"Logical channel '{self.logical_channel_name}' ({self.logical_channel_id}) not found or has no sources.")
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


    async def result(self) -> HLSKey | tuple[int, str]:
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
        async with self._mutex:
            if self._remaining_priorities[source["source_service_id"]] <= min(self._remaining_priorities.values()):
                new_deadline = time.monotonic()
            else:
                new_deadline = time.monotonic() + 1
            self._deadline = min(self._deadline, new_deadline)

    async def _set_result(self, hls_key: HLSKey, source: dict[str, Any]) -> bool:
        async with self._mutex:
            if self._selected:
                return False
            self._results.append((hls_key, source))
            return True

    async def _remove_active_hls_key(self, hls_key: HLSKey) -> None:
        async with self._mutex:
            if hls_key in self._active_hls_keys:
                self._active_hls_keys.remove(hls_key)

    async def _pop_source(self, provider_sources: list[dict[str, Any]], current_source: dict[str, Any] | None) -> dict[str, Any] | None:
        async with self._mutex:
            if current_source:
                self._remaining_priorities.pop(current_source["source_service_id"], None)
            if provider_sources:
                return provider_sources.pop()
            return None

    async def _create_provider_stream(self, provider_alias: ProviderName, provider_sources: list[dict[str, Any]]) -> None:
        """Creates HLS streams for a provider by launching concurrent worker tasks."""
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
        
        hls_key = create_hls_key(self.logical_channel_id, source["source_service_id"])
        
        while time.monotonic() < await self._get_deadline():
            if await self._get_selected():
                return

            # Refactor Note: The original logic of checking for an existing stream is preserved using an async lock.
            async with self.hls_manager.hls_process_lock:
                if hls_key in self.hls_manager.hls_ffmpeg_processes and self.hls_manager.hls_ffmpeg_processes[hls_key]["process"].returncode is None:
                    if not self.hls_manager.hls_ffmpeg_processes[hls_key]["is_long_term"]:
                        # Release lock and sleep to allow other tasks to run
                        await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
                        continue
                    
                    if await self._set_result(hls_key, source):
                        await self._set_deadline(source)
                    return

            # Refactor Note: Replaced blocking/non-blocking acquire with an async-native timed acquire.
            # This attempts to get a slot without blocking indefinitely.
            try:
                # Refactor Note: `acquire()` now returns the new active slot count.
                # This value is captured for accurate, non-racy logging.
                new_active_count = await asyncio.wait_for(provider_slots.acquire(), timeout=0.1)
            except asyncio.TimeoutError:
                await self.hls_manager.prune_hls_ffmpeg_processes()
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
                continue

            log_status_string = f"{new_active_count}/{provider_slots.get_total_slots()}"
            created_hls_key = await self._create_stream(hls_key, provider_alias, source, log_status_string)
            if created_hls_key:
                if not await self._set_result(created_hls_key, source):
                    # Another stream was selected while this one was starting; clean up.
                    await self.hls_manager.stop_hls_ffmpeg_process(created_hls_key, self._hls_names[created_hls_key])
                else:
                    await self._set_deadline(source)
                return # Worker's job is done, either successfully or because another stream was chosen.

            # If stream creation failed, get the next source and loop again.
            source = await self._pop_source(provider_sources, source)
            if not source:
                return
            hls_key = create_hls_key(self.logical_channel_id, source["source_service_id"])

    async def _create_stream(self, hls_key: HLSKey, provider_alias: ProviderName, source: dict[str, Any]) -> HLSKey | None:
        """Creates an HLS stream using FFmpeg via an asynchronous subprocess."""
        hls_name = create_hls_name(self.logical_channel_name, source["source_service_id"])
        self._hls_names[hls_key] = hls_name
        quality_score = self._quality_scores.get(source["source_service_id"])
        score_msg = f"Score={quality_score['total_score']:.2f} | Uptime={quality_score['uptime']*100:.0f}%" if quality_score else "Score=Unknown | Uptime=Unknown"
        full_msg = f"[Priority={source['priority']} | {score_msg}]"
        self._source_quality_messages[hls_key] = full_msg

        channel_hls_dir = None
        stderr_log_file = None
        try:
            command, channel_hls_dir = await create_hls_ffmpeg_command(self.hls_manager, self.config, source["actual_stream_url"], hls_key)
            log_path = self.config.get_ffmpeg_log_path(hls_name)
            stderr_log_file = await aiofiles.open(log_path, 'a', encoding='utf-8')

            async with self._mutex:
                if self._selected:
                    raise asyncio.CancelledError("Stream selection already occurred.")
                process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.DEVNULL, stderr=stderr_log_file)
                self._active_hls_keys.append(hls_key)

            async with self.hls_manager.hls_process_lock:
                self.hls_manager.hls_ffmpeg_processes[hls_key] = {
                    'process': process, 'is_long_term': False, 'is_preview': self.logical_channel_id.startswith("preview_"),
                    'provider_alias': provider_alias, 'logical_channel_id': self.logical_channel_id,
                    'source_service_id': source["source_service_id"], 'logical_channel_name': self.logical_channel_name,
                    'channel_hls_dir': channel_hls_dir, 'last_access': datetime.now(), 'stderr_log_file_obj': stderr_log_file
                }
            
            provider_streams = await self.handler.get_active_stream_status_for_logging(provider_alias)
            self.config.log_message(f"{hls_name} {full_msg}: Claimed a '{provider_alias}' slot and started FFmpeg (PID: {process.pid}) {{{provider_alias}:{provider_streams}}}.", level="INFO")

            end_time = time.monotonic() + self.config.ffmpeg_start_timeout
            while time.monotonic() < end_time:
                if await self._get_selected():
                    raise asyncio.CancelledError("Stream selected elsewhere.")
                if process.returncode is not None:
                    raise ChildProcessError(f"exited prematurely with code {process.returncode}")
                
                if any(f.endswith('.ts') for f in await aiofiles.os.listdir(channel_hls_dir)):
                    self.config.log_message(f"{hls_name} {full_msg}: FFmpeg stream is now healthy (PID: {process.pid})", level="INFO")
                    return hls_key
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
            
            raise TimeoutError("timed out waiting for segments")

        except (ChildProcessError, TimeoutError, asyncio.CancelledError) as e:
            self.config.log_message(f"{hls_name} {full_msg}: FFmpeg validation failed (PID: {process.pid if 'process' in locals() else 'N/A'}): {e}. Cleaning up.", level="ERROR")
            await self._remove_active_hls_key(hls_key)
            await self.hls_manager.stop_hls_ffmpeg_process(hls_key, hls_name)
            if stderr_log_file: await stderr_log_file.close()
            if channel_hls_dir: await aioshutil.rmtree(channel_hls_dir, ignore_errors=True)
            return None

    async def _process_results(self) -> None:
        """Supervisor task to wait until the deadline, select the best stream, and clean up."""
        try:
            deadline = await self._get_deadline()
            sleep_duration = max(0, deadline - time.monotonic())
            await asyncio.sleep(sleep_duration)

            async with self._mutex:
                if self._selected: return
                self._selected = True

                if not self._results:
                    self._res = (503, f"{self.logical_channel_name}: Failed to start HLS stream from any source.")
                    return

                hls_key, source = min(self._results, key=lambda x: self._remaining_priorities[x[1]["source_service_id"]])
                self._res = hls_key
                
                await self.hls_manager.set_ffmpeg_process_long_term(hls_key, True)
                
                keys_to_stop = [k for k in self._active_hls_keys if k != hls_key]
                if keys_to_stop:
                    stop_tasks: list[Coroutine] = [
                        self.hls_manager.stop_hls_ffmpeg_process(k, self._hls_names.get(k, HLSName("Unknown")))
                        for k in keys_to_stop
                    ]
                    asyncio.create_task(asyncio.gather(*stop_tasks))

                self.config.log_message(f"{self._hls_names[hls_key]} {self._source_quality_messages[hls_key]}: Selected as the best stream from {len(self._results)} tested and healthy sources (Total: {len(self._sources)} sources)", level="INFO")
        
        finally:
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            self._result_event.set()