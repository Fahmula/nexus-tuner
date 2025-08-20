import asyncio
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import IO, Any, Self, cast

import aiofiles.os
import aioshutil

from nexus_tuner.config import Config
from nexus_tuner.quality_monitor import QualityMonitor
from nexus_tuner.slots import ProviderSlots
from nexus_tuner.handler import ChannelHandler
from nexus_tuner.stream import StreamManager
from nexus_tuner.utils import (CREATE_STREAM_DEADLINE, CREATE_STREAM_POLL_INTERVAL, FFMPEG_TERMINATE_TIMEOUT,
                                MPEGTS_PACKET_SIZE, NEW_DEADLINE_NON_BEST, NEXUS_TUNER_USER_AGENT, FFmpegProcessInfosMutable, Label, Log,
                                LogicalChannelId, LogicalChannelTitle, Priority, ProviderAlias, QualityScores, QualityScoresImpl, SourceInfo, SourceId, TVGDisplayTitle, TVGName, VideoKey, VideoName,
                                VideoType, create_stream_key, create_video_key, create_video_name, get_segment_format, run_bg, sort_sources)


async def create_hls_ffmpeg_command(stream_manager: StreamManager, config: Config, input_url: str, video_key: VideoKey, logical_channel_id: LogicalChannelId, video_name: VideoName, stream_info: str) -> tuple[list[str], Path]:
    """Constructs the FFmpeg command list and creates the necessary HLS directory asynchronously."""
    channel_hls_dir = config.hls_base_segment_dir / config.get_fs_safe_alphanum(f"{video_key}_{time.time()}")
    await aiofiles.os.makedirs(channel_hls_dir, exist_ok=True)
    
    playlist_path = channel_hls_dir / "playlist.m3u8"
    segment_filename = channel_hls_dir / get_segment_format()
    latest_segment = await stream_manager.get_hls_latest_segment(logical_channel_id)
    start_number = latest_segment[0] if latest_segment else 0
    Log.debug(Label.STREAM, f"{video_name} {stream_info}: Starting HLS stream at segment number {start_number} in directory '{channel_hls_dir}'", VideoType.HLS)

    command = [
        str(config.ffmpeg_path),
        "-hide_banner", "-loglevel", "info",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4",
        "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
        "-user_agent", NEXUS_TUNER_USER_AGENT,
        "-i", input_url,
        "-codec", "copy",
        "-map", "0:v:0?", "-map", "0:a:0?",
        "-f", "hls",
        "-hls_time", str(config.hls_segment_duration),
        "-hls_list_size", str(config.hls_playlist_length),
        "-hls_flags", "delete_segments+omit_endlist+program_date_time",
        "-hls_segment_filename", str(segment_filename),
        "-start_number", str(start_number),
        str(playlist_path)
    ]
    return command, channel_hls_dir


def create_mpegts_ffmpeg_command(config: Config, input_url: str) -> list[str]:
    """Constructs the FFmpeg command list to output a continuous MPEG-TS stream."""
    command = [
        str(config.ffmpeg_path),
        "-hide_banner", "-loglevel", "info",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4",
        "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
        "-user_agent", NEXUS_TUNER_USER_AGENT,
        "-i", input_url,
        "-c:v", "copy",
        "-c:a", "libmp3lame",
        "-map", "0:v:0?", "-map", "0:a:0?",
        "-f", "mpegts",
        "pipe:1"
    ]
    return command


class CreateStream:
    """
    An asyncio-native class to acquire and use resources for creating a stream.
    The original threaded logic is preserved using asyncio tasks and synchronization primitives.
    """
    __slots__ = (
        'config', 'handler', 'stream_manager', 'quality_monitor',
        'logical_channel_id', 'logical_channel_title', 'video_type',
        '_res', '_mutex', '_result_event', '_sources', '_source_names',
        '_quality_scores', '_remaining_priorities', '_input_sources',
        '_results', '_selected', '_slots_acquired', '_active_video_keys',
        '_source_quality_messages', '_video_names', '_deadline',
        '_worker_tasks', '_supervisor_task',
    )
    
    def __init__(self, config: Config, handler: ChannelHandler, stream_manager: StreamManager, quality_monitor: QualityMonitor, logical_channel_id: LogicalChannelId, logical_channel_title: LogicalChannelTitle, video_type: VideoType, input_sources: list[SourceInfo] | None = None) -> None:
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self.stream_manager: StreamManager = stream_manager
        self.quality_monitor: QualityMonitor = quality_monitor
        self.logical_channel_id: LogicalChannelId = logical_channel_id
        self.logical_channel_title: LogicalChannelTitle = logical_channel_title
        self.video_type: VideoType = video_type
        
        self._res: VideoKey | tuple[int, str] = (500, f"Stream not created yet")
        self._mutex: asyncio.Lock = asyncio.Lock()
        self._result_event: asyncio.Event = asyncio.Event()

        self._sources: list[SourceInfo] = []
        self._source_names: dict[SourceId, TVGDisplayTitle | TVGName] = {}
        self._quality_scores: QualityScores = QualityScoresImpl({})
        self._remaining_priorities: dict[SourceId, Priority] = {}
        self._input_sources: list[SourceInfo] | None = input_sources

        self._results: list[tuple[VideoKey, SourceInfo]] = []
        self._selected: bool = False
        self._slots_acquired: set[VideoKey] = set()
        self._active_video_keys: set[VideoKey] = set()
        self._source_quality_messages: dict[VideoKey, str] = {}
        self._video_names: dict[VideoKey, VideoName] = {}
        self._deadline: float = 0
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._supervisor_task: asyncio.Task[None] | None = None

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler, stream_manager: StreamManager, quality_monitor: QualityMonitor, logical_channel_id: LogicalChannelId, logical_channel_title: LogicalChannelTitle, video_type: VideoType, input_sources: list[SourceInfo] | None = None) -> Self:
        """
        Asynchronously creates and initializes the stream creation process.
        This factory pattern is idiomatic for async classes that need to perform
        async operations during initialization.
        """
        instance = cls(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_title, video_type, input_sources)
        await instance._initialize_and_start()
        return instance

    async def _initialize_and_start(self) -> None:
        """Performs async setup and launches all processing tasks."""
        loop = asyncio.get_running_loop()
        self._deadline = loop.time() + CREATE_STREAM_DEADLINE
        self._sources = self.handler.get_sources_for_client_channel(self.logical_channel_id) if self._input_sources is None else deepcopy(self._input_sources)
        for source in self._sources.copy():
            discovered_source = await self.handler.get_discovered_source(source["source_id"])
            if not discovered_source:
                Log.warn(Label.HANDLER, f"{self.logical_channel_title}: Source '{source['source_id']}' not found in discovered sources, skipping use for stream creation.")
                self._sources.remove(source)
                continue
            self._source_names[source["source_id"]] = discovered_source["display_title"] or discovered_source["tvg_name"]
        if not self._sources:
            self._res = (404, f"Logical channel '{self.logical_channel_title}' ({self.logical_channel_id}) not found or has no sources.")
            self._result_event.set()
            return

        self._quality_scores = await self.quality_monitor.get_quality_scores()
        self._remaining_priorities = sort_sources(self._sources, self._quality_scores, reverse=True)

        all_provider_sources: dict[ProviderAlias, list[SourceInfo]] = {}
        for source in self._sources:
            all_provider_sources.setdefault(source["provider_alias"], []).append(source)

        self._supervisor_task = asyncio.create_task(self._process_results())

        for provider_alias, provider_sources in all_provider_sources.items():
            self._worker_tasks.append(asyncio.create_task(self._create_provider_stream(provider_alias, provider_sources)))

    async def result(self) -> VideoKey | tuple[int, str]:
        """Awaits the result of the stream creation process without blocking the event loop."""
        await self._result_event.wait()
        return self._res

    def _add_result(self, video_key: VideoKey, source: SourceInfo, video_name: VideoName, stream_info: str) -> bool:
        """Adds a result to the internal results list and updates the deadline if necessary."""
        if self._selected:
            Log.debug(Label.STREAM, f"{video_name} {stream_info}: Another source has already been selected, not adding result.", self.video_type)
            return False
        self._results.append((video_key, source))
        loop = asyncio.get_running_loop()
        if self._remaining_priorities[source["source_id"]] <= min(self._remaining_priorities.values()):
            new_deadline = loop.time()
            if new_deadline <= self._deadline:
                Log.debug(Label.STREAM, f"{video_name} {stream_info}: Best remaining source is healthy, setting immediate deadline.", self.video_type)
                self._deadline = new_deadline
            else:
                Log.debug(Label.STREAM, f"{video_name} {stream_info}: Best remaining source is healthy, but current deadline is sooner.", self.video_type)
        else:
            new_deadline = loop.time() + NEW_DEADLINE_NON_BEST
            if new_deadline <= self._deadline:
                Log.debug(Label.STREAM, f"{video_name} {stream_info}: Non-best remaining source is healthy, setting new deadline to {NEW_DEADLINE_NON_BEST}s from now.", self.video_type)
                self._deadline = new_deadline
            else:
                Log.debug(Label.STREAM, f"{video_name} {stream_info}: Non-best remaining source is healthy, but current deadline is sooner than {NEW_DEADLINE_NON_BEST}s from now.", self.video_type)
        return True

    def _release_slot(self, provider_slots: ProviderSlots, video_key: VideoKey, video_name: VideoName, stream_info: str) -> None:
        """Releases a slot for a specific video key if not already released."""
        # This cannot be an async method as cancelled asyncio.CancelledError prevents cleanup
        if video_key in self._slots_acquired:
            Log.debug(Label.STREAM, f"{video_name} {stream_info}: Releasing slot for provider '{provider_slots.get_alias()}'", self.video_type)
            self._slots_acquired.remove(video_key)
            run_bg(provider_slots.release())

    def _remove_active_video_key(self, video_key: VideoKey, video_name: VideoName, stream_info: str) -> None:
        """Removes a video key from the active video keys set."""
        Log.debug(Label.STREAM, f"{video_name} {stream_info}: Removing active video key '{video_key}'", self.video_type)
        self._active_video_keys.remove(video_key)

    def _pop_source(self, provider_sources: list[SourceInfo], prev_source: SourceInfo | None) -> tuple[SourceInfo, VideoKey, VideoName, str] | None:
        if prev_source:
            Log.debug(Label.STREAM, f"Removing source '{prev_source['source_id']}' from remaining priorities.", self.video_type)
            self._remaining_priorities.pop(prev_source["source_id"], None)
        if not provider_sources:
            return
        new_source = provider_sources.pop()
        video_key = create_video_key(create_stream_key(self.video_type, self.logical_channel_id), new_source["source_id"])
        video_name = create_video_name(self.logical_channel_title, self._source_names[new_source["source_id"]], new_source["source_id"])
        self._video_names[video_key] = video_name
        quality_score = self._quality_scores.get(new_source["source_id"])
        score_msg = f"Score={quality_score['total_score']:.2f} | Uptime={quality_score['uptime']*100:.0f}%" if quality_score else "Score=Unknown | Uptime=Unknown"
        stream_info = f"[Priority={new_source['priority']} | {score_msg}]"
        Log.debug(Label.STREAM, f"{video_name} {stream_info}: Using source for stream creation...", self.video_type)
        return new_source, video_key, video_name, stream_info

    async def _create_provider_stream(self, provider_alias: ProviderAlias, provider_sources: list[SourceInfo]) -> None:
        """Creates streams for a provider by launching concurrent worker tasks."""
        provider_slots = await self.handler.get_provider_slots(provider_alias)
        if not provider_slots:
            Log.error(Label.HANDLER, f"{self.logical_channel_title}: Provider '{provider_alias}' does not exist.")
            return
        if provider_slots.get_total_slots() <= 0:
            Log.warn(Label.HANDLER, f"{self.logical_channel_title}: Provider '{provider_alias}' is configured with 0 slots, skipping stream creation.")
            return

        if len(provider_sources) > await provider_slots.get_available_slots():
            provider_slots.cancel_background_tasks()
            run_bg(self.stream_manager.prune_ffmpeg_processes(provider_alias))

        status = await self.handler.get_provider_stream_status()
        max_streams = status[provider_alias]["max_streams"]
        
        worker_tasks = [
            asyncio.create_task(self._provider_worker_task(provider_alias, provider_sources))
            for _ in range(max_streams)
        ]
        await asyncio.gather(*worker_tasks, return_exceptions=False)

    async def _provider_worker_task(self, provider_alias: ProviderAlias, provider_sources: list[SourceInfo]) -> None:
        """Tries sources for a provider until a stream is created or sources are exhausted."""
        source_res = self._pop_source(provider_sources, None)
        if not source_res:
            return
        source, video_key, video_name, stream_info = source_res
        
        loop = asyncio.get_running_loop()
        logged_existing = False
        logged_failure = False
        while loop.time() < self._deadline:
            if self._selected:
                Log.debug(Label.STREAM, f"{video_name} {stream_info}: Another source has already been selected, stopping worker task.", self.video_type)
                return

            to_sleep = False
            async with self.stream_manager.stream_process_lock:
                if video_key in self.stream_manager.ffmpeg_processes and self.stream_manager.ffmpeg_processes[video_key]["process"].returncode is None:
                    if self.stream_manager.ffmpeg_processes[video_key]["is_long_term"]:
                        Log.debug(Label.STREAM, f"{video_name} {stream_info}: Using existing long term stream...", self.video_type)
                        self._add_result(video_key, source, video_name, stream_info)
                        return
                    elif not logged_existing:
                        logged_existing = True
                        Log.debug(Label.STREAM, f"{video_name} {stream_info}: Existing non-long term stream is found, waiting...", self.video_type)
                    to_sleep = True
            if to_sleep:
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
                continue
            logged_existing = False

            provider_slots = await self.handler.get_provider_slots(provider_alias)
            if not provider_slots:
                Log.error(Label.HANDLER, f"{self.logical_channel_title}: Provider '{provider_alias}' not found in slots manager.")
                return
            if provider_slots.get_total_slots() <= 0:
                Log.warn(Label.HANDLER, f"{self.logical_channel_title}: Provider '{provider_alias}' is configured with 0 slots, skipping stream creation.")
                return

            new_active_count = await provider_slots.try_acquire()
            if new_active_count is False:
                if not logged_failure:
                    logged_failure = True
                    Log.info(Label.STREAM, f"{video_name} failed to acquire user slot from '{provider_alias}', retrying...", self.video_type)
                provider_slots.cancel_background_tasks()
                run_bg(self.stream_manager.prune_ffmpeg_processes(provider_alias))
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
                continue
            self._slots_acquired.add(video_key)
            logged_failure = False
            try:
                if await self._create_stream(video_key, video_name, provider_alias, provider_slots, source, stream_info, new_active_count):
                    if not self._add_result(video_key, source, video_name, stream_info):
                        await self.stream_manager.stop_ffmpeg_process(video_key, video_name)
                    return
            finally:
                self._release_slot(provider_slots, video_key, video_name, stream_info)

            source_res = self._pop_source(provider_sources, source)
            if not source_res:
                return
            source, video_key, video_name, stream_info = source_res

    async def _check_mpegts_ffmpeg_health_async(self, video_name: VideoName, stream_info: str, stdout_reader: asyncio.StreamReader, is_healthy: list[bool | None]) -> None:
        """Tries to read from stdout, blocks until data is available or the stream ends."""
        error: Exception | str = "no data received or timed out"
        try:
            if await stdout_reader.readexactly(MPEGTS_PACKET_SIZE):
                is_healthy[0] = True
                return
        except Exception as e:
            error = e
        if not self._selected:
            Log.error(Label.STREAM, f"{video_name} {stream_info}: Error reading from FFmpeg MPEG-TS stream: {error}", self.video_type)
        is_healthy[0] = False

    async def _cleanup_pre_stream_failure(self, video_name: VideoName, stream_info: str, channel_hls_dir: Path | None, stderr_log_file: aiofiles.threadpool.text.AsyncTextIOWrapper | None) -> None:
        """Cleans up resources if stream creation fails before starting FFmpeg."""
        if stderr_log_file:
            try:
                await stderr_log_file.close()
            except Exception as close_error:
                Log.critical(Label.STREAM, f"{video_name} {stream_info}: Failed to close log file: {close_error}", self.video_type)
        if channel_hls_dir:
            try:
                await aioshutil.rmtree(channel_hls_dir)
            except Exception as cleanup_error:
                Log.critical(Label.STREAM, f"{video_name} {stream_info}: Failed to clean up HLS directory: {cleanup_error}", self.video_type)

    async def _create_stream(self, video_key: VideoKey, video_name: VideoName, provider_alias: ProviderAlias, provider_slots: ProviderSlots, source: SourceInfo, stream_info: str, new_active_count: str) -> bool:
        """Creates a stream using FFmpeg via an asynchronous subprocess."""
        self._source_quality_messages[video_key] = stream_info

        channel_hls_dir: Path | None = None
        stderr_log_file: aiofiles.threadpool.text.AsyncTextIOWrapper | None = None
        try:
            if self.video_type == VideoType.HLS:
                command, channel_hls_dir = await create_hls_ffmpeg_command(self.stream_manager, self.config, source["stream_url"], video_key, self.logical_channel_id, video_name, stream_info)
            elif self.video_type == VideoType.MPEGTS:
                command = create_mpegts_ffmpeg_command(self.config, source["stream_url"])
            else:
                raise ValueError(f"Unsupported video type: {self.video_type}")
            log_path = self.config.get_ffmpeg_log_path(video_key)
            stderr_log_file = await aiofiles.open(log_path, 'a', encoding='utf-8')
        except BaseException as e:
            self._release_slot(provider_slots, video_key, video_name, stream_info)
            Log.critical(Label.STREAM, f"{video_name} {stream_info}: Failed to create FFmpeg command: {e}", self.video_type)
            await self._cleanup_pre_stream_failure(video_name, stream_info, channel_hls_dir, stderr_log_file)
            if isinstance(e, Exception):
                return False
            raise

        process: asyncio.subprocess.Process | None = None
        is_preview = self.logical_channel_id.startswith("preview_")
        try:
            async with self._mutex:
                if self._selected:
                    raise asyncio.CancelledError("Stream selection already occurred.")
                if self.video_type == VideoType.MPEGTS:
                    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=cast(IO[Any], stderr_log_file))
                    async with self.stream_manager.stream_process_lock:
                        cast(FFmpegProcessInfosMutable, self.stream_manager.ffmpeg_processes)[video_key] = {
                            'process': process, 'is_long_term': False, 'is_preview': is_preview,
                            'video_type': VideoType.MPEGTS, 'provider_alias': provider_alias, 'logical_channel_id': self.logical_channel_id,
                            'source_id': source["source_id"], 'logical_channel_title': self.logical_channel_title,
                            'channel_hls_dir': None, 'last_access': datetime.now(), 'is_mpegts_active': False,
                            'stderr_log_file_obj': stderr_log_file
                        }
                else:
                    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.DEVNULL, stderr=cast(IO[Any], stderr_log_file))
                    async with self.stream_manager.stream_process_lock:
                        cast(FFmpegProcessInfosMutable, self.stream_manager.ffmpeg_processes)[video_key] = {
                            'process': process, 'is_long_term': False, 'is_preview': is_preview,
                            'video_type': VideoType.HLS, 'provider_alias': provider_alias, 'logical_channel_id': self.logical_channel_id,
                            'source_id': source["source_id"], 'logical_channel_title': self.logical_channel_title,
                            'channel_hls_dir': cast(Path, channel_hls_dir), 'last_access': datetime.now(), 'is_mpegts_active': None,
                            'stderr_log_file_obj': stderr_log_file
                        }
                self._slots_acquired.remove(video_key)  # Slot is now owned by the FFmpeg process
                self._active_video_keys.add(video_key)
        except BaseException as e:
            try:
                if not isinstance(e, asyncio.CancelledError):
                    Log.critical(Label.STREAM, f"{video_name} {stream_info}: Failed to start FFmpeg process: {e}", self.video_type)
            except BaseException:
                pass
            try:
                if process and process.returncode is None:
                    try:
                        process.terminate()
                        if process.stdout:
                            Log.debug(Label.STREAM, f"{video_name} [{video_key}]: Closing FFmpeg process stdout stream.", self.video_type)
                            process.stdout._transport.close()  # type: ignore[reportAttributeAccessIssue]
                        await asyncio.wait_for(process.wait(), timeout=FFMPEG_TERMINATE_TIMEOUT)
                        Log.debug(Label.STREAM, f"{video_name} [{video_key}]: FFmpeg process terminated successfully.", self.video_type)
                    except asyncio.TimeoutError:
                        Log.warn(Label.STREAM, f"{video_name} {stream_info}: Killing unresponsive FFmpeg process.", self.video_type)
                        process.kill()
                    except BaseException as terminate_error:  # Catch all exceptions to ensure cleanup, we will re-raise later
                        Log.error(Label.STREAM, f"{video_name} {stream_info}: Error terminating FFmpeg process: {terminate_error}", self.video_type)
                        process.kill()
            finally:
                self._release_slot(provider_slots, video_key, video_name, stream_info)
            await self._cleanup_pre_stream_failure(video_name, stream_info, channel_hls_dir, stderr_log_file)
            if isinstance(e, Exception):
                return False
            raise
        Log.info(Label.STREAM, f"{video_name} {stream_info}: Claimed a '{provider_alias}' slot and started FFmpeg (PID: {process.pid}) {{{provider_alias}:{new_active_count}}}.", self.video_type)

        try:
            is_healthy: list[bool | None] = [None]
            if self.video_type == VideoType.MPEGTS:
                run_bg(self._check_mpegts_ffmpeg_health_async(video_name, stream_info, cast(asyncio.StreamReader, process.stdout), is_healthy))
            loop = asyncio.get_running_loop()
            end_time = loop.time() + (CREATE_STREAM_DEADLINE if is_preview else self.config.ffmpeg_start_timeout)
            while loop.time() < end_time:
                if self._selected:
                    Log.debug(Label.STREAM, f"{video_name} {stream_info}: Another source has already been selected, stopping health check.", self.video_type)
                    return False
                if process.returncode is not None:
                    raise ChildProcessError(f"exited prematurely with code {process.returncode}")
                if self.video_type == VideoType.HLS:
                    if any(f.endswith('.ts') for f in await aiofiles.os.listdir(channel_hls_dir)):
                        Log.info(Label.STREAM, f"{video_name} {stream_info}: FFmpeg stream is now healthy (PID: {process.pid})", self.video_type)
                        return True
                elif self.video_type == VideoType.MPEGTS:
                    res = is_healthy[0]
                    if res is True:
                        Log.info(Label.STREAM, f"{video_name} {stream_info}: FFmpeg stream is now healthy (PID: {process.pid})", self.video_type)
                        return True
                    elif res is False:
                        raise ChildProcessError("MPEG-TS health check failed")
                else:
                    raise ValueError(f"Unsupported video type: {self.video_type}")
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
            raise TimeoutError("timed out waiting for segments or process stability")
        except BaseException as e:
            Log.error(Label.STREAM, f"{video_name} {stream_info}: FFmpeg validation failed (PID: {process.pid if process else 'N/A'}): {e}. Cleaning up.", self.video_type)
            self._remove_active_video_key(video_key, video_name, stream_info)
            await self.stream_manager.stop_ffmpeg_process(video_key, video_name)
            if isinstance(e, Exception):
                return False
            raise

    async def _process_results(self) -> None:
        """
        Supervisor task that actively monitors the deadline to select a stream as
        quickly as possible, and then robustly cleans up all related resources.
        """
        try:
            loop = asyncio.get_running_loop()
            while loop.time() < self._deadline:
                if all(task.done() for task in self._worker_tasks):
                    break
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)

            async with self._mutex:
                self._selected = True
                if not self._results:
                    self._res = (503, f"{self.logical_channel_title}: Failed to start {self.video_type} stream from any source.")
                    return
                video_key = min(self._results, key=lambda x: self._remaining_priorities[x[1]["source_id"]])[0]
                await self.stream_manager.set_ffmpeg_process_long_term(video_key, True)
                self._res = video_key
                self._result_event.set()
                Log.info(Label.STREAM,
                    f"{self._video_names[video_key]} {self._source_quality_messages[video_key]}: "
                    f"Selected as the best stream from {len(self._results)} tested and healthy sources "
                    f"(Total: {len(self._sources)} sources)",
                    self.video_type
                )
                keys_to_stop = [k for k in self._active_video_keys if k != video_key]
                await self.stream_manager.stop_ffmpeg_processes(keys_to_stop, self._video_names)
        finally:
            self._result_event.set()
