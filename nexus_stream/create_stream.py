import asyncio
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import IO, Any, Self, cast

import aiofiles.os
import aioshutil

from nexus_stream.config import Config
from nexus_stream.quality_monitor import QualityMonitor
from nexus_stream.slots import ProviderSlots
from nexus_stream.handler import ChannelHandler
from nexus_stream.stream import StreamManager
from nexus_stream.utils import (CREATE_STREAM_DEADLINE, CREATE_STREAM_POLL_INTERVAL, FFMPEG_TERMINATE_TIMEOUT,
                                MPEGTS_PACKET_SIZE, NEW_DEADLINE_NON_BEST, NEXUS_STREAM_USER_AGENT, FFmpegProcessInfosMutable, Label,
                                LogicalChannelId, LogicalChannelName, Priority, ProviderAlias, QualityScores, SourceInfo, SourceServiceId, VideoKey, VideoName,
                                VideoType, create_stream_key, create_video_key, create_video_name, get_segment_format, sort_sources)


async def create_hls_ffmpeg_command(stream_manager: StreamManager, config: Config, input_url: str, video_key: VideoKey, logical_channel_id: LogicalChannelId) -> tuple[list[str], Path]:
    """Constructs the FFmpeg command list and creates the necessary HLS directory asynchronously."""
    channel_hls_dir = config.hls_base_segment_dir / config.get_fs_safe_alphanum(f"{video_key}_{time.time()}")
    await aiofiles.os.makedirs(channel_hls_dir, exist_ok=True)
    
    playlist_path = channel_hls_dir / "playlist.m3u8"
    segment_filename = channel_hls_dir / get_segment_format()
    latest_segment = await stream_manager.get_hls_latest_segment(logical_channel_id)

    command = [
        config.ffmpeg_path,
        "-hide_banner", "-loglevel", "info",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4",
        "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
        "-user_agent", NEXUS_STREAM_USER_AGENT,
        "-i", input_url,
        "-codec", "copy",
        "-map", "0:v:0?", "-map", "0:a:0?",
        "-f", "hls",
        "-hls_time", str(config.hls_segment_duration),
        "-hls_list_size", str(config.hls_playlist_length),
        "-hls_flags", "delete_segments+omit_endlist+program_date_time",
        "-hls_segment_filename", str(segment_filename),
        "-start_number", str(latest_segment[0] if latest_segment else 0),
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
        "-user_agent", NEXUS_STREAM_USER_AGENT,
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
        'logical_channel_id', 'logical_channel_name', 'video_type',
        '_res', '_mutex', '_result_event',
        '_sources', '_quality_scores', '_remaining_priorities',
        '_input_sources', '_results', '_selected', '_active_video_keys',
        '_source_quality_messages', '_video_names', '_deadline',
        '_worker_tasks', '_supervisor_task',
    )
    
    def __init__(self, config: Config, handler: ChannelHandler, stream_manager: StreamManager, quality_monitor: QualityMonitor, logical_channel_id: LogicalChannelId, logical_channel_name: LogicalChannelName, video_type: VideoType, input_sources: list[SourceInfo] | None = None) -> None:
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self.stream_manager: StreamManager = stream_manager
        self.quality_monitor: QualityMonitor = quality_monitor
        self.logical_channel_id: LogicalChannelId = logical_channel_id
        self.logical_channel_name: LogicalChannelName = logical_channel_name
        self.video_type: VideoType = video_type
        
        self._res: VideoKey | tuple[int, str] = (500, f"[{video_type}] Stream not created yet")
        self._mutex: asyncio.Lock = asyncio.Lock()
        self._result_event: asyncio.Event = asyncio.Event()

        self._sources: list[SourceInfo] = []
        self._quality_scores: QualityScores = QualityScores({})
        self._remaining_priorities: dict[SourceServiceId, Priority] = {}
        self._input_sources: list[SourceInfo] | None = input_sources

        self._results: list[tuple[VideoKey, SourceInfo]] = []
        self._selected: bool = False
        self._active_video_keys: list[VideoKey] = []
        self._source_quality_messages: dict[VideoKey, str] = {}
        self._video_names: dict[VideoKey, VideoName] = {}
        self._deadline: float = 0
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._supervisor_task: asyncio.Task[None] | None = None

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler, stream_manager: StreamManager, quality_monitor: QualityMonitor, logical_channel_id: LogicalChannelId, logical_channel_name: LogicalChannelName, video_type: VideoType, input_sources: list[SourceInfo] | None = None) -> Self:
        """
        Asynchronously creates and initializes the stream creation process.
        This factory pattern is idiomatic for async classes that need to perform
        async operations during initialization.
        """
        instance = cls(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_name, video_type, input_sources)
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

        all_provider_sources: dict[ProviderAlias, list[SourceInfo]] = {}
        for source in self._sources:
            all_provider_sources.setdefault(ProviderAlias(source["provider_alias"]), []).append(source)

        self._supervisor_task = asyncio.create_task(self._process_results())

        for provider_alias, provider_sources in all_provider_sources.items():
            self._worker_tasks.append(asyncio.create_task(self._create_provider_stream(provider_alias, provider_sources)))

    async def result(self) -> VideoKey | tuple[int, str]:
        """Awaits the result of the stream creation process without blocking the event loop."""
        await self._result_event.wait()
        return self._res

    async def _get_deadline(self) -> float:
        async with self._mutex:
            return self._deadline

    async def _get_selected(self) -> bool:
        async with self._mutex:
            return self._selected

    async def _set_deadline(self, source: SourceInfo) -> None:
        loop = asyncio.get_running_loop()
        async with self._mutex:
            if self._remaining_priorities[source["source_service_id"]] <= min(self._remaining_priorities.values()):
                new_deadline = loop.time()
            else:
                new_deadline = loop.time() + NEW_DEADLINE_NON_BEST
            self._deadline = min(self._deadline, new_deadline)

    async def _set_result(self, video_key: VideoKey, source: SourceInfo) -> bool:
        async with self._mutex:
            if self._selected:
                return False
            self._results.append((video_key, source))
            return True

    async def _remove_active_video_key(self, video_key: VideoKey) -> None:
        async with self._mutex:
            if video_key in self._active_video_keys:
                self._active_video_keys.remove(video_key)

    async def _pop_source(self, provider_sources: list[SourceInfo], current_source: SourceInfo | None) -> SourceInfo | None:
        async with self._mutex:
            if current_source:
                self._remaining_priorities.pop(current_source["source_service_id"], None)
            if provider_sources:
                return provider_sources.pop()
            return

    async def _create_provider_stream(self, provider_alias: ProviderAlias, provider_sources: list[SourceInfo]) -> None:
        """Creates streams for a provider by launching concurrent worker tasks."""
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            self.config.critical(Label.HANDLER, f"{self.logical_channel_name}: Provider '{provider_alias}' does not exist.")
            return

        status = await self.handler.get_provider_stream_status()
        max_streams = status[provider_alias]["max"]
        
        worker_tasks = [
            asyncio.create_task(self._provider_worker_task(provider_alias, provider_slots, provider_sources))
            for _ in range(max_streams)
        ]
        await asyncio.gather(*worker_tasks, return_exceptions=False)

    async def _provider_worker_task(self, provider_alias: ProviderAlias, provider_slots: ProviderSlots, provider_sources: list[SourceInfo]) -> None:
        """Tries sources for a provider until a stream is created or sources are exhausted."""
        source = await self._pop_source(provider_sources, None)
        if not source:
            return
        
        video_key = create_video_key(create_stream_key(self.video_type, self.logical_channel_id), source["source_service_id"])
        video_name = create_video_name(self.logical_channel_name, source["source_service_id"])
        self._video_names[video_key] = video_name
        
        loop = asyncio.get_running_loop()
        while loop.time() < await self._get_deadline():
            if await self._get_selected():
                return

            to_sleep = False
            async with self.stream_manager.stream_process_lock:
                if video_key in self.stream_manager.ffmpeg_processes and self.stream_manager.ffmpeg_processes[video_key]["process"].returncode is None:
                    if self.stream_manager.ffmpeg_processes[video_key]["is_long_term"]:
                        if await self._set_result(video_key, source):
                            await self._set_deadline(source)
                        return
                    to_sleep = True
            if to_sleep:
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
                continue

            try:
                new_active_count = await provider_slots.acquire_user_slot()
            except asyncio.TimeoutError:
                self.config.warn(self.video_type, f"{video_name} failed to acquire user slot from '{provider_alias}'.")
                await self.stream_manager.prune_ffmpeg_processes()
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
                continue

            created_video_key = await self._create_stream(video_key, video_name, provider_alias, provider_slots, source, new_active_count)
            if created_video_key:
                if await self._set_result(created_video_key, source):
                    await self._set_deadline(source)
                else:
                    await self.stream_manager.stop_ffmpeg_process(created_video_key, self._video_names[created_video_key])
                return

            source = await self._pop_source(provider_sources, source)
            if not source:
                return
            video_key = create_video_key(create_stream_key(self.video_type, self.logical_channel_id), source["source_service_id"])
            video_name = create_video_name(self.logical_channel_name, source["source_service_id"])
            self._video_names[video_key] = video_name

    async def _check_mpegts_ffmpeg_health_async(self, video_name: VideoName, stream_info: str, stdout_reader: asyncio.StreamReader, is_healthy: list[bool | None]) -> None:
        """Tries to read from stdout, blocks until data is available or the stream ends."""
        error: Exception | str = "no data received or timed out"
        try:
            if await stdout_reader.readexactly(MPEGTS_PACKET_SIZE):
                is_healthy[0] = True
                return
        except Exception as e:
            error = e
        if not await self._get_selected():
            self.config.error(self.video_type, f"{video_name} {stream_info}: Error reading from FFmpeg MPEG-TS stream: {error}")
        is_healthy[0] = False

    async def _cleanup_pre_stream_failure(self, video_name: VideoName, stream_info: str, channel_hls_dir: Path | None, stderr_log_file: aiofiles.threadpool.text.AsyncTextIOWrapper | None) -> None:
        """Cleans up resources if stream creation fails before starting FFmpeg."""
        if stderr_log_file:
            try:
                await stderr_log_file.close()
            except Exception as close_error:
                self.config.critical(self.video_type, f"{video_name} {stream_info}: Failed to close log file: {close_error}")
        if channel_hls_dir:
            try:
                await aioshutil.rmtree(channel_hls_dir)
            except Exception as cleanup_error:
                self.config.critical(self.video_type, f"{video_name} {stream_info}: Failed to clean up HLS directory: {cleanup_error}")

    async def _create_stream(self, video_key: VideoKey, video_name: VideoName, provider_alias: ProviderAlias, provider_slots: ProviderSlots, source: SourceInfo, new_active_count: str) -> VideoKey | None:
        """Creates a stream using FFmpeg via an asynchronous subprocess."""
        quality_score = self._quality_scores.get(source["source_service_id"])
        score_msg = f"Score={quality_score['total_score']:.2f} | Uptime={quality_score['uptime']*100:.0f}%" if quality_score else "Score=Unknown | Uptime=Unknown"
        stream_info = f"[Priority={source['priority']} | {score_msg}]"
        self._source_quality_messages[video_key] = stream_info

        channel_hls_dir: Path | None = None
        stderr_log_file: aiofiles.threadpool.text.AsyncTextIOWrapper | None = None
        try:
            if self.video_type == VideoType.HLS:
                command, channel_hls_dir = await create_hls_ffmpeg_command(self.stream_manager, self.config, source["actual_stream_url"], video_key, self.logical_channel_id)
            elif self.video_type == VideoType.MPEGTS:
                command = create_mpegts_ffmpeg_command(self.config, source["actual_stream_url"])
            else:
                raise ValueError(f"Unsupported video type: {self.video_type}")
            log_path = self.config.get_ffmpeg_log_path(video_key)
            stderr_log_file = await aiofiles.open(log_path, 'a', encoding='utf-8')
        except BaseException as e:
            self.config.critical(self.video_type, f"{video_name} {stream_info}: Failed to create FFmpeg command: {e}")
            await provider_slots.release_user_slot()
            await self._cleanup_pre_stream_failure(video_name, stream_info, channel_hls_dir, stderr_log_file)
            if isinstance(e, Exception):
                return
            raise

        process: asyncio.subprocess.Process | None = None
        try:
            async with self._mutex:
                if self._selected:
                    raise asyncio.CancelledError("Stream selection already occurred.")
                if self.video_type == VideoType.MPEGTS:
                    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=cast(IO[Any], stderr_log_file))
                    async with self.stream_manager.stream_process_lock:
                        cast(FFmpegProcessInfosMutable, self.stream_manager.ffmpeg_processes)[video_key] = {
                            'process': process, 'is_long_term': False, 'is_preview': self.logical_channel_id.startswith("preview_"),
                            'video_type': VideoType.MPEGTS, 'provider_alias': provider_alias, 'logical_channel_id': self.logical_channel_id,
                            'source_service_id': source["source_service_id"], 'logical_channel_name': self.logical_channel_name,
                            'channel_hls_dir': None, 'last_access': datetime.now(), 'is_mpegts_active': False,
                            'stderr_log_file_obj': stderr_log_file
                        }
                else:
                    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.DEVNULL, stderr=cast(IO[Any], stderr_log_file))
                    async with self.stream_manager.stream_process_lock:
                        cast(FFmpegProcessInfosMutable, self.stream_manager.ffmpeg_processes)[video_key] = {
                            'process': process, 'is_long_term': False, 'is_preview': self.logical_channel_id.startswith("preview_"),
                            'video_type': VideoType.HLS, 'provider_alias': provider_alias, 'logical_channel_id': self.logical_channel_id,
                            'source_service_id': source["source_service_id"], 'logical_channel_name': self.logical_channel_name,
                            'channel_hls_dir': cast(Path, channel_hls_dir), 'last_access': datetime.now(), 'is_mpegts_active': None,
                            'stderr_log_file_obj': stderr_log_file
                        }
                self._active_video_keys.append(video_key)
        except BaseException as e:
            if not isinstance(e, asyncio.CancelledError):
                self.config.critical(self.video_type, f"{video_name} {stream_info}: Failed to start FFmpeg process: {e}")
            if process and process.returncode is None:
                try:
                    process.terminate()
                    if process.stdout:
                        process.stdout._transport.close()  # type: ignore[reportAttributeAccessIssue]
                    await asyncio.wait_for(process.wait(), timeout=FFMPEG_TERMINATE_TIMEOUT)
                except asyncio.TimeoutError:
                    self.config.warn(self.video_type, f"{video_name} {stream_info}: Killing unresponsive FFmpeg process.")
                    process.kill()
                except Exception as terminate_error:
                    self.config.error(self.video_type, f"{video_name} {stream_info}: Error terminating FFmpeg process: {terminate_error}")
                    process.kill()
            await provider_slots.release_user_slot()
            await self._cleanup_pre_stream_failure(video_name, stream_info, channel_hls_dir, stderr_log_file)
            if isinstance(e, Exception):
                return
            raise
        self.config.info(self.video_type, f"{video_name} {stream_info}: Claimed a '{provider_alias}' slot and started FFmpeg (PID: {process.pid}) {{{provider_alias}:{new_active_count}}}.")

        try:
            is_healthy: list[bool | None] = [None]
            if self.video_type == VideoType.MPEGTS:
                asyncio.create_task(self._check_mpegts_ffmpeg_health_async(video_name, stream_info, cast(asyncio.StreamReader, process.stdout), is_healthy))
            loop = asyncio.get_running_loop()
            end_time = loop.time() + self.config.ffmpeg_start_timeout
            while loop.time() < end_time:
                if await self._get_selected():
                    return
                if process.returncode is not None:
                    raise ChildProcessError(f"exited prematurely with code {process.returncode}")
                if self.video_type == VideoType.HLS:
                    if any(f.endswith('.ts') for f in await aiofiles.os.listdir(channel_hls_dir)):
                        self.config.info(self.video_type, f"{video_name} {stream_info}: FFmpeg stream is now healthy (PID: {process.pid})")
                        return video_key
                elif self.video_type == VideoType.MPEGTS:
                    res = is_healthy[0]
                    if res is True:
                        self.config.info(self.video_type, f"{video_name} {stream_info}: FFmpeg stream is now healthy (PID: {process.pid})")
                        return video_key
                    elif res is False:
                        raise ChildProcessError("MPEG-TS health check failed")
                else:
                    raise ValueError(f"Unsupported video type: {self.video_type}")
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
            raise TimeoutError("timed out waiting for segments or process stability")
        except BaseException as e:
            self.config.error(self.video_type, f"{video_name} {stream_info}: FFmpeg validation failed (PID: {process.pid if process else 'N/A'}): {e}. Cleaning up.")
            await self._remove_active_video_key(video_key)
            await self.stream_manager.stop_ffmpeg_process(video_key, video_name)
            if isinstance(e, Exception):
                return
            raise

    async def _process_results(self) -> None:
        """
        Supervisor task that actively monitors the deadline to select a stream as
        quickly as possible, and then robustly cleans up all related resources.
        """
        try:
            loop = asyncio.get_running_loop()
            while loop.time() < await self._get_deadline():
                if all(task.done() for task in self._worker_tasks):
                    break
                await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)

            async with self._mutex:
                if self._selected:
                    return
                self._selected = True
                if not self._results:
                    self._res = (503, f"[{self.video_type}] {self.logical_channel_name}: Failed to start {self.video_type} stream from any source.")
                    return
                video_key = min(self._results, key=lambda x: self._remaining_priorities[x[1]["source_service_id"]])[0]
                await self.stream_manager.set_ffmpeg_process_long_term(video_key, True)
                self._res = video_key
                self._result_event.set()
                self.config.info(self.video_type,
                    f"{self._video_names[video_key]} {self._source_quality_messages[video_key]}: "
                    f"Selected as the best stream from {len(self._results)} tested and healthy sources "
                    f"(Total: {len(self._sources)} sources)"
                )
                keys_to_stop = [k for k in self._active_video_keys if k != video_key]
                if keys_to_stop:
                    await self.stream_manager.stop_ffmpeg_processes(keys_to_stop)
        finally:
            if not self._result_event.is_set():
                self._result_event.set()