import asyncio
import aiofiles.os
import aioshutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Coroutine, Final, Iterable, NoReturn, Self, cast

from nexus_tuner.config import Config
from nexus_tuner.handler import ChannelHandler
from nexus_tuner.quality_monitor import QualityMonitor
from nexus_tuner.utils import (CREATE_STREAM_DEADLINE, PROCESS_TERMINATE_TIMEOUT, NEW_DEADLINE_NON_BEST, ProcessInfo, ProcessInfoMutable, ProcessInfos, ProcessInfosMutable,
                                Label, Log, LogicalChannelId, PreviewId, ProviderAlias, SegmentNum, StreamEngine, VideoKey, VideoName, VideoType, get_playlist_path, get_segment_number, run_bg)


# --- Constants ---
CLEANUP_POLL_INTERVAL: Final[int] = 5


class StreamManager:
    """
    Manages all subprocesses for all video stream types using asyncio.

    This class is responsible for:
    - Starting and stopping processes for each requested stream.
    - Tracking the last access time for each stream to detect inactivity.
    - Running a background asyncio task to clean up inactive or dead processes.
    - Providing paths to HLS playlists and segments asynchronously.
    """
    __slots__ = (
        'config', 'handler', 'processes', 'quality_monitor',
        'hls_latest_segments', 'stream_process_lock', 'cleanup_task',
    )
    
    def __init__(self, config: Config, handler: ChannelHandler, quality_monitor: QualityMonitor) -> None:
        """
        Initializes the StreamManager.
        """
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self.quality_monitor: QualityMonitor = quality_monitor
        self.processes: ProcessInfos = ProcessInfos({})
        self.hls_latest_segments: dict[LogicalChannelId | PreviewId, dict[StreamEngine, tuple[SegmentNum, datetime]]] = {}
        self.stream_process_lock: asyncio.Lock = asyncio.Lock()
        self.cleanup_task: asyncio.Task[NoReturn]

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler, quality_monitor: QualityMonitor) -> Self:
        """Asynchronous factory for creating and initializing a StreamManager instance."""
        instance = cls(config, handler, quality_monitor)
        instance.cleanup_task = asyncio.create_task(instance._video_cleanup_loop())
        return instance

    async def get_process_info(self, video_key: VideoKey) -> ProcessInfo | None:
        """Returns the process info for a given Video key."""
        async with self.stream_process_lock:
            return self.processes.get(video_key)

    async def set_process_long_term(self, video_key: VideoKey, video_name: VideoName, long_term: bool) -> None:
        """Sets if an process is long term for a given video key."""
        async with self.stream_process_lock:
            if video_key in self.processes:
                Log.debug(Label.STREAM, f"{video_name}: Setting long term status to {long_term}.", (self.processes[video_key]['video_type'], self.processes[video_key]['stream_engine']))
                cast(ProcessInfoMutable, self.processes[video_key])['is_long_term'] = long_term
                cast(ProcessInfoMutable, self.processes[video_key])['last_access'] = datetime.now()  # Prevent immediate pruning
            else:
                Log.debug(Label.STREAM, f"{video_name}: Cannot set long term status to {long_term}: process not found.")

    async def get_processes_from_logical_id(self, logical_channel_id: LogicalChannelId | PreviewId, *, video_type: VideoType, stream_engine: StreamEngine, long_term_only: bool) -> ProcessInfosMutable:
        """
        Returns a dictionary of all processes associated with the given logical channel ID and video type.
        """
        async with self.stream_process_lock:
            if long_term_only:
                return ProcessInfosMutable({
                    video_key: data for video_key, data in self.processes.items()
                    if data['logical_channel_id'] == logical_channel_id and data['video_type'] == video_type and data['stream_engine'] == stream_engine and data['is_long_term']
                })
            return ProcessInfosMutable({
                video_key: data for video_key, data in self.processes.items()
                if data['logical_channel_id'] == logical_channel_id and data['video_type'] == video_type and data['stream_engine'] == stream_engine
            })

    async def record_video_access(self, logical_channel_id: LogicalChannelId | PreviewId, video_type: VideoType, stream_engine: StreamEngine, *, segment_filename: str | None = None) -> None:
        """Updates the last access time for the stream associated with the given logical channel ID and video type."""
        processes = await self.get_processes_from_logical_id(logical_channel_id, video_type=video_type, stream_engine=stream_engine, long_term_only=False)
        async with self.stream_process_lock:
            for data in processes.values():
                cast(ProcessInfoMutable, data)['last_access'] = datetime.now()
            if segment_filename:
                if logical_channel_id not in self.hls_latest_segments:
                    self.hls_latest_segments[logical_channel_id] = {}
                self.hls_latest_segments[logical_channel_id][stream_engine] = (get_segment_number(segment_filename), datetime.now())

    async def get_hls_playlist_path(self, video_key: VideoKey) -> Path | None:
        """Returns the path to the HLS playlist if the stream is active."""
        async with self.stream_process_lock:
            if video_key in self.processes:
                data = self.processes[video_key]
                if not data['is_long_term']:
                    Log.error(Label.STREAM, f"Stream {video_key} is not long-term, cannot return playlist path.", (VideoType.HLS, data['stream_engine']))
                    return
                if not data['channel_hls_dir']:
                    Log.error(Label.STREAM, f"Stream {video_key} has no HLS directory set.", (VideoType.HLS, data['stream_engine']))
                    return
                return get_playlist_path(data['channel_hls_dir'])
        
    async def get_hls_segment_path(self, logical_channel_id: LogicalChannelId, video_type: VideoType, stream_engine: StreamEngine, segment_filename: str) -> Path | None:
        """Returns the path to a specific HLS segment file if the stream is active."""
        processes = await self.get_processes_from_logical_id(logical_channel_id, video_type=video_type, stream_engine=stream_engine, long_term_only=True)
        if not processes:
            return
        video_key = next(iter(processes))
        async with self.stream_process_lock:
            if video_key in self.processes:
                channel_hls_dir = self.processes[video_key]['channel_hls_dir']
                if not channel_hls_dir:
                    Log.error(Label.STREAM, f"Stream {video_key} has no HLS directory set.")
                    return
                return channel_hls_dir / segment_filename

    async def get_hls_latest_segment(self, logical_channel_id: LogicalChannelId | PreviewId, stream_engine: StreamEngine) -> tuple[int, datetime] | None:
        """Returns the latest segment number and its timestamp for the given logical channel ID."""
        async with self.stream_process_lock:
            return self.hls_latest_segments.get(logical_channel_id, {}).get(stream_engine)

    async def _video_cleanup_loop(self) -> NoReturn:
        """Background task loop to find and stop inactive or dead streams."""
        while True:
            await asyncio.sleep(CLEANUP_POLL_INTERVAL)
            inactive_ids: set[VideoKey] = set()
            async with self.stream_process_lock:
                segment_lc_ids_to_cleanup: list[tuple[LogicalChannelId | PreviewId, StreamEngine, tuple[SegmentNum, datetime]]] = []
                for lc_id, datas in self.hls_latest_segments.items():
                    for stream_engine, data in datas.items():
                        if data[1] < datetime.now() - timedelta(seconds=self.config.latest_segment_timeout):
                            segment_lc_ids_to_cleanup.append((lc_id, stream_engine, data))
                for lc_id, stream_engine, data in segment_lc_ids_to_cleanup:
                    Log.debug(Label.STREAM, f"Cleaning up latest HLS segment number cache ({data[0]}) for logical channel ID '{lc_id}'.", (VideoType.HLS, stream_engine))
                    self.hls_latest_segments[lc_id].pop(stream_engine)
                    if not self.hls_latest_segments[lc_id]:
                        self.hls_latest_segments.pop(lc_id)

                providers_to_kill = self.handler.reset_kill_provider_streams()
                
                current_processes = list(self.processes.items())

                if providers_to_kill:
                    Log.warn(Label.STREAM, f"Killing streams from providers: {', '.join(providers_to_kill)}")
                    provider_keys_to_kill = [video_key for video_key, data in current_processes if data['provider_alias'] in providers_to_kill]
                    for video_key in provider_keys_to_kill:
                        data = cast(ProcessInfosMutable, self.processes).pop(video_key)
                        run_bg(self._stop_process(video_key, data_to_cleanup=data))  # Prevents process from being cancelled

                now = datetime.now()
                for video_key, data in current_processes:
                    timeout = self.config.segment_prune_timeout if data['is_preview'] else self.config.process_inactivity_timeout
                    if data['process'].returncode is not None:
                        Log.info(Label.STREAM, f"{data['video_name']}: Cleaning up dead process (PID: {data['process'].pid}).")
                        if not data['errored_at']:
                            cast(ProcessInfoMutable, data)['errored_at'] = datetime.now()
                            Log.debug(Label.STREAM, f"{data['video_name']}: Updated error timestamp.", (data['video_type'], data['stream_engine']))
                        inactive_ids.add(video_key)
                    elif data['is_long_term']:
                        if not data['is_mpegts_active'] and now - data['last_access'] > timedelta(seconds=timeout):
                            Log.info(Label.STREAM, f"{data['video_name']}: Timed out due to inactivity after {timeout}s (PID: {data['process'].pid}).")
                            inactive_ids.add(video_key)
                    else:
                        if now - data['last_access'] > timedelta(seconds=CREATE_STREAM_DEADLINE + NEW_DEADLINE_NON_BEST + 5):
                            Log.info(Label.STREAM, f"{data['video_name']}: Not long-term and hasn't been cleaned up (PID: {data['process'].pid}).")
                            inactive_ids.add(video_key)
            
            tasks = [self.stop_process(video_key) for video_key in inactive_ids]
            if tasks:
                await asyncio.gather(*tasks)

    async def prune_processes(self, alias: ProviderAlias) -> None:
        """Prunes processes to free up resources."""
        async with self.stream_process_lock:
            now = datetime.now()
            inactive_ids = [
                (video_key, data['video_name']) for video_key, data in self.processes.items()
                if alias == data['provider_alias'] and data['is_long_term'] and not data['is_mpegts_active'] and now - data['last_access'] > timedelta(seconds=self.config.segment_prune_timeout)
            ]
        
        tasks: list[Coroutine[Any, Any, None]] = []
        for video_key, video_name in inactive_ids:
            Log.info(Label.STREAM, f"Pruning inactive {alias} stream '{video_name}' [{video_key}].")
            tasks.append(self.stop_process(video_key))
        if tasks:
            await asyncio.gather(*tasks)

    async def stop_process(self, video_key: VideoKey) -> None:
        """Stops an process and cleans up resources."""
        task = run_bg(self._stop_process(video_key, data_to_cleanup=None))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.shield(task)
            raise

    async def stop_processes_with_logical_channel_id(self, logical_channel_id: LogicalChannelId, video_type: VideoType, stream_engine: StreamEngine) -> None:
        """Stops processes by logical channel ID and video type, and cleans up resources."""
        video_keys = (await self.get_processes_from_logical_id(logical_channel_id, video_type=video_type, stream_engine=stream_engine, long_term_only=False)).keys()
        await self.stop_processes(video_keys)

    async def _stop_process(self, video_key: VideoKey, *, data_to_cleanup: ProcessInfo | None) -> None:
        """
        MUST BE CALLED WITH run_bg() TO PREVENT CANCELLATION.
        Stops a single process and cleans its resources.
        If data_to_cleanup is provided, it will NOT release the slot or pop from self.processes.
        """
        if data_to_cleanup is None:
            async with self.stream_process_lock:
                if (data_to_cleanup := cast(ProcessInfosMutable, self.processes).pop(video_key, None)) is None:
                    return
            should_release_slot = True
            video_type = data_to_cleanup['video_type']
            video_name = data_to_cleanup['video_name']
            stream_engine = data_to_cleanup['stream_engine']
            Log.debug(Label.STREAM, f"{video_name}: Stopping process...", (video_type, stream_engine))
        else:
            should_release_slot = False
            video_type: VideoType = data_to_cleanup['video_type']
            video_name = data_to_cleanup['video_name']
            stream_engine = data_to_cleanup['stream_engine']
            Log.debug(Label.STREAM, f"{video_name}: Stopping process with provided data...", (video_type, stream_engine))

        process: asyncio.subprocess.Process = data_to_cleanup['process']
        alias = data_to_cleanup['provider_alias']
        hls_dir: Path | None = data_to_cleanup['channel_hls_dir']
        log_file: aiofiles.threadpool.text.AsyncTextIOWrapper = data_to_cleanup["stderr_log_file_obj"]

        if process.returncode is None:
            try:
                try:
                    process.terminate()
                    if process.stdout:
                        Log.debug(Label.STREAM, f"{video_name}: Closing process stdout stream.", (video_type, stream_engine))
                        process.stdout._transport.close()  # type: ignore[reportAttributeAccessIssue]
                    await asyncio.wait_for(process.wait(), timeout=PROCESS_TERMINATE_TIMEOUT)
                    Log.debug(Label.STREAM, f"{video_name}: Process terminated successfully.", (video_type, stream_engine))
                except asyncio.TimeoutError:
                    Log.warn(Label.STREAM, f"{video_name}: Killing unresponsive process.", (video_type, stream_engine))
                    process.kill()
                except Exception as e:
                    Log.error(Label.STREAM, f"{video_name}: Error terminating process: {e}", (video_type, stream_engine))
                    process.kill()
            except BaseException as e:
                Log.critical(Label.STREAM, f"{video_name}: Error while stopping process: {e}", (video_type, stream_engine))
                raise
        else:
            Log.debug(Label.STREAM, f"{video_name}: Process already terminated with code {process.returncode}.", (video_type, stream_engine))
        
        try:
            await log_file.close()
        except Exception as e:
            Log.error(Label.STREAM, f"{video_name}: Error closing log file: {e}", (video_type, stream_engine))

        provider_slots = await self.handler.get_provider_slots(alias)
        if provider_slots:
            if should_release_slot:
                Log.debug(Label.STREAM, f"{video_name}: Releasing slot for provider '{alias}'.", (video_type, stream_engine))
                new_active_count = await provider_slots.release()
            else:
                Log.debug(Label.STREAM, f"{video_name}: Not releasing slot for provider '{alias}' as it should have been already released.", (video_type, stream_engine))
                new_active_count = f"0/{provider_slots.get_total_slots()}"
        else:
            Log.error(Label.STREAM, f"{video_name}: No slots found for provider '{alias}'.", (video_type, stream_engine))
            new_active_count = "N/A"

        try:
            if hls_dir and await aiofiles.os.path.exists(hls_dir):
                await aioshutil.rmtree(hls_dir)
        except Exception as e:
            Log.error(Label.STREAM, f"{video_name}: Failed to clean HLS directory {hls_dir}: {e}", (video_type, stream_engine))

        Log.info(Label.STREAM, f"{video_name}: Successfully stopped and cleaned up all resources {{{alias}:{new_active_count}}}", (video_type, stream_engine))
        if data_to_cleanup['errored_at'] and data_to_cleanup['is_long_term'] and not data_to_cleanup['is_preview']:
            run_bg(self.quality_monitor.append_runtime(data_to_cleanup['source_id'], data_to_cleanup['started_at'], data_to_cleanup['errored_at']))

    async def stop_processes(self, video_keys: Iterable[VideoKey] | None = None) -> None:
        """Stops all (or a specified list of) active processes and cleans up resources."""
        tasks: list[Coroutine[Any, Any, None]]
        async with self.stream_process_lock:
            if video_keys is None:
                Log.info(Label.STREAM, "Stopping all active processes...")
                tasks = [self.stop_process(video_key) for video_key in self.processes]
            else:
                tasks = [self.stop_process(video_key) for video_key in self.processes if video_key in video_keys]
        if tasks:
            await asyncio.gather(*tasks)
