import asyncio
import aiofiles
import aioshutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Iterable, NoReturn

from nexus_stream.config import CREATE_STREAM_DEADLINE, NEW_DEADLINE_NON_BEST, Config, VideoKey, VideoType
from nexus_stream.handler import ChannelHandler
from nexus_stream.slots import ProviderName

from asyncio.subprocess import Process
from _io import TextIOWrapper


# --- Constants ---
FFMPEG_TERMINATE_TIMEOUT = 5  # seconds
CLEANUP_POLL_INTERVAL = 5     # seconds


class StreamManager:
    """
    Manages all FFmpeg subprocesses for all video stream types using asyncio.

    This class is responsible for:
    - Starting and stopping FFmpeg processes for each requested stream.
    - Tracking the last access time for each stream to detect inactivity.
    - Running a background asyncio task to clean up inactive or dead FFmpeg processes.
    - Providing paths to HLS playlists and segments asynchronously.
    """
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        """
        Initializes the StreamManager.
        """
        self.config = config
        self.handler = handler
        self.ffmpeg_processes: dict[VideoKey, dict[str, Any]] = {}
        self.hls_latest_segments: dict[str, tuple[int, datetime]] = {}
        self.stream_process_lock = asyncio.Lock()

        self.hls_base_dir: Path = self.config.hls_base_segment_dir
        self.config.log_message(f"HLS segments will be stored in: {self.hls_base_dir}", level="DEBUG")
        
        self.cleanup_task: asyncio.Task | None = None

    def start_cleanup_task(self) -> None:
        """Creates and starts the background cleanup task."""
        if self.cleanup_task is None or self.cleanup_task.done():
            self.cleanup_task = asyncio.create_task(self._video_cleanup_loop())
            self.config.log_message("Video FFmpeg cleanup task started.", level="INFO")

    async def get_ffmpeg_process_info(self, video_key: VideoKey) -> dict[str, Any] | None:
        """Returns the FFmpeg process info for a given Video key."""
        async with self.stream_process_lock:
            return self.ffmpeg_processes.get(video_key)

    async def set_ffmpeg_process_long_term(self, video_key: VideoKey, long_term: bool) -> None:
        """Sets if an FFmpeg process is long term for a given Video key asynchronously."""
        async with self.stream_process_lock:
            if video_key in self.ffmpeg_processes:
                self.ffmpeg_processes[video_key]['is_long_term'] = long_term

    async def get_ffmpeg_processes_from_logical_id(self, logical_channel_id: str, *, video_type: VideoType, long_term_only: bool) -> dict[VideoKey, dict[str, Any]]:
        """
        Returns a dictionary of all FFmpeg processes associated with the given logical channel ID and video type asynchronously.
        """
        async with self.stream_process_lock:
            if long_term_only:
                return {
                    video_key: data for video_key, data in self.ffmpeg_processes.items()
                    if data['logical_channel_id'] == logical_channel_id and data['video_type'] == video_type and data['is_long_term']
                }
            return {
                video_key: data for video_key, data in self.ffmpeg_processes.items()
                if data['logical_channel_id'] == logical_channel_id and data['video_type'] == video_type
            }

    async def record_video_access(self, logical_channel_id: str, video_type: VideoType, *, segment_filename: str | None = None) -> None:
        """Updates the last access time for the stream associated with the given logical channel ID and video type."""
        processes = await self.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=video_type, long_term_only=False)
        async with self.stream_process_lock:
            for data in processes.values():
                data['last_access'] = datetime.now()
            if segment_filename:
                self.hls_latest_segments[logical_channel_id] = (self.config.get_segment_number(segment_filename), datetime.now())

    async def get_hls_playlist_path(self, video_key: VideoKey) -> Path | None:
        """Returns the path to the HLS playlist if the stream is active asynchronously."""
        async with self.stream_process_lock:
            if video_key in self.ffmpeg_processes:
                data = self.ffmpeg_processes[video_key]
                if not data['is_long_term']:
                    self.config.log_message(f"Stream {video_key} is not long-term, cannot return playlist path.", level="ERROR")
                    return None
                if not data['channel_hls_dir']:
                    self.config.log_message(f"Stream {video_key} has no HLS directory set.", level="ERROR")
                    self.config.log_message(f"Stream {video_key} is not long-term, cannot return playlist path.", level="ERROR")
                    return None
                if not data['channel_hls_dir']:
                    self.config.log_message(f"Stream {video_key} has no HLS directory set.", level="ERROR")
                    return None
                return data['channel_hls_dir'] / "playlist.m3u8"
        return None
        
    async def get_hls_segment_path(self, logical_channel_id: str, video_type: VideoType, segment_filename: str) -> Path | None:
        """Returns the path to a specific HLS segment file if the stream is active asynchronously."""
        processes = await self.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=video_type, long_term_only=True)
        if not processes:
            return None
        video_key = next(iter(processes))
        async with self.stream_process_lock:
            if video_key in self.ffmpeg_processes:
                channel_hls_dir = self.ffmpeg_processes[video_key]['channel_hls_dir']
                if not channel_hls_dir:
                    self.config.log_message(f"Stream {video_key} has no HLS directory set.", level="ERROR")
                    return None
                return channel_hls_dir / segment_filename
        return None

    async def get_hls_latest_segment(self, logical_channel_id: str) -> tuple[int, datetime] | None:
        """Returns the latest segment number and its timestamp for the given logical channel ID asynchronously."""
        async with self.stream_process_lock:
            return self.hls_latest_segments.get(logical_channel_id)

    async def _video_cleanup_loop(self) -> NoReturn:
        """Background task loop to find and stop inactive or dead streams."""
        while True:
            await asyncio.sleep(CLEANUP_POLL_INTERVAL)
            inactive_ids: set[tuple[VideoKey, str]] = set()
            async with self.stream_process_lock:
                segment_lc_ids_to_cleanup = [(lc_id, data) for lc_id, data in self.hls_latest_segments.items() if data[1] < datetime.now() - timedelta(seconds=self.config.latest_segment_timeout)]
                for lc_id, data in segment_lc_ids_to_cleanup:
                    self.config.log_message(f"Cleanup: Removing latest HLS segment number cache ({data[0]}) for logical channel ID '{lc_id}'.", level="DEBUG")
                    self.hls_latest_segments.pop(lc_id, None)    
                
                providers_to_kill = await self.handler.reset_kill_provider_streams()
                
                current_processes = list(self.ffmpeg_processes.items())

                if providers_to_kill:
                    self.config.log_message(f"Cleanup: Killing streams from providers: {', '.join(providers_to_kill)}", level="WARN")
                    provider_keys_to_kill = [video_key for video_key, data in current_processes if data['provider_alias'] in providers_to_kill]
                    for video_key in provider_keys_to_kill:
                        data = self.ffmpeg_processes.pop(video_key)
                        await self._stop_ffmpeg_process(video_key, data['logical_channel_name'], data_to_cleanup=data)

                now = datetime.now()
                for video_key, data in current_processes:
                    if video_key not in self.ffmpeg_processes:
                        continue

                    timeout = self.config.segment_prune_timeout if data['is_preview'] else self.config.ffmpeg_inactivity_timeout
                    
                    if data['process'].returncode is not None:
                        logical_channel_name = data['logical_channel_name']
                        self.config.log_message(f"Cleanup: Found dead process for '{logical_channel_name}' (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add((video_key, logical_channel_name))
                    elif data['is_long_term']:
                        if not data['is_mpegts_active'] and now - data['last_access'] > timedelta(seconds=timeout):
                            logical_channel_name = data['logical_channel_name']
                            self.config.log_message(f"Cleanup: Stream '{logical_channel_name}' timed out due to inactivity after {timeout}s (PID: {data['process'].pid}).", level="INFO")
                            inactive_ids.add((video_key, logical_channel_name))
                    else:
                        if now - data['last_access'] > timedelta(seconds=CREATE_STREAM_DEADLINE + NEW_DEADLINE_NON_BEST + 5):
                            logical_channel_name = data['logical_channel_name']
                            self.config.log_message(f"Cleanup: Stream '{logical_channel_name}' is not long-term and hasn't been cleaned up (PID: {data['process'].pid}).", level="INFO")
                            inactive_ids.add((video_key, logical_channel_name))
            
            tasks = [self.stop_ffmpeg_process(video_key, name) for video_key, name in inactive_ids]
            if tasks:
                await asyncio.gather(*tasks)

    async def prune_ffmpeg_processes(self) -> None:
        """Prunes FFmpeg processes to free up resources asynchronously."""
        async with self.stream_process_lock:
            now = datetime.now()
            inactive_ids = [
                (video_key, data['logical_channel_name']) for video_key, data in self.ffmpeg_processes.items()
                if data['is_long_term'] and not data['is_mpegts_active'] and now - data['last_access'] > timedelta(seconds=self.config.segment_prune_timeout)
            ]
        
        tasks = []
        for video_key, logical_channel_name in inactive_ids:
            self.config.log_message(f"Pruning inactive stream '{logical_channel_name}' [{video_key}].", level="INFO")
            tasks.append(self.stop_ffmpeg_process(video_key, logical_channel_name))
        if tasks:
            await asyncio.gather(*tasks)

    async def stop_ffmpeg_process(self, video_key: VideoKey, name: str) -> None:
        """Stops an FFmpeg process and cleans up resources asynchronously."""
        await self._stop_ffmpeg_process(video_key, name, data_to_cleanup=None)

    async def stop_ffmpeg_processes_with_logical_channel_id(self, logical_channel_id: str, video_type: VideoType) -> None:
        """Stops FFmpeg processes by logical channel ID and video type, and cleans up resources asynchronously."""
        video_keys = (await self.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=video_type, long_term_only=False)).keys()
        await self.stop_ffmpeg_processes(video_keys)

    async def _stop_ffmpeg_process(self, video_key: VideoKey, name: str, *, data_to_cleanup: dict[str, Any] | None) -> None:
        """
        Stops a single FFmpeg process and cleans its resources asynchronously.
        If data_to_cleanup is provided, it will NOT release the slot or pop from ffmpeg_processes.
        """
        if data_to_cleanup is None:
            async with self.stream_process_lock:
                if (data_to_cleanup := self.ffmpeg_processes.pop(video_key, None)) is None:
                    return
            should_release_slot = True
        else:
            should_release_slot = False

        process: Process = data_to_cleanup['process']
        provider = ProviderName(data_to_cleanup['provider_alias'])
        hls_dir: Path | None = data_to_cleanup['channel_hls_dir']
        log_file: TextIOWrapper | None = data_to_cleanup.get('stderr_log_file_obj')

        if process.returncode is None:
            try:
                process.terminate()
                if process.stdout:
                    process.stdout._transport.close()
                await asyncio.wait_for(process.wait(), timeout=FFMPEG_TERMINATE_TIMEOUT)
            except asyncio.TimeoutError:
                self.config.log_message(f"{name} [{video_key}]: Killing unresponsive FFmpeg process.", level="WARN")
                process.kill()
            except Exception as e:
                self.config.log_message(f"{name} [{video_key}]: Error terminating FFmpeg process: {e}", level="ERROR")
                process.kill()
        
        if log_file:
            try:
                await log_file.close()
            except Exception as e:
                self.config.log_message(f"{name} [{video_key}]: Error closing FFmpeg log file: {e}", level="ERROR")

        if should_release_slot:
            new_active_count = await self.handler.slots.get(provider).release_user_slot()
        
        try:
            if hls_dir and await aiofiles.os.path.exists(hls_dir):
                await aioshutil.rmtree(hls_dir)
        except OSError as e:
            self.config.log_message(f"{name} [{video_key}]: Failed to clean HLS directory {hls_dir}: {e}", level="ERROR")
            
        await self.config.cleanup_ffmpeg_logs_by_age()

        self.config.log_message(f"{name} [{video_key}]: Successfully stopped and cleaned up all resources {{{provider}:{new_active_count}}}", level="INFO")

    async def stop_ffmpeg_processes(self, video_keys: Iterable[VideoKey] | None = None) -> None:
        """Stops all (or a specified list of) active FFmpeg processes and cleans up resources asynchronously."""
        processes_to_stop: dict[VideoKey, dict[str, Any]] = {}
        async with self.stream_process_lock:
            if video_keys is None:
                self.config.log_message("Stopping all active FFmpeg processes.", level="INFO")
                processes_to_stop = self.ffmpeg_processes.copy()
            else:
                processes_to_stop = {video_key: self.ffmpeg_processes[video_key] for video_key in video_keys if video_key in self.ffmpeg_processes}
                
        tasks = [
            self.stop_ffmpeg_process(lc_id, data['logical_channel_name'])
            for lc_id, data in processes_to_stop.items()
        ]
        if tasks:
            await asyncio.gather(*tasks)