import asyncio
# Refactor Note: Replaced shutil with aioshutil for non-blocking file system operations.
import aiofiles
import aioshutil
from pathlib import Path
from datetime import datetime, timedelta
# Refactor Note: TYPE_CHECKING imports are updated to point to the async versions of the classes.
from typing import TYPE_CHECKING, Any, Iterable, NoReturn

from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.slots import ProviderName

if TYPE_CHECKING:
    # This type hint is for the unique key used to identify a stream.
    from nexus_stream.create_stream import HLSKey
    # Refactor Note: The process object is now expected to be an asyncio.subprocess.Process.
    # This provides awaitable methods like .wait(), which is essential for non-blocking code.
    from asyncio.subprocess import Process
    from _io import TextIOWrapper


# --- Constants ---
FFMPEG_TERMINATE_TIMEOUT = 5  # seconds
CLEANUP_POLL_INTERVAL = 5     # seconds


class StreamManager:
    """
    Manages all FFmpeg subprocesses for HLS transcoding using asyncio.

    This class is responsible for:
    - Starting and stopping FFmpeg processes for each requested stream.
    - Tracking the last access time for each stream to detect inactivity.
    - Running a background asyncio task to clean up inactive or dead FFmpeg processes.
    - Providing paths to HLS playlists and segments asynchronously.
    """
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        """
        Initializes the HLSStreamManager.
        """
        self.config = config
        self.handler = handler
        self.hls_ffmpeg_processes: dict['HLSKey', dict[str, Any]] = {}
        # Refactor Note: Replaced threading.RLock with asyncio.Lock for coroutine-safe access.
        self.hls_process_lock = asyncio.Lock()

        self.hls_base_dir: Path = self.config.hls_base_segment_dir
        self.config.log_message(f"HLS segments will be stored in: {self.hls_base_dir}", level="DEBUG")
        
        # Refactor Note: The cleanup thread is replaced by an asyncio.Task.
        # The task is not started in __init__ as it's an async operation.
        # A dedicated `start_cleanup_task` method should be called after instantiation.
        self.cleanup_task: asyncio.Task | None = None

    def start_cleanup_task(self) -> None:
        """Creates and starts the background cleanup task."""
        if self.cleanup_task is None or self.cleanup_task.done():
            # Refactor Note: The background thread is replaced with an asyncio.Task,
            # which is the idiomatic way to run background operations in asyncio.
            self.cleanup_task = asyncio.create_task(self._hls_cleanup_loop())
            self.config.log_message("HLS FFmpeg cleanup task started.", level="INFO")

    # Refactor Note: This method is now async to use the async lock.
    async def set_ffmpeg_process_long_term(self, hls_key: 'HLSKey', long_term: bool) -> None:
        """Sets if an FFmpeg process is long term for a given HLS key asynchronously."""
        async with self.hls_process_lock:
            if hls_key in self.hls_ffmpeg_processes:
                self.hls_ffmpeg_processes[hls_key]['is_long_term'] = long_term

    # Refactor Note: This method is now async to use the async lock.
    async def get_ffmpeg_processes_from_logical_id(self, logical_channel_id: str, *, long_term_only: bool) -> dict['HLSKey', dict[str, Any]]:
        """
        Returns a dictionary of all FFmpeg processes associated with the given logical channel ID asynchronously.
        """
        async with self.hls_process_lock:
            if long_term_only:
                return {
                    video_key: data for video_key, data in self.ffmpeg_processes.items()
                    if data['logical_channel_id'] == logical_channel_id and data['video_type'] == video_type and data['is_long_term']
                }
            return {
                video_key: data for video_key, data in self.ffmpeg_processes.items()
                if data['logical_channel_id'] == logical_channel_id and data['video_type'] == video_type
            }

    # Refactor Note: This helper method is now async as it calls another async method.
    async def _record_hls_access(self, logical_channel_id: str) -> None:
        """Updates the last access time for the HLS stream associated with the given logical channel ID."""
        # This method now returns a copy, so we don't need to hold the lock while iterating.
        processes = await self.get_ffmpeg_processes_from_logical_id(logical_channel_id, long_term_only=False)
        async with self.hls_process_lock:
            for hls_key in processes:
                if hls_key in self.hls_ffmpeg_processes: # Check again in case it was removed
                    self.hls_ffmpeg_processes[hls_key]['last_access'] = datetime.now()

    # Refactor Note: Replaced the thread-spawning implementation with a simple async call.
    # The caller can decide whether to `await` it or run it in the background with `create_task`.
    async def record_hls_access(self, logical_channel_id: str) -> None:
        """Records access to an HLS stream by updating the last access time asynchronously."""
        await self._record_hls_access(logical_channel_id)

    # Refactor Note: This method is now async to use the async lock.
    async def get_hls_playlist_path(self, hls_key: 'HLSKey') -> Path | None:
        """Returns the path to the HLS playlist if the stream is active asynchronously."""
        async with self.hls_process_lock:
            if hls_key in self.hls_ffmpeg_processes:
                data = self.hls_ffmpeg_processes[hls_key]
                if not data['is_long_term']:
                    self.config.log_message(f"Stream {video_key} is not long-term, cannot return playlist path.", level="ERROR")
                    return None
                if not data['channel_hls_dir']:
                    self.config.log_message(f"Stream {video_key} has no HLS directory set.", level="ERROR")
                    return None
                return data['channel_hls_dir'] / "playlist.m3u8"
        return None
        
    # Refactor Note: This method is now async to use the async lock.
    async def get_hls_segment_path(self, logical_channel_id: str, segment_filename: str) -> Path | None:
        """Returns the path to a specific HLS segment file if the stream is active asynchronously."""
        # This method now returns a copy, so we don't need to hold the lock while iterating.
        processes = await self.get_ffmpeg_processes_from_logical_id(logical_channel_id, long_term_only=True)
        for hls_key in processes:
            # We only need the first one for a given logical channel ID
            async with self.hls_process_lock:
                if hls_key in self.hls_ffmpeg_processes:
                    return self.hls_ffmpeg_processes[hls_key]['channel_hls_dir'] / segment_filename
        return None

    # Refactor Note: The cleanup loop is now an async method, intended to be run as a background task.
    async def _hls_cleanup_loop(self) -> NoReturn:
        """Background task loop to find and stop inactive or dead HLS streams."""
        while True:
            # Refactor Note: Replaced time.sleep() with await asyncio.sleep() for non-blocking delay.
            await asyncio.sleep(CLEANUP_POLL_INTERVAL)
            inactive_ids: set[tuple['HLSKey', str]] = set()
            async with self.hls_process_lock:
                # Refactor Note: Awaiting the async method from the refactored ChannelHandler.
                providers_to_kill = await self.handler.reset_kill_provider_streams()
                
                # Create a copy of items to avoid mutation issues during iteration
                current_processes = list(self.hls_ffmpeg_processes.items())

                if providers_to_kill:
                    self.config.log_message(f"Cleanup: Killing HLS streams for providers: {', '.join(providers_to_kill)}", level="WARN")
                    provider_keys_to_kill = [hls_key for hls_key, data in current_processes if data['provider_alias'] in providers_to_kill]
                    for hls_key in provider_keys_to_kill:
                        data = self.hls_ffmpeg_processes.pop(hls_key)
                        # Refactor Note: Awaiting the now-async stop method.
                        await self._stop_hls_ffmpeg_process(hls_key, data['logical_channel_name'], data_to_cleanup=data)

                now = datetime.now()
                # Iterate over the copied list
                for hls_key, data in current_processes:
                    # Skip if already removed by provider kill logic
                    if hls_key not in self.hls_ffmpeg_processes:
                        continue

                    timeout = self.config.hls_segment_prune_timeout if data['is_preview'] else self.config.ffmpeg_hls_inactivity_timeout
                    
                    # process.returncode is synchronous and non-blocking, so it's safe to call.
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
            
            # Stop processes outside the main lock to avoid long-running operations inside the lock.
            tasks = [self.stop_hls_ffmpeg_process(hls_key, name) for hls_key, name in inactive_ids]
            # Refactor Note: Use asyncio.gather to run cleanup tasks concurrently.
            if tasks:
                await asyncio.gather(*tasks)

    # Refactor Note: This method is now async to use the async lock and call async stop methods.
    async def prune_hls_ffmpeg_processes(self) -> None:
        """Prunes HLS FFmpeg processes to free up resources asynchronously."""
        async with self.hls_process_lock:
            now = datetime.now()
            inactive_ids = [
                (video_key, data['logical_channel_name']) for video_key, data in self.ffmpeg_processes.items()
                if not data['is_mpegts_active'] and now - data['last_access'] > timedelta(seconds=self.config.segment_prune_timeout) and data['is_long_term']
            ]
        
        tasks = []
        for hls_key, logical_channel_name in inactive_ids:
            self.config.log_message(f"Pruning inactive HLS stream '{logical_channel_name}' [{hls_key}].", level="INFO")
            tasks.append(self.stop_hls_ffmpeg_process(hls_key, logical_channel_name))
        if tasks:
            await asyncio.gather(*tasks)

    # Refactor Note: This method is now async as it calls the async helper.
    async def stop_hls_ffmpeg_process(self, hls_key: 'HLSKey', name: str) -> None:
        """Stops an HLS FFmpeg process and cleans up resources asynchronously."""
        await self._stop_hls_ffmpeg_process(hls_key, name, data_to_cleanup=None)

    # Refactor Note: This method is now async to use the async lock and call async stop methods.
    async def stop_hls_ffmpeg_processes_with_logical_channel_id(self, logical_channel_id: str) -> None:
        """Stops HLS FFmpeg processes by logical channel ID and cleans up resources asynchronously."""
        async with self.hls_process_lock:
            hls_keys = [hls_key for hls_key, data in self.hls_ffmpeg_processes.items() if data['logical_channel_id'] == logical_channel_id]
        await self.stop_hls_ffmpeg_processes(hls_keys)

    # Refactor Note: This core method is now fully async.
    async def _stop_hls_ffmpeg_process(self, hls_key: 'HLSKey', name: str, *, data_to_cleanup: dict[str, Any] | None) -> None:
        """
        Stops a single FFmpeg process and cleans its resources asynchronously.
        If data_to_cleanup is provided, it will NOT release the slot or pop from hls_ffmpeg_processes.
        """
        if data_to_cleanup is None:
            async with self.hls_process_lock:
                if (data_to_cleanup := self.hls_ffmpeg_processes.pop(hls_key, None)) is None:
                    return
            should_release_slot = True
        else:
            should_release_slot = False

        process: 'Process' = data_to_cleanup['process']
        provider = ProviderName(data_to_cleanup['provider_alias'])
        hls_dir: Path = data_to_cleanup['channel_hls_dir']
        log_file = data_to_cleanup.get('stderr_log_file_obj')

        if process.returncode is None:
            process.terminate()
            try:
                # Refactor Note: Replaced blocking process.wait() with an awaitable version.
                # This requires the process to be an asyncio.subprocess.Process instance.
                await asyncio.wait_for(process.wait(), timeout=FFMPEG_TERMINATE_TIMEOUT)
            except asyncio.TimeoutError:
                self.config.log_message(f"{name}: Killing unresponsive FFmpeg process.", level="WARN")
                process.kill()
        
        if log_file:
            try:
                # File I/O is blocking, but closing is generally fast. For extreme performance,
                # this could be wrapped in asyncio.to_thread.
                await log_file.close()
            except Exception as e:
                self.config.log_message(f"{name} [{video_key}]: Error closing FFmpeg log file: {e}", level="ERROR")

        if should_release_slot:
            await self.handler.slots.get(provider).release_user_slot()
        
        # Refactor Note: Awaiting the async method from the refactored ChannelHandler.
        provider_streams = await self.handler.get_active_stream_status_for_logging(provider)

        try:
            # Refactor Note: Replaced shutil.rmtree with aioshutil.rmtree for non-blocking I/O.
            if await aiofiles.os.path.exists(hls_dir):
                await aioshutil.rmtree(hls_dir)
        except OSError as e:
            self.config.log_message(f"{name} [{video_key}]: Failed to clean HLS directory {hls_dir}: {e}", level="ERROR")
            
        # Refactor Note: Awaiting the async method from the refactored Config.
        await self.config.cleanup_ffmpeg_logs_by_age()

        self.config.log_message(f"{name} [{video_key}]: Successfully stopped and cleaned up all resources {{{provider}:{provider_streams}}}", level="INFO")

    # Refactor Note: This method is now async and uses asyncio.gather for concurrent cleanup.
    async def stop_hls_ffmpeg_processes(self, hls_keys: Iterable['HLSKey'] | None = None) -> None:
        """Stops all (or a specified list of) active HLS FFmpeg processes and cleans up resources asynchronously."""
        processes_to_stop: dict['HLSKey', dict[str, Any]] = {}
        async with self.hls_process_lock:
            if hls_keys is None:
                self.config.log_message("Stopping all active HLS FFmpeg processes.", level="INFO")
                # Pop all items to prevent the cleanup loop from trying to stop them again.
                processes_to_stop = self.hls_ffmpeg_processes
                self.hls_ffmpeg_processes = {}
            else:
                processes_to_stop = {hls_key: self.hls_ffmpeg_processes[hls_key] for hls_key in hls_keys if hls_key in self.hls_ffmpeg_processes}
                
        # Refactor Note: Use asyncio.gather to stop all processes concurrently, which is much
        # faster than stopping them one by one in a loop.
        tasks = [
            self.stop_hls_ffmpeg_process(lc_id, data['logical_channel_name'])
            for lc_id, data in processes_to_stop.items()
        ]
        if tasks:
            await asyncio.gather(*tasks)