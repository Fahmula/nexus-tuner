import subprocess
import threading
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Iterable, NoReturn
from nexus_stream.config import Config, VideoKey, VideoType
from nexus_stream.handler import ChannelHandler
from nexus_stream.slots import ProviderName


# --- Constants ---
FFMPEG_TERMINATE_TIMEOUT = 5  # seconds
CLEANUP_POLL_INTERVAL = 5     # seconds


class StreamManager:
    """
    Manages all FFmpeg subprocesses.

    This class is responsible for:
    - Starting and stopping FFmpeg processes for each requested stream.
    - Tracking the last access time for each stream to detect inactivity.
    - Running a background thread to clean up inactive or dead FFmpeg processes.
    - Providing paths to HLS playlists and segments.

    The `ffmpeg_processes` dictionary has the following structure:
    {
        "video_key": {
            "process": subprocess.Popen,
            "is_long_term": bool,
            "is_preview": bool,
            "video_type": VideoType,
            "provider_alias": str,
            "logical_channel_id": str,
            "source_service_id": str,
            "logical_channel_name": str,
            "channel_hls_dir": Path | None,
            "last_access": datetime,
            "is_mpegts_active": bool,
            "stderr_log_file_obj": TextIOWrapper
        }
    }
    Where `video_key` is a unique identifier combining logical channel ID and source service ID.
    """
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        """
        Initializes the StreamManager.

        Args:
            config: The main application Config object.
            handler: The main ChannelHandler object, which holds the slots.
        """
        self.config = config
        self.handler = handler
        self.ffmpeg_processes: dict[VideoKey, dict[str, Any]] = {}
        self.stream_process_lock = threading.RLock()

        self.hls_base_dir: Path = self.config.hls_base_segment_dir
        self.config.log_message(f"HLS segments will be stored in: {self.hls_base_dir}", level="DEBUG")
        
        self.cleanup_thread = threading.Thread(target=self._video_cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def get_ffmpeg_process_info(self, video_key: VideoKey) -> dict[str, Any] | None:
        """Returns the FFmpeg process info for a given Video key."""
        with self.stream_process_lock:
            return self.ffmpeg_processes.get(video_key)

    def get_ffmpeg_processes_from_logical_id(self, logical_channel_id: str, *, video_type: VideoType, long_term_only: bool) -> dict[VideoKey, dict[str, Any]]:
        """
        Returns a dictionary of all FFmpeg processes associated with the given logical channel ID.
        This is useful for checking if a stream is already running.
        """
        with self.stream_process_lock:
            if long_term_only:
                return {
                    video_key: data for video_key, data in self.ffmpeg_processes.items()
                    if data['logical_channel_id'] == logical_channel_id and data['video_type'] == video_type and data['is_long_term']
                }
            return {
                video_key: data for video_key, data in self.ffmpeg_processes.items()
                if data['logical_channel_id'] == logical_channel_id and data['video_type'] == video_type
            }

    def set_ffmpeg_process_long_term(self, video_key: VideoKey, long_term: bool) -> None:
        """Sets if an FFmpeg process is long term for a given Video key."""
        with self.stream_process_lock:
            if video_key in self.ffmpeg_processes:
                self.ffmpeg_processes[video_key]['is_long_term'] = long_term

    def _record_video_access(self, logical_channel_id: str, video_type: VideoType) -> None:
        """Updates the last access time for the stream associated with the given logical channel ID."""
        with self.stream_process_lock:
            for video_key in self.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=video_type, long_term_only=False):
                self.ffmpeg_processes[video_key]['last_access'] = datetime.now()

    def record_video_access(self, logical_channel_id: str, video_type: VideoType) -> None:
        """ Records access to an stream by starting a background thread to update the last access time."""
        threading.Thread(target=self._record_video_access, args=(logical_channel_id, video_type), daemon=True).start()

    def get_hls_playlist_path(self, video_key: VideoKey) -> Path | None:
        """Returns the path to the HLS playlist if the stream is active."""
        with self.stream_process_lock:
            if video_key in self.ffmpeg_processes:
                data = self.ffmpeg_processes[video_key]
                if not data['is_long_term']:
                    self.config.log_message(f"Stream {video_key} is not long-term, cannot return playlist path.", level="ERROR")
                    return None
                if not data['channel_hls_dir']:
                    self.config.log_message(f"Stream {video_key} has no HLS directory set.", level="ERROR")
                    return None
                return data['channel_hls_dir'] / "playlist.m3u8"
        return None
        
    def get_hls_segment_path(self, logical_channel_id: str, video_type: VideoType, segment_filename: str) -> Path | None:
        """Returns the path to a specific HLS segment file if the stream is active."""
        with self.stream_process_lock:
            for video_key in self.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=video_type, long_term_only=True):
                channel_hls_dir = self.ffmpeg_processes[video_key]['channel_hls_dir']
                if not channel_hls_dir:
                    self.config.log_message(f"Stream {video_key} has no HLS directory set.", level="ERROR")
                    return None
                return channel_hls_dir / segment_filename
        return None

    def _video_cleanup_loop(self) -> NoReturn:
        """Background thread loop to find and stop inactive or dead streams."""
        self.config.log_message("Video cleanup thread started.", level="INFO")
        while True:
            time.sleep(CLEANUP_POLL_INTERVAL)
            inactive_ids: set[tuple[VideoKey, str]] = set()
            with self.stream_process_lock:
                providers_to_kill = self.handler.reset_kill_provider_streams()
                provider_keys_to_kill = [video_key for video_key, data in self.ffmpeg_processes.items() if data['provider_alias'] in providers_to_kill]
                if provider_keys_to_kill:
                    self.config.log_message(f"Cleanup: Killing streams from providers: {', '.join(providers_to_kill)}", level="WARN")
                    for video_key in provider_keys_to_kill:
                        data = self.ffmpeg_processes.pop(video_key)
                        self._stop_ffmpeg_process(video_key, data['logical_channel_name'], data_to_cleanup=data)
                now = datetime.now()
                for video_key, data in self.ffmpeg_processes.items():
                    timeout = self.config.segment_prune_timeout if data['is_preview'] else self.config.ffmpeg_inactivity_timeout
                    if data['process'].poll() is not None:
                        logical_channel_name = data['logical_channel_name']
                        self.config.log_message(f"Cleanup: Found dead process for '{logical_channel_name}' (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add((video_key, logical_channel_name))
                    elif not data['is_mpegts_active'] and now - data['last_access'] > timedelta(seconds=timeout):
                        logical_channel_name = data['logical_channel_name']
                        self.config.log_message(f"Cleanup: Stream '{logical_channel_name}' timed out due to inactivity (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add((video_key, logical_channel_name))
            
            for video_key_to_stop, logical_channel_name in inactive_ids:
                self.stop_ffmpeg_process(video_key_to_stop, logical_channel_name)

    def prune_ffmpeg_processes(self) -> None:
        """Prunes FFmpeg processes to free up resources."""
        with self.stream_process_lock:
            now = datetime.now()
            inactive_ids = [
                (video_key, data['logical_channel_name']) for video_key, data in self.ffmpeg_processes.items()
                if not data['is_mpegts_active'] and now - data['last_access'] > timedelta(seconds=self.config.segment_prune_timeout)
            ]
        for video_key, logical_channel_name in inactive_ids:
            self.config.log_message(f"Pruning inactive stream '{logical_channel_name}' [{video_key}].", level="INFO")
            self.stop_ffmpeg_process(video_key, logical_channel_name)

    def stop_ffmpeg_process(self, video_key: VideoKey, name: str) -> None:
        """Stops an FFmpeg process and cleans up resources."""
        return self._stop_ffmpeg_process(video_key, name, data_to_cleanup=None)

    def stop_ffmpeg_processes_with_logical_channel_id(self, logical_channel_id: str, video_type: VideoType) -> None:
        """Stops FFmpeg processes by logical channel ID and cleans up resources."""
        self.stop_ffmpeg_processes(self.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=video_type, long_term_only=False).keys())

    def _stop_ffmpeg_process(self, video_key: VideoKey, name: str, *, data_to_cleanup: dict[str, Any] | None) -> None:
        """If data_to_cleanup is provided, it will NOT release the slot or pop from ffmpeg_processes,
        the caller is responsible for those. If data_to_cleanup is None, it will find the process if it exists.
        """
        if data_to_cleanup is None:
            with self.stream_process_lock:
                if (data_to_cleanup := self.ffmpeg_processes.pop(video_key, None)) is None:
                    return
            should_release_slot = True
        else:
            should_release_slot = False

        process = data_to_cleanup['process']
        provider = ProviderName(data_to_cleanup['provider_alias'])
        hls_dir = data_to_cleanup['channel_hls_dir']
        log_file = data_to_cleanup.get('stderr_log_file_obj')

        if process.poll() is None:
            if process.stdout:
                process.stdout.close()
            process.terminate()
            try:
                process.wait(timeout=FFMPEG_TERMINATE_TIMEOUT)
            except subprocess.TimeoutExpired:
                self.config.log_message(f"{name} [{video_key}]: Killing unresponsive FFmpeg process.", level="WARN")
                process.kill()
        
        if log_file:
            try:
                log_file.close()
            except Exception as e:
                self.config.log_message(f"{name} [{video_key}]: Error closing FFmpeg log file: {e}", level="ERROR")

        if should_release_slot:
            self.handler.slots.get(provider).release()
        provider_streams = self.handler.get_active_stream_status_for_logging(provider)

        try:
            if hls_dir and hls_dir.exists():
                shutil.rmtree(hls_dir)
        except OSError as e:
            self.config.log_message(f"{name} [{video_key}]: Failed to clean HLS directory {hls_dir}: {e}", level="ERROR")
            
        self.config.cleanup_ffmpeg_logs_by_age()

        self.config.log_message(f"{name} [{video_key}]: Successfully stopped and cleaned up all resources {{{provider}:{provider_streams}}}", level="INFO")

    def stop_ffmpeg_processes(self, video_keys: Iterable[VideoKey] | None = None) -> None:
        """Stops all active FFmpeg processes and cleans up resources."""
        if video_keys is None:
            self.config.log_message("Stopping all active FFmpeg processes.", level="INFO")
            with self.stream_process_lock:
                processes_to_stop = self.ffmpeg_processes.copy()
        else:
            with self.stream_process_lock:
                processes_to_stop = {video_key: self.ffmpeg_processes[video_key] for video_key in video_keys if video_key in self.ffmpeg_processes}
        for lc_id, data in processes_to_stop.items():
            self.stop_ffmpeg_process(lc_id, data['logical_channel_name'])
