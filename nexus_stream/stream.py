import subprocess
import threading
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Iterable
from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.slots import ProviderName

if TYPE_CHECKING:
    from nexus_stream.create_stream import HLSKey


# --- Constants ---
FFMPEG_TERMINATE_TIMEOUT = 5  # seconds
CLEANUP_POLL_INTERVAL = 5     # seconds


class HLSStreamManager:
    """
    Manages all FFmpeg subprocesses for HLS transcoding.

    This class is responsible for:
    - Starting and stopping FFmpeg processes for each requested stream.
    - Tracking the last access time for each stream to detect inactivity.
    - Running a background thread to clean up inactive or dead FFmpeg processes.
    - Providing paths to HLS playlists and segments.

    The `hls_ffmpeg_processes` dictionary has the following structure:
    {
        "hls_key": {
            "process": subprocess.Popen,
            "is_long_term": bool,
            "provider_alias": str,
            "logical_channel_id": str,
            "source_service_id": str,
            "logical_channel_name": str,
            "channel_hls_dir": Path,
            "last_access": datetime,
            "stderr_log_file_obj": TextIOWrapper
        }
    }
    Where `hls_key` is a unique identifier combining logical channel ID and source service ID.
    """
    def __init__(self, config: 'Config', handler: 'ChannelHandler') -> None:
        """
        Initializes the HLSStreamManager.

        Args:
            config: The main application Config object.
            handler: The main ChannelHandler object, which holds the slots.
        """
        self.config = config
        self.handler = handler
        self.hls_ffmpeg_processes: dict['HLSKey', dict[str, Any]] = {}
        self.hls_process_lock = threading.RLock()

        self.hls_base_dir: Path = self.config.hls_base_segment_dir
        self.config.log_message(f"HLS segments will be stored in: {self.hls_base_dir}", level="DEBUG")
        
        self.cleanup_thread = threading.Thread(target=self._hls_cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def set_ffmpeg_process_long_term(self, hls_key: 'HLSKey', long_term: bool) -> None:
        """Sets if an FFmpeg process is long term for a given HLS key."""
        with self.hls_process_lock:
            if hls_key in self.hls_ffmpeg_processes:
                self.hls_ffmpeg_processes[hls_key]['is_long_term'] = long_term

    def get_ffmpeg_processes_from_logical_id(self, logical_channel_id: str, *, long_term_only: bool) -> dict['HLSKey', dict[str, Any]]:
        """
        Returns a dictionary of all FFmpeg processes associated with the given logical channel ID.
        This is useful for checking if a stream is already running.
        """
        with self.hls_process_lock:
            if long_term_only:
                return {
                    hls_key: data for hls_key, data in self.hls_ffmpeg_processes.items()
                    if data['logical_channel_id'] == logical_channel_id and data['is_long_term']
                }
            return {
                hls_key: data for hls_key, data in self.hls_ffmpeg_processes.items()
                if data['logical_channel_id'] == logical_channel_id
            }

    def record_hls_access(self, logical_channel_id: str) -> None:
        """Updates the last access time for an active stream to keep it alive."""
        with self.hls_process_lock:
            for hls_key in self.get_ffmpeg_processes_from_logical_id(logical_channel_id, long_term_only=False):
                self.hls_ffmpeg_processes[hls_key]['last_access'] = datetime.now()

    def get_hls_playlist_path(self, hls_key: 'HLSKey') -> Path | None:
        """Returns the path to the HLS playlist if the stream is active."""
        with self.hls_process_lock:
            if hls_key in self.hls_ffmpeg_processes:
                data = self.hls_ffmpeg_processes[hls_key]
                if not data['is_long_term']:
                    self.config.log_message(f"Stream {hls_key} is not long-term, cannot return playlist path.", level="ERROR")
                    return None
                return data['channel_hls_dir'] / "playlist.m3u8"
        return None
        
    def get_hls_segment_path(self, logical_channel_id: str, segment_filename: str) -> Path | None:
        """Returns the path to a specific HLS segment file if the stream is active."""
        with self.hls_process_lock:
            for hls_key in self.get_ffmpeg_processes_from_logical_id(logical_channel_id, long_term_only=True):
                return self.hls_ffmpeg_processes[hls_key]['channel_hls_dir'] / segment_filename
        return None

    def _hls_cleanup_loop(self) -> None:
        """Background thread loop to find and stop inactive or dead HLS streams."""
        self.config.log_message("HLS FFmpeg cleanup thread started.", level="INFO")
        while True:
            time.sleep(CLEANUP_POLL_INTERVAL)
            inactive_ids: set[tuple['HLSKey', str]] = set()
            with self.hls_process_lock:
                now = datetime.now()
                for hls_key, data in self.hls_ffmpeg_processes.items():
                    if data['process'].poll() is not None:
                        logical_channel_name = data['logical_channel_name']
                        self.config.log_message(f"Cleanup: Found dead process for '{logical_channel_name}' (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add((hls_key, logical_channel_name))
                    elif now - data['last_access'] > timedelta(seconds=self.config.ffmpeg_hls_inactivity_timeout):
                        logical_channel_name = data['logical_channel_name']
                        self.config.log_message(f"Cleanup: Stream '{logical_channel_name}' timed out due to inactivity (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add((hls_key, logical_channel_name))
            
            for hls_key_to_stop, logical_channel_name in inactive_ids:
                self.stop_hls_ffmpeg_process(hls_key_to_stop, logical_channel_name)

    def stop_hls_ffmpeg_process(self, hls_key: 'HLSKey', logical_channel_name: str) -> None:
        """Public method to stop an HLS stream and its associated process."""
        with self.hls_process_lock:
            if (data_to_cleanup := self.hls_ffmpeg_processes.pop(hls_key, None)) is None:
                return

        process = data_to_cleanup['process']
        provider = ProviderName(data_to_cleanup['provider_alias'])
        hls_dir = data_to_cleanup['channel_hls_dir']
        log_file = data_to_cleanup.get('stderr_log_file_obj')

        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=FFMPEG_TERMINATE_TIMEOUT)
            except subprocess.TimeoutExpired:
                self.config.log_message(f"Killing unresponsive FFmpeg process for '{logical_channel_name}' [{hls_key}].", level="WARN")
                process.kill()
        
        if log_file:
            try:
                log_file.close()
            except Exception as e:
                self.config.log_message(f"Error closing FFmpeg log file for '{logical_channel_name}' [{hls_key}]: {e}", level="ERROR")

        provider_slots = self.handler.slots.get(provider).release()
        provider_streams = self.handler.get_active_stream_status_for_logging(provider)

        try:
            if hls_dir.exists():
                shutil.rmtree(hls_dir)
        except OSError as e:
            self.config.log_message(f"Failed to clean HLS directory {hls_dir}: {e}", level="ERROR")
            
        self.config.cleanup_ffmpeg_logs_by_age()

        self.config.log_message(f"[{provider}:{provider_streams}] Successfully stopped and cleaned up all resources for '{logical_channel_name}' [{hls_key}].", level="INFO")

    def stop_hls_ffmpeg_processes(self, hls_keys: Iterable['HLSKey'] | None = None) -> None:
        """Stops all active HLS FFmpeg processes and cleans up resources."""
        if hls_keys is None:
            msg = "Stopping all active HLS FFmpeg processes:"
            with self.hls_process_lock:
                processes_to_stop = self.hls_ffmpeg_processes.copy()
        else:
            msg = f"Stopping specified HLS FFmpeg processes:"
            with self.hls_process_lock:
                processes_to_stop = {hls_key: self.hls_ffmpeg_processes[hls_key] for hls_key in hls_keys if hls_key in self.hls_ffmpeg_processes}
        self.config.log_message(f"{msg} {', '.join(f"'{v['logical_channel_name']}' [{k}]" for k, v in processes_to_stop.items())}", level="INFO")
        for lc_id, data in processes_to_stop.items():
            self.stop_hls_ffmpeg_process(lc_id, data['logical_channel_name'])
