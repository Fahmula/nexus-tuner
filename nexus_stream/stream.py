import subprocess
import json
import threading
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

# Forward-declare Config to avoid circular import issues with type hints
from typing import TYPE_CHECKING

from nexus_stream.handler import QUALITY_MONITOR_TIMEOUT
if TYPE_CHECKING:
    from nexus_stream.config import Config
    from nexus_stream.handler import ChannelHandler

# --- Constants ---
FFMPEG_TERMINATE_TIMEOUT = 5  # seconds
CLEANUP_POLL_INTERVAL = 5     # seconds
STARTUP_POLL_INTERVAL = 0.2   # seconds

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
        "logical_channel_id": {
            "process": subprocess.Popen,
            "last_access": datetime,
            "provider_alias": str,
            "channel_hls_dir": Path,
            "stderr_log_file_obj": TextIOWrapper
        }
    }
    """
    def __init__(self, config: 'Config', handler: 'ChannelHandler'):
        """
        Initializes the HLSStreamManager.

        Args:
            config: The main application Config object.
            handler: The main ChannelHandler object, which holds the semaphores.
        """
        self.config = config
        self.handler = handler
        self.hls_ffmpeg_processes: dict[str, dict[str, Any]] = {}
        self.hls_process_lock = threading.RLock()

        self.hls_base_dir: Path = self.config.hls_base_segment_dir
        self.config.log_message(f"HLS segments will be stored in: {self.hls_base_dir}", level="DEBUG")
        
        self.cleanup_thread = threading.Thread(target=self._hls_cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def _get_hls_ffmpeg_command(self, input_url: str, logical_channel_id: str) -> tuple[list[str], Path]:
        """Constructs the FFmpeg command list and creates the necessary HLS directory."""
        channel_hls_dir = self.hls_base_dir / logical_channel_id
        channel_hls_dir.mkdir(parents=True, exist_ok=True)
        
        playlist_path = channel_hls_dir / "playlist.m3u8"
        segment_filename = channel_hls_dir / "segment_%05d.ts"
        
        command = [
            self.config.ffmpeg_path,
            "-hide_banner", "-loglevel", "info",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4",
            "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
            "-user_agent", "NexusStream/1.0 (FFMPEG-HLS)",
            "-i", input_url,
            "-codec", "copy",
            "-map", "0:v:0?", "-map", "0:a:0?",
            "-f", "hls",
            "-hls_time", str(self.config.hls_segment_duration),
            "-hls_list_size", str(self.config.hls_playlist_length),
            "-hls_flags", "delete_segments+omit_endlist+program_date_time",
            "-hls_segment_filename", str(segment_filename),
            str(playlist_path)
        ]
        return command, channel_hls_dir

    def ensure_stream_is_active(self, logical_channel_id: str, actual_url: str, provider_alias: str) -> bool | None:
        """
        Ensures an HLS stream is running using a high-performance, concurrent-startup safe method.
        It allows multiple new streams to start up in parallel without blocking each other.
        Returns None if the provider is at full capacity, True if the stream is active, or False if it failed to start.
        """
        logical_channel_name = self.handler.get_logical_channel_by_id(logical_channel_id)['display_name']

        if logical_channel_id in self.hls_ffmpeg_processes:
            process_obj = self.hls_ffmpeg_processes[logical_channel_id]['process']
            if process_obj.poll() is None:
                return True

        with self.hls_process_lock:
            if logical_channel_id in self.hls_ffmpeg_processes and self.hls_ffmpeg_processes[logical_channel_id]['process'].poll() is None:
                return True
            
            provider_semaphore = self.handler.provider_semaphores.get(provider_alias)
            if not provider_semaphore or not provider_semaphore.acquire(timeout=QUALITY_MONITOR_TIMEOUT):
                self.config.log_message(f"Provider '{provider_alias}' is at full capacity. Cannot start {logical_channel_name}.", level="INFO")
                return None

            try:
                command, channel_hls_dir = self._get_hls_ffmpeg_command(actual_url, logical_channel_id)
                log_path = self.config.get_ffmpeg_log_path(logical_channel_name, provider_alias)
                stderr_log_file = open(log_path, 'a', encoding='utf-8')
                process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=stderr_log_file)
                
                self.hls_ffmpeg_processes[logical_channel_id] = {
                    'process': process,
                    'last_access': datetime.now(),
                    'provider_alias': provider_alias,
                    'channel_hls_dir': channel_hls_dir,
                    'stderr_log_file_obj': stderr_log_file
                }
                self.config.log_message(f"Claimed slot and started FFmpeg for '{logical_channel_name}' (PID: {process.pid}).", level="INFO")
            except Exception as e:
                self.config.log_message(f"Immediate Popen failure for {logical_channel_name}: {e}", level="CRITICAL")
                provider_semaphore.release()
                return False

        try:
            start_time = time.monotonic()
            while time.monotonic() - start_time < self.config.ffmpeg_start_timeout:
                if process.poll() is not None:
                    raise ChildProcessError(f"FFmpeg process for {logical_channel_name} (PID: {process.pid}) exited prematurely.")
                if any(channel_hls_dir.glob('*.ts')):
                    self.config.log_message(f"FFmpeg for '{logical_channel_name}' (PID: {process.pid}) is now healthy.", level="INFO")
                    provider_streams = self.handler.get_active_stream_status_for_logging(provider_alias)
                    self.config.log_message(f"{provider_alias} stream count: {provider_streams}", level="INFO")
                    return True
                time.sleep(STARTUP_POLL_INTERVAL)
            
            raise TimeoutError(f"FFmpeg for {logical_channel_name} (PID: {process.pid}) timed out waiting for segments.")

        except (ChildProcessError, TimeoutError) as e:
            self.config.log_message(f"Validation failed for {logical_channel_name}: {e}. Cleaning up.", level="ERROR")
            self._stop_hls_ffmpeg_process_internal(logical_channel_id)
            return False

    def record_hls_access(self, logical_channel_id: str) -> None:
        """Updates the last access time for an active stream to keep it alive."""
        with self.hls_process_lock:
            if logical_channel_id in self.hls_ffmpeg_processes:
                self.hls_ffmpeg_processes[logical_channel_id]['last_access'] = datetime.now()

    def get_hls_playlist_path(self, logical_channel_id: str) -> Path | None:
        """Returns the path to the HLS playlist if the stream is active."""
        with self.hls_process_lock:
            if logical_channel_id in self.hls_ffmpeg_processes:
                data = self.hls_ffmpeg_processes[logical_channel_id]
                return data['channel_hls_dir'] / "playlist.m3u8"
        return None
        
    def get_hls_segment_path(self, logical_channel_id: str, segment_filename: str) -> Path | None:
        """Returns the path to a specific HLS segment file if the stream is active."""
        with self.hls_process_lock:
            if logical_channel_id in self.hls_ffmpeg_processes:
                return self.hls_ffmpeg_processes[logical_channel_id]['channel_hls_dir'] / segment_filename
        return None

    def _stop_hls_ffmpeg_process_internal(self, logical_channel_id: str) -> None:
        """Stops an FFmpeg process and cleans up all associated resources."""
        
        data_to_cleanup = None
        with self.hls_process_lock:
            if (data := self.hls_ffmpeg_processes.pop(logical_channel_id, None)) is None:
                return
            data_to_cleanup = data

        if data_to_cleanup:
            process = data_to_cleanup['process']
            provider = data_to_cleanup['provider_alias']
            hls_dir = data_to_cleanup['channel_hls_dir']
            log_file = data_to_cleanup.get('stderr_log_file_obj')
            logical_channel_name = self.handler.get_logical_channel_by_id(logical_channel_id)['display_name']

            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=FFMPEG_TERMINATE_TIMEOUT)
                except subprocess.TimeoutExpired:
                    self.config.log_message(f"Killing unresponsive FFmpeg process for '{logical_channel_name}'.", level="WARN")
                    process.kill()
            
            if log_file and not log_file.closed:
                try:
                    log_file.close()
                except Exception as e:
                    self.config.log_message(f"Error closing FFmpeg log file for '{logical_channel_name}': {e}", level="ERROR")

            provider_semaphore = self.handler.provider_semaphores.get(provider)
            provider_semaphore.release()

            provider_streams = self.handler.get_active_stream_status_for_logging(provider)
            self.config.log_message(f"{provider} stream count: {provider_streams}", level="INFO")

            try:
                if hls_dir.exists():
                    shutil.rmtree(hls_dir)
            except OSError as e:
                self.config.log_message(f"Failed to clean HLS directory {hls_dir}: {e}", level="ERROR")
                
            self.config.cleanup_ffmpeg_logs_by_age()

            self.config.log_message(f"Successfully stopped and cleaned up all resources for '{logical_channel_name}'.", level="INFO")

    def _hls_cleanup_loop(self) -> None:
        """Background thread loop to find and stop inactive or dead HLS streams."""
        self.config.log_message("HLS FFmpeg cleanup thread started.", level="INFO")
        while True:
            time.sleep(CLEANUP_POLL_INTERVAL)
            inactive_ids = set()
            with self.hls_process_lock:
                now = datetime.now()
                for lc_id, data in self.hls_ffmpeg_processes.items():
                    if data['process'].poll() is not None:
                        logical_channel_name = self.handler.get_logical_channel_by_id(lc_id)['display_name']
                        self.config.log_message(f"Cleanup: Found dead process for '{logical_channel_name}' (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add(lc_id)
                    elif now - data['last_access'] > timedelta(seconds=self.config.ffmpeg_hls_inactivity_timeout):
                        logical_channel_name = self.handler.get_logical_channel_by_id(lc_id)['display_name']
                        self.config.log_message(f"Cleanup: Stream '{logical_channel_name}' timed out due to inactivity (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add(lc_id)
            
            for lc_id_to_stop in inactive_ids:
                self.stop_hls_ffmpeg_process(lc_id_to_stop)

    def stop_hls_ffmpeg_process(self, logical_channel_id: str) -> None:
        """Public method to stop an HLS stream and its associated process."""
        self._stop_hls_ffmpeg_process_internal(logical_channel_id)


    def stop_all_hls_ffmpeg_processes(self) -> None:
        """Stops all active HLS FFmpeg processes and cleans up resources."""
        self.config.log_message("Stopping all HLS FFmpeg processes...", level="INFO")
        for lc_id in list(self.hls_ffmpeg_processes.keys()):
            self.stop_hls_ffmpeg_process(lc_id)
