import subprocess
import threading
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Callable, Any

# Forward-declare Config to avoid circular import issues with type hints
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from config import Config

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
        "lc_user_id": {
            "process": subprocess.Popen,
            "last_access": datetime,
            "provider_alias": str,
            "channel_hls_dir": Path,
            "stderr_log_file_obj": TextIOWrapper
        }
    }
    """
    def __init__(self, config: 'Config', check_capacity_func: Callable[[str], bool], 
                 increment_count_func: Callable[[str], None], decrement_count_func: Callable[[str], None]):
        """
        Initializes the HLSStreamManager.

        Args:
            config: The main application Config object.
            check_capacity_func: A function to check if a provider has capacity.
            increment_count_func: A function to increment a provider's stream count.
            decrement_count_func: A function to decrement a provider's stream count.
        """
        self.config = config
        self.hls_ffmpeg_processes: dict[str, dict[str, Any]] = {}
        self.hls_process_lock = threading.RLock()
        
        # Callbacks to the ChannelHandler to manage provider stream counts
        self.check_capacity = check_capacity_func
        self.increment_count = increment_count_func
        self.decrement_count = decrement_count_func

        self.hls_base_dir: Path = self.config.hls_base_segment_dir
        self.config.log_message(f"HLS segments will be stored in: {self.hls_base_dir}", level="DEBUG")
        
        self.cleanup_thread = threading.Thread(target=self._hls_cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def _get_hls_ffmpeg_command(self, input_url: str, lc_user_id: str) -> tuple[list[str], Path]:
        """Constructs the FFmpeg command list and creates the necessary HLS directory."""
        channel_hls_dir = self.hls_base_dir / lc_user_id
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

    def ensure_stream_is_active(self, lc_user_id: str, actual_url: str, provider_alias: str) -> bool:
        """
        Ensures an HLS stream is running for a given channel.
        
        If a process is already running, it updates its last access time.
        If not, it checks provider capacity and attempts to start a new FFmpeg process.

        Args:
            lc_user_id: The unique ID of the logical channel.
            actual_url: The source stream URL to transcode.
            provider_alias: The alias of the provider for capacity checking.

        Returns:
            True if a stream is active or was successfully started, False otherwise.
        """
        with self.hls_process_lock:
            # Check if a healthy process already exists
            if lc_user_id in self.hls_ffmpeg_processes:
                process_obj = self.hls_ffmpeg_processes[lc_user_id]['process']
                if process_obj.poll() is None:
                    self.hls_ffmpeg_processes[lc_user_id]['last_access'] = datetime.now()
                    return True
                else:
                    self.config.log_message(f"Found dead HLS process for {lc_user_id} (PID: {process_obj.pid}). Cleaning up before restart.", level="INFO")
                    self._stop_hls_ffmpeg_process_internal(lc_user_id)

            # Check provider capacity before starting a new process
            if not self.check_capacity(provider_alias):
                self.config.log_message(f"Provider '{provider_alias}' at capacity. Cannot start HLS for '{lc_user_id}'.", level="WARN")
                return False

            # Start a new process
            command, channel_hls_dir = self._get_hls_ffmpeg_command(actual_url, lc_user_id)
            log_path = self.config.get_ffmpeg_log_path(lc_user_id, provider_alias)
            
            try:
                stderr_log_file = open(log_path, 'w', encoding='utf-8')
                process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=stderr_log_file)
                self.config.log_message(f"Started FFmpeg for '{provider_alias}' -> '{lc_user_id}' (PID: {process.pid}).", level="INFO")

                # Wait for the first segment to appear to confirm successful startup
                start_time = time.monotonic()
                while time.monotonic() - start_time < self.config.ffmpeg_start_timeout:
                    if process.poll() is not None: # Process died during startup
                        raise ChildProcessError(f"FFmpeg process for {lc_user_id} exited prematurely (Code: {process.returncode}).")
                    if any(channel_hls_dir.glob('*.ts')):
                        # Success!
                        self.hls_ffmpeg_processes[lc_user_id] = {
                            'process': process,
                            'last_access': datetime.now(),
                            'provider_alias': provider_alias,
                            'channel_hls_dir': channel_hls_dir,
                            'stderr_log_file_obj': stderr_log_file
                        }
                        self.increment_count(provider_alias)
                        return True
                    time.sleep(STARTUP_POLL_INTERVAL)
                
                # Timeout waiting for segments
                raise TimeoutError(f"FFmpeg process for {lc_user_id} started but did not produce segments in time.")

            except (ChildProcessError, TimeoutError, OSError, ValueError) as e:
                self.config.log_message(f"Failed to start or validate FFmpeg for {lc_user_id}: {e}", level="ERROR")
                # Ensure any spawned resources are cleaned up on failure
                if 'process' in locals() and process.poll() is None:
                    process.kill()
                if 'stderr_log_file' in locals():
                    stderr_log_file.close()
                shutil.rmtree(channel_hls_dir, ignore_errors=True)
                return False

    def record_hls_access(self, lc_user_id: str) -> None:
        """Updates the last access time for an active stream to keep it alive."""
        with self.hls_process_lock:
            if lc_user_id in self.hls_ffmpeg_processes:
                self.hls_ffmpeg_processes[lc_user_id]['last_access'] = datetime.now()

    def get_hls_playlist_path(self, lc_user_id: str) -> Path | None:
        """Returns the path to the HLS playlist if the stream is active."""
        with self.hls_process_lock:
            if lc_user_id in self.hls_ffmpeg_processes:
                data = self.hls_ffmpeg_processes[lc_user_id]
                return data['channel_hls_dir'] / "playlist.m3u8"
        return None
        
    def get_hls_segment_path(self, lc_user_id: str, segment_filename: str) -> Path | None:
        """Returns the path to a specific HLS segment file if the stream is active."""
        with self.hls_process_lock:
            if lc_user_id in self.hls_ffmpeg_processes:
                return self.hls_ffmpeg_processes[lc_user_id]['channel_hls_dir'] / segment_filename
        return None

    def _stop_hls_ffmpeg_process_internal(self, lc_user_id: str) -> None:
        """Stops an FFmpeg process, decrements provider count, and cleans up files."""
        with self.hls_process_lock:
            if (data := self.hls_ffmpeg_processes.pop(lc_user_id, None)) is None:
                return

            process, provider, hls_dir, log_file = (
                data['process'], data['provider_alias'], data['channel_hls_dir'], data.get('stderr_log_file_obj')
            )

            if process.poll() is None:
                self.config.log_message(f"Stopping FFmpeg HLS for '{lc_user_id}' (PID: {process.pid}).", level="INFO")
                process.terminate()
                try:
                    process.wait(timeout=FFMPEG_TERMINATE_TIMEOUT)
                except subprocess.TimeoutExpired:
                    self.config.log_message(f"Killing unresponsive FFmpeg process for '{lc_user_id}' (PID: {process.pid}).", level="WARN")
                    process.kill()
            
            if log_file:
                try:
                    log_file.close()
                except Exception as e:
                    self.config.log_message(f"Error closing FFmpeg log file for '{lc_user_id}': {e}", level="ERROR")

            self.decrement_count(provider)
            self.config.cleanup_ffmpeg_logs_by_age()
            
            try:
                if hls_dir.exists():
                    shutil.rmtree(hls_dir)
            except OSError as e:
                self.config.log_message(f"Failed to clean HLS directory {hls_dir}: {e}", level="ERROR")

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
                        self.config.log_message(f"Cleanup: Found dead process for '{lc_id}' (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add(lc_id)
                    elif now - data['last_access'] > timedelta(seconds=self.config.ffmpeg_hls_inactivity_timeout):
                        self.config.log_message(f"Cleanup: Stream '{lc_id}' timed out due to inactivity (PID: {data['process'].pid}).", level="INFO")
                        inactive_ids.add(lc_id)
            
            for lc_id_to_stop in inactive_ids:
                self.stop_hls_ffmpeg_process(lc_id_to_stop)

    def stop_hls_ffmpeg_process(self, lc_user_id: str) -> None:
        """Public method to stop an HLS stream and its associated process."""
        self._stop_hls_ffmpeg_process_internal(lc_user_id)