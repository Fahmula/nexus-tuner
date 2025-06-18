from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import subprocess
import threading
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, NewType
from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.slots import ProviderName, ProviderSlots


HLSKey = NewType("HLSKey", str)
HLSName = NewType("HLSName", str)

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
        "hls_key": {
            "process": subprocess.Popen,
            "provider_alias": str,
            "logical_channel_id": str,
            "source_service_id": str,
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
        self.hls_ffmpeg_processes: dict[HLSKey, dict[str, Any]] = {}
        self.hls_process_lock = threading.RLock()

        self.hls_base_dir: Path = self.config.hls_base_segment_dir
        self.config.log_message(f"HLS segments will be stored in: {self.hls_base_dir}", level="DEBUG")
        
        self.cleanup_thread = threading.Thread(target=self._hls_cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def _create_hls_key(self, logical_channel_id: str, source_service_id: str) -> HLSKey:
        """Generates a unique key for the HLS stream."""
        return HLSKey(f"{logical_channel_id}_{source_service_id}")

    def _create_hls_name(self, logical_channel_name: str, source_service_id: str) -> HLSName:
        """Generates a unique name for the HLS stream."""
        return HLSName(f"{logical_channel_name}_{source_service_id}")

    def _get_hls_ffmpeg_command(self, input_url: str, hls_key: HLSKey) -> tuple[list[str], Path]:
        """Constructs the FFmpeg command list and creates the necessary HLS directory."""
        channel_hls_dir = self.hls_base_dir / self.config.get_fs_safe_alphanum(hls_key)
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

    def get_ffmpeg_processes_from_logical_id(self, logical_channel_id: str) -> dict[HLSKey, dict[str, Any]]:
        """
        Returns a dictionary of all FFmpeg processes associated with the given logical channel ID.
        This is useful for checking if a stream is already running.
        """
        with self.hls_process_lock:
            return {
                hls_key: data for hls_key, data in self.hls_ffmpeg_processes.items()
                if data['logical_channel_id'] == logical_channel_id
            }

    def create_hls_stream(self, logical_channel_id: str, logical_channel_name: str, sources: list[dict[str, Any]] | None = None) -> HLSKey | tuple[int, str]:
        """
        Creates an HLS stream for the given logical channel ID and name with all providers simultaneously.
        Returns the HLSKey if successful, or an error tuple (status_code, message) if not.
        """
        if sources is None:
            sources_to_try = self.handler.get_sources_for_client_facing_channel(logical_channel_id)
        else:
            sources_to_try = deepcopy(sources)
        if not sources_to_try:
            return 404, f"Logical channel '{logical_channel_name}' ({logical_channel_id}) not found or has no sources."

        provider_sources: dict[ProviderName, list[dict[str, Any]]] = {}
        for source in sources_to_try:
            provider_sources.setdefault(ProviderName(source['provider_alias']), []).append(source)

        results: list[tuple[HLSKey, dict[str, str]]] = []
        mutex = threading.Lock()
        inital_deadline = time.monotonic() + 10
        with ThreadPoolExecutor(max_workers=len(provider_sources)) as executor:
            for provider_alias, sources in provider_sources.items():
                executor.submit(self._create_provider_streams, provider_alias, logical_channel_id, logical_channel_name, sources, results, [inital_deadline], mutex)
        if not results:
            return 503, f"Failed to start HLS stream for '{logical_channel_name}' from any source."
        hls_key = min(results, key=lambda x: x[1]["priority"])[0]
        for other_key, source in results:
            if other_key != hls_key:
                self.stop_hls_ffmpeg_process(other_key, logical_channel_name)
        return hls_key

    def _create_provider_streams(self, provider_alias: ProviderName, logical_channel_id: str, logical_channel_name: str, sources: list[dict[str, Any]], results: list[tuple[HLSKey, dict[str, str]]], deadlines: list[float], mutex: threading.Lock) -> None:
        """Creates HLS streams for a specific provider with all sources simulatenously."""
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            self.config.log_message(f"Provider '{provider_alias}' does not exist.", level="CRITICAL")
            return
        sources.sort(key=lambda x: x["priority"], reverse=True)
        max_streams = self.handler.get_provider_stream_status()[provider_alias]["max"]
        with ThreadPoolExecutor(max_workers=max_streams) as executor:
            for _ in range(max_streams):
                executor.submit(self._provider_worker_thread, provider_slots, provider_alias, logical_channel_id, logical_channel_name, sources, results, deadlines, mutex)

    def _provider_worker_thread(self, provider_slots: ProviderSlots, provider_alias: ProviderName, logical_channel_id: str, logical_channel_name: str, sources: list[dict[str, Any]], results: list[tuple[HLSKey, dict[str, str]]], deadlines: list[float], mutex: threading.Lock) -> None:
        """Creates a single HLS stream for each provider source as resources become available until success, dealine, or no more sources."""
        start_time = time.monotonic()
        source = self._pop_source(sources, mutex)
        if not source:
            return
        hls_key = self._create_hls_key(logical_channel_id, source['source_service_id'])
        while source and time.monotonic() - start_time < self._get_deadline(deadlines, mutex):
            self.hls_process_lock.acquire()
            if hls_key in self.hls_ffmpeg_processes and self.hls_ffmpeg_processes[hls_key]['process'].poll() is None:
                self.hls_process_lock.release()
                self._set_result(results, hls_key, source, mutex)
                self._set_deadline(deadlines, time.monotonic() + 1, mutex)
                return

            if not provider_slots.acquire(blocking=False):
                self.hls_process_lock.release()
                time.sleep(0.01)
                continue
            
            hls_key = self._create_single_stream(provider_slots, provider_alias, logical_channel_id, logical_channel_name, source['source_service_id'], source['actual_stream_url'])
            if hls_key:
                self._set_result(results, hls_key, source, mutex)
                self._set_deadline(deadlines, time.monotonic() + 1, mutex)
                return

            source = self._pop_source(sources, mutex)
            if not source:
                return
            hls_key = self._create_hls_key(logical_channel_id, source['source_service_id'])

    def _get_deadline(self, deadlines: list[float], mutex: threading.Lock) -> float:
        with mutex:
            return min(deadlines)

    def _set_deadline(self, deadlines: list[float], new_deadline: float, mutex: threading.Lock) -> None:
        with mutex:
            deadlines[0] = min(deadlines[0], new_deadline)

    def _pop_source(self, sources: list[dict[str, Any]], mutex: threading.Lock) -> dict[str, Any] | None:
        with mutex:
            if sources:
                return sources.pop()
            return None

    def _set_result(self, results: list[tuple[HLSKey, dict[str, str]]], hls_key: HLSKey, source: dict[str, str], mutex: threading.Lock) -> None:
        with mutex:
            results.append((hls_key, source))

    def _create_single_stream(self, provider_slots: ProviderSlots, provider_alias: ProviderName, logical_channel_id: str, logical_channel_name: str, source_service_id: str, actual_url: str) -> HLSKey | None:
        """
        Ensures an HLS stream is running using a high-performance, concurrent-startup safe method.
        It allows multiple new streams to start up in parallel without blocking each other.
        Returns the HLSKey if successful, or None if it fails to start.
        """
        try:
            hls_key = self._create_hls_key(logical_channel_id, source_service_id)
            hls_name = self._create_hls_name(logical_channel_id, source_service_id)

            try:
                command, channel_hls_dir = self._get_hls_ffmpeg_command(actual_url, hls_key)
                log_path = self.config.get_ffmpeg_log_path(hls_name)
                stderr_log_file = open(log_path, 'a', encoding='utf-8')
                process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=stderr_log_file)
            except Exception as e:
                self.config.log_message(f"Immediate Popen failure for {hls_name}: {e}", level="CRITICAL")
                provider_slots.release()
                return

            self.hls_ffmpeg_processes[hls_key] = {
                'process': process,
                'provider_alias': provider_alias,
                'logical_channel_id': logical_channel_id,
                'source_service_id': source_service_id,
                'logical_channel_name': logical_channel_name,
                'channel_hls_dir': channel_hls_dir,
                'last_access': datetime.now(),
                'stderr_log_file_obj': stderr_log_file
            }
            self.config.log_message(f"Claimed slot and started FFmpeg for '{hls_name}' (PID: {process.pid}).", level="INFO")
        finally:
            self.hls_process_lock.release()

        try:
            start_time = time.monotonic()
            while time.monotonic() - start_time < self.config.ffmpeg_start_timeout:
                if process.poll() is not None:
                    raise ChildProcessError(f"FFmpeg process for {hls_name} (PID: {process.pid}) exited prematurely.")
                if any(channel_hls_dir.glob('*.ts')):
                    self.config.log_message(f"FFmpeg for '{hls_name}' (PID: {process.pid}) is now healthy.", level="INFO")
                    provider_streams = self.handler.get_active_stream_status_for_logging(provider_alias)
                    self.config.log_message(f"{provider_alias} stream count: {provider_streams}", level="INFO")
                    return hls_key
                time.sleep(STARTUP_POLL_INTERVAL)
            
            raise TimeoutError(f"FFmpeg for {hls_name} (PID: {process.pid}) timed out waiting for segments.")

        except (ChildProcessError, TimeoutError) as e:
            self.config.log_message(f"Validation failed for {hls_name}: {e}. Cleaning up.", level="ERROR")
            self.stop_hls_ffmpeg_process(hls_key, logical_channel_name)
            return

    def record_hls_access(self, logical_channel_id: str) -> None:
        """Updates the last access time for an active stream to keep it alive."""
        with self.hls_process_lock:
            for hls_key in self.get_ffmpeg_processes_from_logical_id(logical_channel_id):
                self.hls_ffmpeg_processes[hls_key]['last_access'] = datetime.now()

    def get_hls_playlist_path(self, hls_key: HLSKey) -> Path | None:
        """Returns the path to the HLS playlist if the stream is active."""
        with self.hls_process_lock:
            if hls_key in self.hls_ffmpeg_processes:
                data = self.hls_ffmpeg_processes[hls_key]
                return data['channel_hls_dir'] / "playlist.m3u8"
        return None
        
    def get_hls_segment_path(self, logical_channel_id: str, segment_filename: str) -> Path | None:
        """Returns the path to a specific HLS segment file if the stream is active."""
        with self.hls_process_lock:
            for hls_key in self.get_ffmpeg_processes_from_logical_id(logical_channel_id):
                return self.hls_ffmpeg_processes[hls_key]['channel_hls_dir'] / segment_filename
        return None

    def _hls_cleanup_loop(self) -> None:
        """Background thread loop to find and stop inactive or dead HLS streams."""
        self.config.log_message("HLS FFmpeg cleanup thread started.", level="INFO")
        while True:
            time.sleep(CLEANUP_POLL_INTERVAL)
            inactive_ids: set[tuple[HLSKey, str]] = set()
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

    def stop_hls_ffmpeg_process(self, hls_key: HLSKey, logical_channel_name: str) -> None:
        """Public method to stop an HLS stream and its associated process."""
        data_to_cleanup = None
        with self.hls_process_lock:
            if (data := self.hls_ffmpeg_processes.pop(hls_key, None)) is None:
                return
            data_to_cleanup = data
        if not data_to_cleanup:
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
        
        if log_file and not log_file.closed:
            try:
                log_file.close()
            except Exception as e:
                self.config.log_message(f"Error closing FFmpeg log file for '{logical_channel_name}' [{hls_key}]: {e}", level="ERROR")

        provider_slots = self.handler.slots.get(provider).release()

        provider_streams = self.handler.get_active_stream_status_for_logging(provider)
        self.config.log_message(f"{provider} stream count: {provider_streams}", level="INFO")

        try:
            if hls_dir.exists():
                shutil.rmtree(hls_dir)
        except OSError as e:
            self.config.log_message(f"Failed to clean HLS directory {hls_dir}: {e}", level="ERROR")
            
        self.config.cleanup_ffmpeg_logs_by_age()

        self.config.log_message(f"Successfully stopped and cleaned up all resources for '{logical_channel_name}' [{hls_key}].", level="INFO")

    def stop_all_hls_ffmpeg_processes(self) -> None:
        """Stops all active HLS FFmpeg processes and cleans up resources."""
        self.config.log_message("Stopping all HLS FFmpeg processes...", level="INFO")
        for lc_id, data in self.hls_ffmpeg_processes.items():
            self.stop_hls_ffmpeg_process(lc_id, data['logical_channel_name'])
