from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import shutil
import subprocess
import threading
import time
from typing import Any, NewType

from nexus_stream.config import Config
from nexus_stream.quality_monitor import QualityMonitor
from nexus_stream.slots import ProviderName, ProviderSlots
from nexus_stream.stream import ChannelHandler, HLSStreamManager


HLSKey = NewType("HLSKey", str)
HLSName = NewType("HLSName", str)


STARTUP_POLL_INTERVAL = 0.2   # seconds


def create_hls_key(logical_channel_id: str, source_service_id: str) -> HLSKey:
    """Generates a unique key for the HLS stream."""
    return HLSKey(f"{logical_channel_id}_{source_service_id}")


def create_hls_name(logical_channel_name: str, source_service_id: str) -> HLSName:
    """Generates a unique name for the HLS stream."""
    return HLSName(f"{logical_channel_name} - {source_service_id}")


def create_hls_ffmpeg_command(hls_manager: HLSStreamManager, config: Config, input_url: str, hls_key: HLSKey) -> tuple[list[str], Path]:
    """Constructs the FFmpeg command list and creates the necessary HLS directory."""
    channel_hls_dir = hls_manager.hls_base_dir / config.get_fs_safe_alphanum(hls_key)
    channel_hls_dir.mkdir(parents=True, exist_ok=True)
    
    playlist_path = channel_hls_dir / "playlist.m3u8"
    segment_filename = channel_hls_dir / "segment_%05d.ts"
    
    command = [
        config.ffmpeg_path,
        "-hide_banner", "-loglevel", "info",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4",
        "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
        "-user_agent", "NexusStream/1.0 (FFMPEG-HLS)",
        "-i", input_url,
        "-codec", "copy",
        "-map", "0:v:0?", "-map", "0:a:0?",
        "-f", "hls",
        "-hls_time", str(config.hls_segment_duration),
        "-hls_list_size", str(config.hls_playlist_length),
        "-hls_flags", "delete_segments+omit_endlist+program_date_time",
        "-hls_segment_filename", str(segment_filename),
        str(playlist_path)
    ]
    return command, channel_hls_dir


class CreateHLSStream:
    """
    A class to acquire and use resources for creating a stream.
    Multiple instances of this class can run in parallel regardless
    of input parameters.
    """

    def __init__(self, config: Config, handler: ChannelHandler, hls_manager: HLSStreamManager, quality_monitor: QualityMonitor, logical_channel_id: str, logical_channel_name: str, input_sources: list[dict[str, Any]] | None = None) -> None:
        self.config = config
        self.handler = handler
        self.hls_manager = hls_manager
        self.quality_monitor = quality_monitor
        self._res: HLSKey | tuple[int, str] = (500, "Stream not created yet")  # Final result of the stream, accessed via `result()`
        self._result_mutex = threading.Lock()  # Used to block `result()` until the stream is created or an error occurs
        self._mutex = threading.Lock()  # Used to synchronize access to shared state within this instance for all the threads
    
        self.logical_channel_id = logical_channel_id
        self.logical_channel_name = logical_channel_name
        sources = self.handler.get_sources_for_client_facing_channel(logical_channel_id) if input_sources is None else deepcopy(input_sources)
        if not sources:
            self._res = (404, f"Logical channel '{logical_channel_name}' ({logical_channel_id}) not found or has no sources.")
            return

        # Prioritize sources based on manual priority then quality score
        # Convert these into a integer 0, 1, 2, ... relative to these sources
        # Ones that are exactly equal to have the same number, lower is better
        self._quality_scores = self.quality_monitor.get_quality_scores()
        prev_score = None
        curr_priority = -1
        source_scores = sorted(((source["priority"], -self._quality_scores.get(source["source_service_id"], {}).get("total_score", 0)), source["source_service_id"]) for source in sources)
        source_priorities: dict[str, int] = {}
        for score, source_service_id in source_scores:
            if score != prev_score:
                prev_score = score
                curr_priority += 1
            source_priorities[source_service_id] = curr_priority
        sources.sort(key=lambda x: source_priorities[x["source_service_id"]], reverse=True)  # Highest priority last since we use `pop()`
        self._remaining_priorities = source_priorities.copy()  # To know if the stream we just found is the best one (exculding failed sources)

        self._results: list[tuple[HLSKey, dict[str, Any]]] = []  # All the successful streams that we will choose from for self._res
        self._selected = False  # If a stream has been chosen, stop the other threads early
        self._active_hls_keys: list[HLSKey] = []  # All the streams that we have started and will stop (exculding the one that is selected)
        self._deadline = time.monotonic() + 10  # Maximum deadline for this entire process
        self._threads: list[threading.Thread] = []  # Used to end before deadline if all sources are exhausted
        all_provider_sources: dict[ProviderName, list[dict[str, Any]]] = {}
        for source in sources:
            all_provider_sources.setdefault(ProviderName(source["provider_alias"]), []).append(source)  # Sources maintain their order from above

        # Create a thread for each provider to handle its sources
        for provider_alias, provider_sources in all_provider_sources.items():
            thread = threading.Thread(target=self._create_provider_stream, args=(provider_alias, provider_sources))
            thread.start()
            self._threads.append(thread)

        self._result_mutex.acquire()  # Block `result()` until we finish processing results
        threading.Thread(target=self._process_results, daemon=True).start()

    def _get_deadline(self) -> float:
        with self._mutex:
            return self._deadline

    def _get_selected(self) -> bool:
        with self._mutex:
            return self._selected

    def _set_deadline(self, source: dict[str, Any]) -> None:
        with self._mutex:
            if self._remaining_priorities[source["source_service_id"]] <= min(self._remaining_priorities.values()):
                new_deadline = time.monotonic()  # Found best remaining source, no need to wait
            else:
                new_deadline = time.monotonic() + 1
            self._deadline = min(self._deadline, new_deadline)

    def _set_result(self, hls_key: HLSKey, source: dict[str, Any]) -> bool:
        with self._mutex:
            if self._selected:
                return False  # Don't bother setting result if we already selected a stream
            self._results.append((hls_key, source))
            return True

    def _remove_active_hls_key(self, hls_key: HLSKey) -> None:
        with self._mutex:
            self._active_hls_keys.remove(hls_key)

    def _pop_source(self, provider_sources: list[dict[str, Any]], current_source: dict[str, Any] | None) -> dict[str, Any] | None:
        with self._mutex:
            if current_source:  # This source has failed, update what the best remaining source is
                self._remaining_priorities.pop(current_source["source_service_id"])
            if provider_sources:
                return provider_sources.pop()
            return None

    def _create_provider_stream(self, provider_alias: ProviderName, provider_sources: list[dict[str, Any]]) -> None:
        """Creates HLS streams for a specific provider using all sources."""
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            self.config.log_message(f"Provider '{provider_alias}' does not exist.", level="CRITICAL")
            return

        # We create as many threads as the maximum number of streams allowed for this provider
        # Each thread will only occupy the remaining slots accurately, dynamically adjusting if resources are freed elsewhere
        max_streams = self.handler.get_provider_stream_status()[provider_alias]["max"]
        with ThreadPoolExecutor(max_workers=max_streams) as executor:
            for _ in range(max_streams):
                executor.submit(self._provider_worker_thread, provider_alias, provider_slots, provider_sources)

    def _provider_worker_thread(self, provider_alias: ProviderName, provider_slots: ProviderSlots, provider_sources: list[dict[str, Any]]) -> None:
        """Tries all sources for a provider until a stream is created or all sources are exhausted."""
        source = self._pop_source(provider_sources, None)
        if not source:
            return
        hls_key = create_hls_key(self.logical_channel_id, source["source_service_id"])
        while time.monotonic() < self._get_deadline():
            if self._get_selected():
                return
            self.hls_manager.hls_process_lock.acquire()  # We don't want to release this lock too early since we need to reacquire it to create a stream
            if hls_key in self.hls_manager.hls_ffmpeg_processes and self.hls_manager.hls_ffmpeg_processes[hls_key]["process"].poll() is None:  # Process already exists
                if not self.hls_manager.hls_ffmpeg_processes[hls_key]["is_long_term"]:
                    self.hls_manager.hls_process_lock.release()
                    time.sleep(0.01)
                    continue  # Short term streams can be killed at any time
                self.hls_manager.hls_process_lock.release()
                if not self._set_result(hls_key, source):
                    return  # Don't stop process since it's a long running process owned elsewhere
                self._set_deadline(source)
                return

            # This ensure we are respecting the global available slots, it is released by `self._create_stream()`
            if not provider_slots.acquire(blocking=False):
                self.hls_manager.hls_process_lock.release()
                self.hls_manager.prune_hls_ffmpeg_processes()  # If any inactive streams, prune them so we can start this. Allows a user to switch channels.
                time.sleep(0.01)
                continue
            
            hls_key = self._create_stream(hls_key, provider_alias, provider_slots, source)
            if hls_key:
                if not self._set_result(hls_key, source):
                    self.hls_manager.stop_hls_ffmpeg_process(hls_key, self.logical_channel_name)
                    return
                self._set_deadline(source)
                return

            # Source failed, try the next one
            source = self._pop_source(provider_sources, source)
            if not source:
                return
            hls_key = create_hls_key(self.logical_channel_id, source["source_service_id"])

    def _create_stream(self, hls_key: HLSKey, provider_alias: ProviderName, provider_slots: ProviderSlots, source: dict[str, Any]) -> HLSKey | None:
        """Creates an HLS stream using the provided source and provider slots."""
        try:
            hls_name = create_hls_name(self.logical_channel_name, source["source_service_id"])

            channel_hls_dir = None
            stderr_log_file = None
            try:
                command, channel_hls_dir = create_hls_ffmpeg_command(self.hls_manager, self.config, source["actual_stream_url"], hls_key)
                log_path = self.config.get_ffmpeg_log_path(hls_name)
                stderr_log_file = open(log_path, 'a', encoding='utf-8')
            except Exception as e:
                self.config.log_message(f"Failed to create FFmpeg command for {hls_name}: {e}", level="CRITICAL")
                provider_slots.release()
                if stderr_log_file:
                    try:
                        stderr_log_file.close()
                    except Exception as e:
                        self.config.log_message(f"Failed to close log file for {hls_name}: {e}", level="CRITICAL")
                if channel_hls_dir:
                    shutil.rmtree(channel_hls_dir, ignore_errors=True)
                return

            try:
                with self._mutex:
                    if self._selected:
                        provider_slots.release()
                        try:
                            stderr_log_file.close()
                        except Exception as e:
                            self.config.log_message(f"Failed to close log file for {hls_name}: {e}", level="CRITICAL")
                        shutil.rmtree(channel_hls_dir, ignore_errors=True)
                        return
                    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=stderr_log_file)
                    self._active_hls_keys.append(hls_key)
                    self.hls_manager.hls_ffmpeg_processes[hls_key] = {
                        'process': process,
                        'is_long_term': False,
                        'provider_alias': provider_alias,
                        'logical_channel_id': self.logical_channel_id,
                        'source_service_id': source["source_service_id"],
                        'logical_channel_name': self.logical_channel_name,
                        'channel_hls_dir': channel_hls_dir,
                        'last_access': datetime.now(),
                        'stderr_log_file_obj': stderr_log_file
                    }
            except Exception as e:
                self.config.log_message(f"Immediate Popen failure for {hls_name}: {e}", level="CRITICAL")
                provider_slots.release()
                return
        finally:
            self.hls_manager.hls_process_lock.release()
        provider_streams = self.handler.get_active_stream_status_for_logging(provider_alias)
        quality_score = self._quality_scores.get(source["source_service_id"])
        quality_msg = f"Score={quality_score['total_score']:.2f} | Uptime={quality_score['uptime_100']*100:.2f}%" if quality_score else "Score=Unknown | Uptime=Unknown"
        self.config.log_message(f"[{provider_alias}:{provider_streams}] Claimed slot and started FFmpeg for '{hls_name}' [Priority={source['priority']} | {quality_msg}] (PID: {process.pid}).", level="INFO")

        try:
            end_time = time.monotonic() + self.config.ffmpeg_start_timeout
            while time.monotonic() < end_time:
                if self._get_selected():
                    self._remove_active_hls_key(hls_key)
                    self.hls_manager.stop_hls_ffmpeg_process(hls_key, self.logical_channel_name)
                    return
                if process.poll() is not None:
                    if self._get_selected():
                        self._remove_active_hls_key(hls_key)
                        self.hls_manager.stop_hls_ffmpeg_process(hls_key, self.logical_channel_name)
                        return
                    raise ChildProcessError(f"FFmpeg process (PID: {process.pid}) exited prematurely.")
                if any(channel_hls_dir.glob('*.ts')):
                    provider_streams = self.handler.get_active_stream_status_for_logging(provider_alias)
                    self.config.log_message(f"[{provider_alias}:{provider_streams}] FFmpeg for '{hls_name}' (PID: {process.pid}) is now healthy.", level="INFO")
                    return hls_key
                time.sleep(STARTUP_POLL_INTERVAL)
            
            raise TimeoutError(f"FFmpeg for {hls_name} (PID: {process.pid}) timed out waiting for segments.")

        except (ChildProcessError, TimeoutError) as e:
            self.config.log_message(f"Validation failed for {hls_name}: {e}. Cleaning up.", level="ERROR")
            self._remove_active_hls_key(hls_key)
            self.hls_manager.stop_hls_ffmpeg_process(hls_key, self.logical_channel_name)
            return

    def _process_results(self) -> None:
        """Processes results from the threads and selects the best stream."""
        try:
            while time.monotonic() < self._get_deadline():
                if any(thread.is_alive() for thread in self._threads):
                    time.sleep(0.1)
                else:
                    break
    
            with self._mutex:
                if not self._results:
                    self._res = (503, f"Failed to start HLS stream for '{self.logical_channel_name}' from any source.")
                    return
                hls_key = min(self._results, key=lambda x: self._remaining_priorities[x[1]["source_service_id"]])[0]
                self._res = hls_key
                self._selected = True
                self.hls_manager.set_ffmpeg_process_long_term(hls_key, True)
                threading.Thread(target=self.hls_manager.stop_hls_ffmpeg_processes, args=([k for k in self._active_hls_keys if k != hls_key],), daemon=True).start()

            self.config.log_message(f"Selected source {hls_key} for '{self.logical_channel_name}'.", level="INFO")
        finally:
            self._result_mutex.release()

    def result(self) -> HLSKey | tuple[int, str]:
        """
        Blocks until the stream is created or an error occurs.
        If successful, returns an HLSKey.
        If failed, returns a tuple with an error code and message.
        """
        with self._result_mutex:
            return self._res
