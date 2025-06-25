from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import shutil
import subprocess
import threading
import time
from typing import IO, Any

from nexus_stream.config import CREATE_STREAM_DEADLINE, NEW_DEADLINE_NON_BEST, Config, VideoKey, VideoName, VideoType
from nexus_stream.quality_monitor import QualityMonitor
from nexus_stream.slots import ProviderName, ProviderSlots
from nexus_stream.stream import ChannelHandler, StreamManager


CREATE_STREAM_POLL_INTERVAL = 0.01
MPEGTS_PACKET_SIZE = 188       # Size of a single MPEG-TS packet in bytes
MPEGTS_PACKETS_PER_CHUNK = 21  # Number of packets to read at once in the MPEG-TS stream


def create_video_key(logical_channel_id: str, source_service_id: str, video_type: VideoType) -> VideoKey:
    """Generates a unique key for the stream."""
    return VideoKey(f"{video_type}_{logical_channel_id}_{source_service_id}")


def create_video_name(logical_channel_name: str, source_service_id: str, video_type: VideoType) -> VideoName:
    """Generates a unique name for the stream."""
    return VideoName(f"[{video_type}] {logical_channel_name} - {source_service_id}")


def create_hls_ffmpeg_command(stream_manager: StreamManager, config: Config, input_url: str, video_key: VideoKey) -> tuple[list[str], Path]:
    """Constructs the FFmpeg command list and creates the necessary HLS directory."""
    channel_hls_dir = stream_manager.hls_base_dir / config.get_fs_safe_alphanum(video_key)
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


def create_mpegts_ffmpeg_command(config: Config, input_url: str) -> list[str]:
    """Constructs the FFmpeg command list to output a continuous MPEG-TS stream."""
    command = [
        config.ffmpeg_path,
        "-hide_banner", "-loglevel", "info",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4",
        "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
        "-user_agent", "NexusStream/1.0 (FFMPEG-MPEGTS)",
        "-i", input_url,
        "-c:v", "copy",
        "-c:a", "libmp3lame",
        "-map", "0:v:0?", "-map", "0:a:0?",
        "-f", "mpegts",
        "pipe:1"
    ]
    return command


def sort_sources(sources: list[dict[str, Any]], quality_scores: dict[str, dict[str, float]], *, reverse: bool) -> dict[str, int]:
    """Sorts the input sources in based on their manual priority and quality scores.
    Returns a mapping of source_service_id to their priority (the input is sorted in place so this is not needed most of the time).
    """
    prev_score = None
    curr_priority = -1
    source_scores = sorted(((source["priority"],
                             -quality_scores.get(source["source_service_id"], {}).get("total_score", 0),
                             -quality_scores.get(source["source_service_id"], {}).get("uptime", 0),
                             source["source_service_id"]),
                    source["source_service_id"]) for source in sources)
    source_priorities: dict[str, int] = {}
    for score, source_service_id in source_scores:
        if score != prev_score:
            prev_score = score
            curr_priority += 1
        source_priorities[source_service_id] = curr_priority
    sources.sort(key=lambda x: source_priorities[x["source_service_id"]], reverse=reverse)  # Highest priority last since we use `pop()`
    return source_priorities


class CreateStream:
    """
    A class to acquire and use resources for creating a stream.
    Multiple instances of this class can run in parallel regardless
    of input parameters.
    """

    def __init__(self, config: Config, handler: ChannelHandler, stream_manager: StreamManager, quality_monitor: QualityMonitor, logical_channel_id: str, logical_channel_name: str, video_type: VideoType, input_sources: list[dict[str, Any]] | None = None) -> None:
        self.config = config
        self.handler = handler
        self.stream_manager = stream_manager
        self.quality_monitor = quality_monitor
        self.video_type = video_type
        self._res: VideoKey | tuple[int, str] = (500, f"[{video_type}] Stream not created yet")  # Final result of the stream, accessed via `result()`
        self._result_mutex = threading.Lock()  # Used to block `result()` until the stream is created or an error occurs
        self._mutex = threading.Lock()  # Used to synchronize access to shared state within this instance for all the threads
    
        self.logical_channel_id = logical_channel_id
        self.logical_channel_name = logical_channel_name
        self._sources = self.handler.get_sources_for_client_facing_channel(logical_channel_id) if input_sources is None else deepcopy(input_sources)
        if not self._sources:
            self._res = (404, f"[{video_type}] Logical channel '{logical_channel_name}' ({logical_channel_id}) not found or has no sources.")
            return

        # Prioritize sources based on manual priority then quality score
        # Convert these into a integer 0, 1, 2, ... relative to these sources
        # Ones that are exactly equal to have the same number, lower is better
        self._quality_scores = self.quality_monitor.get_quality_scores()
        self._remaining_priorities = sort_sources(self._sources, self._quality_scores, reverse=True)  # Highest priority last since we use `pop()`

        self._results: list[tuple[VideoKey, dict[str, Any]]] = []  # All the successful streams that we will choose from for self._res
        self._selected = False  # If a stream has been chosen, stop the other threads early
        self._active_video_keys: list[VideoKey] = []  # All the streams that we have started and will stop (exculding the one that is selected)
        self._source_quality_messages: dict[str, str] = {}  # Used for logging the quality of each source
        self._video_names: dict[VideoKey, VideoName] = {}  # Maps video keys to their names for logging
        self._deadline = time.monotonic() + CREATE_STREAM_DEADLINE  # Maximum deadline for this entire process
        self._threads: list[threading.Thread] = []  # Used to end before deadline if all sources are exhausted
        all_provider_sources: dict[ProviderName, list[dict[str, Any]]] = {}
        for source in self._sources:
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
                new_deadline = time.monotonic() + NEW_DEADLINE_NON_BEST
            self._deadline = min(self._deadline, new_deadline)

    def _set_result(self, video_key: VideoKey, source: dict[str, Any]) -> bool:
        with self._mutex:
            if self._selected:
                return False  # Don't bother setting result if we already selected a stream
            self._results.append((video_key, source))
            return True

    def _remove_active_video_key(self, video_key: VideoKey) -> None:
        with self._mutex:
            self._active_video_keys.remove(video_key)

    def _pop_source(self, provider_sources: list[dict[str, Any]], current_source: dict[str, Any] | None) -> dict[str, Any] | None:
        with self._mutex:
            if current_source:  # This source has failed, update what the best remaining source is
                self._remaining_priorities.pop(current_source["source_service_id"])
            if provider_sources:
                return provider_sources.pop()
            return None

    def _create_provider_stream(self, provider_alias: ProviderName, provider_sources: list[dict[str, Any]]) -> None:
        """Create streams for a specific provider using all sources."""
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            self.config.log_message(f"{self.logical_channel_name}: Provider '{provider_alias}' does not exist.", level="CRITICAL")
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
        video_key = create_video_key(self.logical_channel_id, source["source_service_id"], self.video_type)
        while time.monotonic() < self._get_deadline():
            if self._get_selected():
                return
            self.stream_manager.stream_process_lock.acquire()  # We don't want to release this lock too early since we need to reacquire it to create a stream
            if video_key in self.stream_manager.ffmpeg_processes and self.stream_manager.ffmpeg_processes[video_key]["process"].poll() is None:  # Process already exists
                if not self.stream_manager.ffmpeg_processes[video_key]["is_long_term"]:
                    self.stream_manager.stream_process_lock.release()
                    time.sleep(CREATE_STREAM_POLL_INTERVAL)
                    continue  # Short term streams can be killed at any time
                self.stream_manager.stream_process_lock.release()
                if not self._set_result(video_key, source):
                    return  # Don't stop process since it's a long running process owned elsewhere
                self._set_deadline(source)
                return

            # This ensure we are respecting the global available slots, it is released by `self._create_stream()`
            if not provider_slots.acquire(blocking=False):
                self.stream_manager.stream_process_lock.release()
                self.stream_manager.prune_ffmpeg_processes()  # If any inactive streams, prune them so we can start this. Allows a user to switch channels.
                time.sleep(CREATE_STREAM_POLL_INTERVAL)
                continue
            
            video_key = self._create_stream(video_key, provider_alias, provider_slots, source)
            if video_key:
                if not self._set_result(video_key, source):
                    self.stream_manager.stop_ffmpeg_process(video_key, self._video_names[video_key])
                    return
                self._set_deadline(source)
                return

            # Source failed, try the next one
            source = self._pop_source(provider_sources, source)
            if not source:
                return
            video_key = create_video_key(self.logical_channel_id, source["source_service_id"], self.video_type)

    def _check_mpegts_ffmpeg_health(self, video_name: VideoName, stream_info: str, stdout: IO[bytes], is_healthy: list[bool | None], mpegts_mtx: threading.Lock) -> None:
        """Tries to read from stdout, blocks until data is available or the stream ends."""
        error: Exception | str = "no data received or timed out"
        try:
            if stdout.read(MPEGTS_PACKET_SIZE):
                with mpegts_mtx:
                    is_healthy[0] = True
                    return
        except Exception as e:
            error = e
        self.config.log_message(f"{video_name} {stream_info}: Error reading from FFmpeg MPEG-TS stream: {error}", level="error")
        with mpegts_mtx:
            is_healthy[0] = False

    def _create_stream(self, video_key: VideoKey, provider_alias: ProviderName, provider_slots: ProviderSlots, source: dict[str, Any]) -> VideoKey | None:
        """Creates a stream using the provided source and provider slots."""
        try:
            video_name = create_video_name(self.logical_channel_name, source["source_service_id"], self.video_type)
            self._video_names[video_key] = video_name
            quality_score = self._quality_scores.get(source["source_service_id"])
            score_msg = f"Score={quality_score['total_score']:.2f} | Uptime={quality_score['uptime']*100:.0f}%" if quality_score else "Score=Unknown | Uptime=Unknown"
            stream_info = f"[Priority={source['priority']} | {score_msg}]"
            self._source_quality_messages[video_key] = stream_info

            channel_hls_dir = None
            stderr_log_file = None
            try:
                if self.video_type == VideoType.HLS:
                    command, channel_hls_dir = create_hls_ffmpeg_command(self.stream_manager, self.config, source["actual_stream_url"], video_key)
                elif self.video_type == VideoType.MPEGTS:
                    command = create_mpegts_ffmpeg_command(self.config, source["actual_stream_url"])
                else:
                    raise ValueError(f"Unsupported video type: {self.video_type}")
                log_path = self.config.get_ffmpeg_log_path(video_name, self.video_type)
                stderr_log_file = open(log_path, 'a', encoding='utf-8')
            except Exception as e:
                self.config.log_message(f"{video_name} {stream_info}: Failed to create FFmpeg command: {e}", level="CRITICAL")
                provider_slots.release()
                if stderr_log_file:
                    try:
                        stderr_log_file.close()
                    except Exception as e:
                        self.config.log_message(f"{video_name} {stream_info}: Failed to close log file: {e}", level="CRITICAL")
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
                            self.config.log_message(f"{video_name} {stream_info}: Failed to close log file: {e}", level="CRITICAL")
                        if channel_hls_dir:
                            shutil.rmtree(channel_hls_dir, ignore_errors=True)
                        return
                    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=stderr_log_file)
                    self._active_video_keys.append(video_key)
                    self.stream_manager.ffmpeg_processes[video_key] = {
                        'process': process,
                        'is_long_term': False,
                        'is_preview': self.logical_channel_id.startswith("preview_"),
                        'video_type': self.video_type,
                        'provider_alias': provider_alias,
                        'logical_channel_id': self.logical_channel_id,
                        'source_service_id': source["source_service_id"],
                        'logical_channel_name': self.logical_channel_name,
                        'channel_hls_dir': channel_hls_dir,
                        'last_access': datetime.now(),
                        'is_mpegts_active': False,
                        'stderr_log_file_obj': stderr_log_file
                    }
            except Exception as e:
                self.config.log_message(f"{video_name} {stream_info}: Immediate Popen failure: {e}", level="CRITICAL")
                provider_slots.release()
                return
        finally:
            self.stream_manager.stream_process_lock.release()
        provider_streams = self.handler.get_active_stream_status_for_logging(provider_alias)
        self.config.log_message(f"{video_name} {stream_info}: Claimed a '{provider_alias}' slot and started FFmpeg (PID: {process.pid}) {{{provider_alias}:{provider_streams}}}.", level="INFO")
        is_healthy: list[bool | None] = [None]
        mpegts_mtx = threading.Lock()
        if self.video_type == VideoType.MPEGTS:
            threading.Thread(target=self._check_mpegts_ffmpeg_health, args=(video_name, stream_info, process.stdout, is_healthy, mpegts_mtx), daemon=True).start()

        try:
            end_time = time.monotonic() + self.config.ffmpeg_start_timeout
            while time.monotonic() < end_time:
                if self._get_selected():
                    self._remove_active_video_key(video_key)
                    self.stream_manager.stop_ffmpeg_process(video_key, video_name)
                    return
                if process.poll() is not None:
                    if self._get_selected():
                        self._remove_active_video_key(video_key)
                        self.stream_manager.stop_ffmpeg_process(video_key, video_name)
                        return
                    raise ChildProcessError(f"exited prematurely")
                if self.video_type == VideoType.HLS:
                    if any(channel_hls_dir.glob('*.ts')):  # type: ignore
                        provider_streams = self.handler.get_active_stream_status_for_logging(provider_alias)
                        self.config.log_message(f"{video_name} {stream_info}: FFmpeg stream is now healthy (PID: {process.pid})", level="INFO")
                        return video_key
                elif self.video_type == VideoType.MPEGTS:
                    with mpegts_mtx:
                        res = is_healthy[0]
                    if res is True:
                        provider_streams = self.handler.get_active_stream_status_for_logging(provider_alias)
                        self.config.log_message(f"{video_name} {stream_info}: FFmpeg stream is now healthy (PID: {process.pid})", level="INFO")
                        return video_key
                    elif res is False:
                        if self._get_selected():
                            self._remove_active_video_key(video_key)
                            self.stream_manager.stop_ffmpeg_process(video_key, video_name)
                            return
                        raise ChildProcessError("exited prematurely")
                else:
                    raise ValueError(f"Unsupported video type: {self.video_type}")
                time.sleep(CREATE_STREAM_POLL_INTERVAL)
            
            raise TimeoutError("timed out waiting for segments or process stability")

        except (ChildProcessError, TimeoutError) as e:
            self.config.log_message(f"{video_name} {stream_info}: FFmpeg validation failed (PID: {process.pid}): {e}. Cleaning up.", level="ERROR")
            self._remove_active_video_key(video_key)
            self.stream_manager.stop_ffmpeg_process(video_key, video_name)
            return

    def _process_results(self) -> None:
        """Processes results from the threads and selects the best stream."""
        try:
            while time.monotonic() < self._get_deadline():
                if any(thread.is_alive() for thread in self._threads):
                    time.sleep(CREATE_STREAM_POLL_INTERVAL)
                else:
                    break
    
            with self._mutex:
                if not self._results:
                    self._res = (503, f"[{self.video_type}] {self.logical_channel_name}: Failed to start {self.video_type} stream from any source.")
                    return
                video_key = min(self._results, key=lambda x: self._remaining_priorities[x[1]["source_service_id"]])[0]
                self._res = video_key
                self._selected = True
                self.stream_manager.set_ffmpeg_process_long_term(video_key, True)
                keys_to_stop = tuple(k for k in self._active_video_keys if k != video_key)
                if keys_to_stop:
                    threading.Thread(target=self.stream_manager.stop_ffmpeg_processes, args=(keys_to_stop,), daemon=True).start()

            self.config.log_message(f"{self._video_names[video_key]} {self._source_quality_messages[video_key]}: Selected as the best stream from {len(self._results)} tested and healthy sources (Total: {len(self._sources)} sources)", level="INFO")
        finally:
            self._result_mutex.release()

    def result(self) -> VideoKey | tuple[int, str]:
        """
        Blocks until the stream is created or an error occurs.
        If successful, returns an VideoKey.
        If failed, returns a tuple with an error code and message.
        """
        with self._result_mutex:
            return self._res
