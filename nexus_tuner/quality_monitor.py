import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Coroutine, Final, Any, Literal, Self, cast

import math
import numpy as np
import random
import soundfile as sf  # type: ignore
from scipy.signal import butter, fftconvolve, sosfiltfilt, stft  # type: ignore

from nexus_tuner.config import Config
from nexus_tuner.handler import ChannelHandler
from nexus_tuner.slots import ProviderSlots
from nexus_tuner.utils import (FAILED_STOP_REASONS, PROCESS_TERMINATE_INTERVAL, PROCESS_TERMINATE_TIMEOUT, NEXUS_TUNER_USER_AGENT, Bitrate, BitrateScore, ChannelNum, DateTimeISO, Framerate, FramerateScore, Height,
                                Label, Log, LogicalChannelId, Offset, ProbeInfo, ProbeSuccess, ProviderAlias, QualityInfoImpl, QualityProcessInfos, QualityProcessInfosImpl, QualityScore, QualityScores,
                                QualityScoresImpl, ResolutionScore, QualityCacheData, QualityCacheDataImpl, Runtime, RuntimeInfo, RuntimeScore, SourceId, SourceMappingInfoMutable, StopReason, StreamName,
                                StreamURL, TVGDisplayTitle, TVGLogo, TVGName, TotalScore, Uptime, UptimeScore, VideoKey, VideoName, Width, create_stream_name, create_video_name, run_bg)

if TYPE_CHECKING:
    from nexus_tuner.stream import StreamManager

# --- Constants ---
RESOLUTION_WEIGHT: Final[int] = 50
BITRATE_WEIGHT: Final[int] = 30
FRAMERATE_WEIGHT: Final[int] = 20
UPTIME_WEIGHT: Final[int] = 0
RUNTIME_WEIGHT: Final[int] = 0

RESOLUTION_NORM: Final[int] = 2160
BITRATE_NORM: Final[int] = 12_000_000
FRAMERATE_NORM: Final[int] = 60
RUNTIME_NORM: Final[int] = 60 * 60 * 12

BACKGROUND_SLOT_WAIT_INTERVAL: Final[int] = 1
QUALITY_MONITOR_TIMEOUT: Final[int] = 5
QUALITY_MONITOR_GRACE: Final[int] = 5
QUALITY_MONITOR_DELAY: Final[int] = 30
MAX_HISTORY_PER_SOURCE: Final[int] = 10
MIN_DAYS_AT_MAX_HISTORY: Final[int] = 7
MIN_DAYS_AT_NON_MAX_HISTORY: Final[int] = 1

OFFSET_SR: Final[int] = 16000
OFFSET_HOP: Final[int] = 256
OFFSET_FRAME_LEN = 1024
OFFSET_CLIP_DURATION = 5
OFFSET_GRACE = 1
OFFSET_WAIT_INTERVAL = 120
OFFSET_CONF_THRESH: Final[float] = 0.8  # Should equate to a few seconds off the true value
OFFSET_ROUND_VARIANCE_WARN: Final[float] = 2.0
OFFSET_ANALYSIS_WINDOW: Final[float] = 2.5
OFFSET_ANALYSIS_STEP: Final[float] = 0.5
OFFSET_MATCH_CLUSTER_TOLERANCE: Final[float] = 0.125
OFFSET_FINE_SEARCH_PAD: Final[float] = 3.0
OFFSET_RETRY_INTERVAL: Final[int] = 5
OFFSET_RETRY_TIMEOUT: Final[int] = 60 * 60

OFFSET_BANDPASS_SOS: np.ndarray = cast(np.ndarray, butter(4, (80, 3500), btype="bandpass", fs=OFFSET_SR, output="sos"))


class QualityMonitor:
    __slots__ = ('config', 'handler', 'stream_manager', 'processes', '_running', '_rate_limited',
                 '_full_providers', '_mutex', 'quality_process_lock', '_quality_scores')
    
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self.stream_manager: StreamManager  # Injected after creation
        self.processes: QualityProcessInfos = QualityProcessInfosImpl({})
        self._running: LogicalChannelId | StreamName | Literal["scheduler"] | None = None
        self._rate_limited: bool = False
        self._full_providers: dict[ProviderAlias, bool] = {}
        self._mutex: asyncio.Lock = asyncio.Lock()
        self.quality_process_lock: asyncio.Lock = asyncio.Lock()
        self._quality_scores: QualityScores = QualityScoresImpl({})

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler) -> Self:
        """Asynchronous factory for creating and initializing a QualityMonitor instance."""
        instance = cls(config, handler)
        quality_cache = await config.get_quality_cache(label=Label.STARTUP)
        if quality_cache:
            instance._build_quality_scores(quality_cache)
        return instance

    async def get_quality_score(self, source_id: SourceId) -> QualityScore | None:
        """Returns the current quality score for a specific source."""
        async with self._mutex:
            return self._quality_scores.get(source_id)

    async def get_quality_scores(self) -> QualityScores:
        """Returns the current quality scores for all sources."""
        async with self._mutex:
            return QualityScoresImpl({**self._quality_scores})

    async def reload_quality_scores(self) -> None:
        """Reloads the quality scores from the configuration."""
        async with self._mutex:
            quality_cache = await self.config.get_quality_cache()
            if not quality_cache:
                Log.critical(Label.QUALITY, "Quality cache is missing or corrupted, cannot reload quality scores.")
                return
            self._build_quality_scores(quality_cache)

    def create_default_entry(self, quality_cache: QualityCacheDataImpl, source_id: SourceId) -> None:
        """Creates a default quality entry for a source."""
        quality_cache[source_id] = QualityInfoImpl({
            "updated_at": DateTimeISO(datetime.now().isoformat()), "statuses": [], "widths": [],
            "heights": [], "bitrates": [], "framerates": [], "runtimes": []
        })

    def trim_entry(self, source_entry: QualityInfoImpl) -> None:
        """Trims the history of a quality entry to the maximum allowed length."""
        source_entry["statuses"] = source_entry["statuses"][-MAX_HISTORY_PER_SOURCE:]
        source_entry["widths"] = source_entry["widths"][-MAX_HISTORY_PER_SOURCE:]
        source_entry["heights"] = source_entry["heights"][-MAX_HISTORY_PER_SOURCE:]
        source_entry["bitrates"] = source_entry["bitrates"][-MAX_HISTORY_PER_SOURCE:]
        source_entry["framerates"] = source_entry["framerates"][-MAX_HISTORY_PER_SOURCE:]
        source_entry["runtimes"] = source_entry["runtimes"][-MAX_HISTORY_PER_SOURCE:]

    async def remove_source(self, source_id: SourceId) -> bool:
        """Removes a source from the quality scores and cache."""
        async with self._mutex:
            quality_cache = await self.config.get_quality_cache()
            if quality_cache is None:
                Log.critical(Label.QUALITY, f"Quality cache was removed/corrupted after startup, cannot remove {source_id}.")
                return False
            if source_id in quality_cache:
                del quality_cache[source_id]
                if not await self.config.save_quality_cache(quality_cache):
                    Log.critical(Label.QUALITY, f"Failed to save quality cache after removing {source_id}.")
                    return False
            else:
                Log.debug(Label.QUALITY, f"Source {source_id} not found in quality cache, nothing to remove.")
            if source_id in self._quality_scores:
                new_quality_scores = QualityScoresImpl({**self._quality_scores})
                del new_quality_scores[source_id]
                self._quality_scores = new_quality_scores
            else:
                Log.debug(Label.QUALITY, f"Source {source_id} not found in quality scores, nothing to remove.")
            return True

    async def update_source_id(self, old_source_id: SourceId, new_source_id: SourceId) -> bool:
        """Replaces an old source ID with a new one in the quality scores and cache."""
        async with self._mutex:
            quality_cache = await self.config.get_quality_cache()
            if quality_cache is None:
                Log.critical(Label.QUALITY, f"Quality cache was removed/corrupted after startup, cannot replace {old_source_id} with {new_source_id}.")
                return False
            if old_source_id in quality_cache:
                quality_cache[new_source_id] = quality_cache.pop(old_source_id)
                if not await self.config.save_quality_cache(quality_cache):
                    Log.critical(Label.QUALITY, f"Failed to save quality cache after replacing {old_source_id} with {new_source_id}.")
                    return False
            else:
                Log.debug(Label.QUALITY, f"Source {old_source_id} not found in quality cache, cannot replace with {new_source_id}.")
            if old_source_id in self._quality_scores:
                new_quality_scores = QualityScoresImpl({**self._quality_scores})
                new_quality_scores[new_source_id] = new_quality_scores.pop(old_source_id)
                self._quality_scores = new_quality_scores
            else:
                Log.debug(Label.QUALITY, f"Source {old_source_id} not found in quality scores, cannot replace with {new_source_id}.")
            return True

    async def _cleanup_process(self, process: asyncio.subprocess.Process | None, provider_slots: ProviderSlots, source_id: SourceId, video_name: VideoName) -> None:
        """Ensures the subprocess is properly terminated and resources are released. Call with run_bg() to prevent asyncio.CancelledError from interrupting cleanup."""
        loop = asyncio.get_running_loop()
        try:
            if process and process.returncode is None:
                try:
                    process.terminate()
                    if process.stdout:
                        process.stdout._transport.close()  # type: ignore[reportAttributeAccessIssue]
                    await asyncio.wait_for(process.wait(), timeout=PROCESS_TERMINATE_TIMEOUT)
                    Log.debug(Label.QUALITY, f"{video_name}: process terminated successfully.")
                except asyncio.TimeoutError:
                    Log.warn(Label.QUALITY, f"{video_name}: Killing unresponsive process.")
                    process.kill()
                except Exception as e:
                    Log.error(Label.QUALITY, f"{video_name}: Error terminating process - {e}")
                    process.kill()
            end_time = loop.time() + PROCESS_TERMINATE_TIMEOUT
            while process and process.returncode is None and loop.time() < end_time:
                await asyncio.sleep(PROCESS_TERMINATE_INTERVAL)
            if process and process.returncode is None:
                Log.critical(Label.STREAM, f"{video_name}: process was not terminated properly, cannot release slot.")
                return
            async with self.quality_process_lock:
                cast(QualityProcessInfosImpl, self.processes).pop(source_id, None)
            run_bg(provider_slots.release())
        except BaseException as e:
            Log.critical(Label.QUALITY, f"{video_name}: Error stopping process, cannot release slot - {e}")
            raise

    async def _get_stream_info(self, stream_url: StreamURL, provider_slots: ProviderSlots, provider_alias: ProviderAlias, source_id: SourceId, tvg_logo: TVGLogo, source_title: TVGDisplayTitle | TVGName, logical_channel_id: LogicalChannelId, channel_num: ChannelNum, video_name: VideoName) -> ProbeSuccess | None:
        """
        Extracts stream information using ffprobe, ensuring the subprocess is
        terminated on timeout or cancellation.
        """
        cmd: list[str] = [
            str(self.config.ffprobe_path),
            "-hide_banner", "-loglevel", "info",
            "-user_agent", NEXUS_TUNER_USER_AGENT,
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,r_frame_rate",
            "-show_entries", "packet=pts_time,size",
            "-read_intervals", f"%+{QUALITY_MONITOR_TIMEOUT}",
            "-of", "json",
            stream_url
        ]

        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            async with self.quality_process_lock:
                cast(QualityProcessInfosImpl, self.processes)[source_id] = {
                    "provider_alias": provider_alias,
                    "tvg_logo": tvg_logo,
                    "source_title": source_title,
                    "logical_channel_id": logical_channel_id,
                    "channel_num": channel_num,
                    "type": "Quality",
                    "started_at": datetime.now(),
                }

            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=QUALITY_MONITOR_TIMEOUT + QUALITY_MONITOR_GRACE)

            if process.returncode != 0:
                err = stderr.decode()
                Log.debug(Label.QUALITY, f"{video_name}: ffprobe failed with code {process.returncode} - {err}".replace(stream_url, "{{stream_url}}").strip())
                if "429 Too Many Requests" in err:
                    self._rate_limited = True
                    raise asyncio.CancelledError("Provider is rate limiting requests.")
                return
            info = json.loads(stdout)

        except asyncio.TimeoutError:
            Log.debug(Label.QUALITY, f"{video_name}: ffprobe timed out.")
            return
        except Exception as e:
            Log.error(Label.QUALITY, f"{video_name}: Failed to parse ffprobe output - {e}")
            return
        finally:
            run_bg(self._cleanup_process(process, provider_slots, source_id, video_name))

        stream = info.get('streams', [{}])[0]
        width: Width = Width(int(stream.get('width', 0)))
        height: Height = Height(int(stream.get('height', 0)))
        fr_str = stream.get('r_frame_rate', '0/1')
        nums = fr_str.split('/')
        framerate: Framerate = Framerate(float(nums[0]) / float(nums[1]) if len(nums) == 2 and nums[1] != '0' else float(nums[0]))

        packets = info.get('packets', [])
        if not packets:
            Log.debug(Label.QUALITY, f"{video_name}: No packets found in ffprobe output.")
            return

        sizes, times = zip(*((float(pkt['size']), float(pkt['pts_time'])) for pkt in packets))
        duration_s = max(times) - min(times)
        if duration_s <= 0:
            Log.debug(Label.QUALITY, f"{video_name}: Invalid duration {duration_s}.")
            return
        
        total_bytes = sum(sizes)
        bitrate: Bitrate = Bitrate((total_bytes * 8) / duration_s)

        return ProbeSuccess({"status": "online", "width": width, "height": height, "bitrate": bitrate, "framerate": framerate})

    async def _run_single_probe(self, provider_alias: ProviderAlias, stream_url: StreamURL, tvg_logo: TVGLogo, source_id: SourceId, source_title: TVGDisplayTitle | TVGName, logical_channel_id: LogicalChannelId, channel_num: ChannelNum, video_name: VideoName) -> tuple[SourceId, ProbeInfo, str]:
        """
        Probes a single stream, persistently trying to acquire a slot, and ensures
        all resources are cleaned up upon completion, failure, or cancellation.
        """
        try:
            paused = False
            while True:
                if self.handler.get_pending_stream_count() > 0:
                    if not paused:
                        Log.debug(Label.QUALITY, f"{video_name}: Pausing probe for pending user streams...")
                        paused = True
                    await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
                    continue
                if paused:
                    Log.debug(Label.QUALITY, f"{video_name}: Resuming probe after pending user streams.")
                    paused = False

                provider_slots = await self.handler.get_provider_slots(provider_alias)
                if not provider_slots:
                    msg = f"{video_name}: Provider slot manager for {provider_alias} not found, cannot run probe."
                    Log.error(Label.QUALITY, msg)
                    raise RuntimeError(msg)
                if provider_slots.get_total_slots() <= 0:
                    msg = f"{video_name}: Provider {provider_alias} is configured with 0 slots, cannot run probe."
                    Log.warn(Label.QUALITY, msg)
                    raise ValueError(msg)
                if self._rate_limited:
                    msg = f"{video_name}: Detected rate limit, cannot run probe."
                    raise RuntimeError(msg)
                if not await provider_slots.try_acquire():
                    await self.stream_manager.prune_processes(provider_alias)
                    if self._running != "scheduler" and not await self.stream_manager.is_provider_available_soon(provider_alias):
                        msg = f"{video_name}: Provider {provider_alias} is full of active streams, cannot run probe."
                        Log.debug(Label.QUALITY, msg)
                        raise RuntimeError(msg)
                    await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
                    continue

                task = asyncio.create_task(self._get_stream_info(stream_url, provider_slots, provider_alias, source_id, tvg_logo, source_title, logical_channel_id, channel_num, video_name))
                try:
                    provider_slots.add_background_task(task)
                    stream_info = await task
                except asyncio.CancelledError:
                    Log.debug(Label.QUALITY, f"{video_name}: ffprobe task was cancelled.")
                    if provider_slots.pop_cancelled_task(task):
                        continue  # Retry since we cancelled for a user stream
                    raise
                if not stream_info:
                    return source_id, {"status": "offline", "reason": "No stream info available"}, video_name
                return source_id, stream_info, video_name

        except asyncio.CancelledError:
            Log.info(Label.QUALITY, f"{video_name}: Probe task was cancelled by slot manager.")
            raise
        except Exception as e:
            Log.error(Label.QUALITY, f"{video_name}: Unexpected error during probe - {e}")
            return source_id, {"status": "offline", "reason": f"Probe failed: {e}"}, video_name
    
    async def analyze_mapped_sources(self, input_lc_id: LogicalChannelId | None = None) -> int | str:
        """Finds and probes all mapped sources concurrently."""
        if self._running:
            if not input_lc_id:
                Log.warn(Label.QUALITY, "Quality analysis is already running, cannot start another.")
            return self._running
        try:
            self._running = input_lc_id or "scheduler"
            loop = asyncio.get_running_loop()
            success_count = 0
            valid_mappings: list[tuple[DateTimeISO, LogicalChannelId, list[SourceId], StreamName, TVGLogo, ChannelNum]] = []
            if input_lc_id:
                logical_channel = await self.handler.get_logical_channel_by_id(input_lc_id)
                if not logical_channel:
                    Log.error(Label.QUALITY, f"Logical Channel ID {input_lc_id} not found.")
                    return success_count
                channel_num = logical_channel["channel_num"]
                stream_name = create_stream_name(logical_channel["logical_channel_title"], channel_num)
                self._running = stream_name
                Log.info(Label.QUALITY, f"{stream_name}: Starting stream quality analysis.")
                mappings = await self.handler.get_mappings_for_logical_channel(input_lc_id)
                if not mappings:
                    Log.error(Label.QUALITY, f"{stream_name}: No mapped sources found.")
                    return success_count
                valid_mappings.append((DateTimeISO("0001-01-01"), input_lc_id, [source_id for source_id in mappings], stream_name, logical_channel["tvg_logo"], channel_num))
            else:
                all_mappings = await self.handler.copy_channel_mappings_data()
                if not all_mappings:
                    Log.warn(Label.QUALITY, "No mapped sources to analyze.")
                    return success_count

                quality_cache = await self.config.get_quality_cache()
                if quality_cache is None:
                    Log.critical(Label.QUALITY, "Quality cache was removed/corrupted after startup, cannot analyze sources.")
                    return success_count
                now = datetime.now()
                for logical_channel_id, mappings in all_mappings.items():
                    logical_channel = await self.handler.get_logical_channel_by_id(logical_channel_id)
                    if not logical_channel:
                        Log.error(Label.QUALITY, f"Logical Channel ID {logical_channel_id} not found in mappings.")
                        continue
                    channel_num = logical_channel["channel_num"]
                    stream_name = create_stream_name(logical_channel["logical_channel_title"], channel_num)
                    if not mappings:
                        Log.debug(Label.QUALITY, f"{stream_name}: No valid sources found.")
                        continue
                    min_updated_at = min([quality_cache.get(source_id, {}).get("updated_at", "0001-01-01") for source_id in mappings])
                    at_max_history = all(len(quality_cache.get(source_id, {}).get("statuses", [])) >= MAX_HISTORY_PER_SOURCE for source_id in mappings)
                    delta = timedelta(days=MIN_DAYS_AT_MAX_HISTORY) if at_max_history else timedelta(days=MIN_DAYS_AT_NON_MAX_HISTORY)
                    if datetime.fromisoformat(min_updated_at) > now - delta:
                        continue
                    valid_mappings.append((min_updated_at, logical_channel_id, [source_id for source_id in mappings], stream_name, logical_channel["tvg_logo"], channel_num))
                if not valid_mappings:
                    Log.info(Label.QUALITY, "No sources are due for quality probing.")
                    return success_count
                valid_mappings.sort(key=lambda x: x[0])

            to_delay = False
            for _, logical_channel_id, source_ids, stream_name, tvg_logo, channel_num in valid_mappings:
                if to_delay:
                    await asyncio.sleep(QUALITY_MONITOR_DELAY)
                else:
                    to_delay = True
                tasks: list[Coroutine[Any, Any, tuple[SourceId, ProbeInfo, str]]] = []
                for source_id in source_ids:
                    discovered_source = await self.handler.get_discovered_source(source_id)
                    if not discovered_source:
                        Log.debug(Label.QUALITY, f"{stream_name} [{source_id}]: Not found in discovered sources.")
                        continue
                    source_title = discovered_source['display_title'] or discovered_source['tvg_name']
                    video_name = create_video_name(stream_name, source_title, source_id)
                    provider_slots = await self.handler.get_provider_slots(discovered_source["provider_alias"])
                    if not provider_slots:
                        Log.error(Label.QUALITY, f"{video_name}: Provider slots for {discovered_source['provider_alias']} not found while probing.")
                        continue
                    if provider_slots.get_total_slots() <= 0:
                        Log.warn(Label.QUALITY, f"{video_name}: Provider {provider_slots.get_alias()} is configured with 0 slots, skipping probing.")
                        continue
                    tasks.append(
                        self._run_single_probe(
                            discovered_source["provider_alias"],
                            discovered_source["stream_url"], 
                            discovered_source["tvg_logo"] or tvg_logo,
                            source_id,
                            source_title,
                            logical_channel_id,
                            channel_num,
                            video_name,
                        )
                    )
                if not tasks:
                    Log.debug(Label.QUALITY, f"{stream_name} No valid sources found to probe.")
                    continue

                raw_results = await asyncio.gather(*tasks, return_exceptions=True)
                if self._rate_limited:
                    Log.warn(Label.QUALITY, "Rate limited by provider, stopping further analysis.")
                    return success_count
                stream_infos: list[tuple[SourceId, ProbeInfo, str]] = []
                for raw_result in raw_results:
                    if isinstance(raw_result, BaseException):
                        if not isinstance(raw_result, asyncio.CancelledError) and not isinstance(raw_result, Exception):
                            Log.error(Label.QUALITY, f"{stream_name}: Error probing source - {raw_result}")
                        continue
                    stream_infos.append(raw_result)

                async with self._mutex:
                    quality_cache = await self.config.get_quality_cache()
                    if quality_cache is None:
                        Log.critical(Label.QUALITY, f"{stream_name}: Quality cache was removed/corrupted after startup, stopping analysis.")
                        return success_count
                    modified_cache = QualityCacheDataImpl({})
                    for source_id, probe_info, video_name in stream_infos:
                        if source_id not in quality_cache:
                            if not await self.handler.get_discovered_source(source_id):
                                Log.warn(Label.QUALITY, f"{video_name}: Not found in discovered sources, skipping.")
                                continue  # Dead mapping that was removed after the start of this analysis
                            self.create_default_entry(quality_cache, source_id)
                        source_entry = quality_cache[source_id]

                        source_entry["updated_at"] = DateTimeISO(datetime.now().isoformat())
                        if probe_info["status"] == "online":
                            source_entry["statuses"].append("online")
                            source_entry["widths"].append(probe_info["width"])
                            source_entry["heights"].append(probe_info["height"])
                            source_entry["bitrates"].append(probe_info["bitrate"])
                            source_entry["framerates"].append(probe_info["framerate"])
                            if input_lc_id:
                                Log.info(Label.QUALITY, f"{video_name}: Online - {probe_info['width']}x{probe_info['height']} @ {probe_info['framerate']:.2f}fps, {probe_info['bitrate'] / 1_000_000:.2f}mbps")
                        else:
                            source_entry["statuses"].append("offline")
                            if input_lc_id:
                                Log.warn(Label.QUALITY, f"{video_name}: Offline")

                        self.trim_entry(source_entry)
                        modified_cache[source_id] = source_entry
                    if not await self.config.save_quality_cache(quality_cache):
                        Log.critical(Label.QUALITY, f"{stream_name}: Failed to save quality cache, stopping analysis.")
                        return success_count
                    self._build_quality_scores(modified_cache)
            success_count += 1
            if input_lc_id:
                Log.debug(Label.QUALITY, f"{valid_mappings[0][3]}: Measuring relative source offsets...")
                end_time = loop.time() + OFFSET_RETRY_TIMEOUT
                while loop.time() < end_time:
                    res = await self.measure_relative_offset_for_channel(input_lc_id)
                    if res:
                        success_count += 1
                        Log.info(Label.QUALITY, f"{valid_mappings[0][3]}: Completed analysis for {len(valid_mappings[0][2])} mappings(s).")
                        break
                    if all(self._full_providers.values()):
                        Log.warn(Label.QUALITY, f"{valid_mappings[0][3]}: Cannot measure offsets because all providers are full of active streams.")
                        break
                    if res is None:
                        break
                    Log.warn(Label.QUALITY, f"{valid_mappings[0][3]}: Retrying offset measurement... ({int(end_time - loop.time())}s until timeout)")
                    await asyncio.sleep(OFFSET_RETRY_INTERVAL)
                else:
                    Log.error(Label.QUALITY, f"{valid_mappings[0][3]}: Failed to measure relative source offsets within {OFFSET_RETRY_TIMEOUT}s.")
                    return success_count
            return success_count
        finally:
            self._running = None
            self._rate_limited = False
            self._full_providers.clear()

    async def append_runtime(self, video_name: VideoName, source_id: SourceId, started_at: datetime, stopped_at: datetime, stop_reason: StopReason) -> None:
        """Appends runtime information for a source."""
        runtime_info = RuntimeInfo({
            "stop_reason": stop_reason,
            "runtime": Runtime((stopped_at - started_at).total_seconds())
        })
        runtime_info_log = f"{video_name} {{'stop_reason': '{stop_reason}', 'runtime': {runtime_info['runtime']}}}"
        async with self._mutex:
            quality_cache = await self.config.get_quality_cache()
            if quality_cache is None:
                Log.critical(Label.QUALITY, f"{runtime_info_log}: Failed to get quality cache for new runtime.")
                return
            if source_id not in quality_cache:
                if not await self.handler.get_discovered_source(source_id):
                    Log.warn(Label.QUALITY, f"{runtime_info_log}: Not found in discovered sources when adding new runtime.")
                    return
                self.create_default_entry(quality_cache, source_id)
            source_entry = quality_cache[source_id]
            if source_entry["runtimes"] and source_entry["runtimes"][-1]["stop_reason"] not in FAILED_STOP_REASONS:
                prev_runtime_info = source_entry["runtimes"].pop()
                Log.debug(Label.QUALITY, f"{runtime_info_log}: Merging with previous runtime {prev_runtime_info}.")
                runtime_info = RuntimeInfo({
                    "stop_reason": stop_reason,
                    "runtime": Runtime(prev_runtime_info["runtime"] + runtime_info["runtime"])
                })
                runtime_info_log = f"{video_name} {{'stop_reason': {stop_reason}, 'runtime': {runtime_info['runtime']}}}"
            source_entry["runtimes"].append(runtime_info)
            self.trim_entry(source_entry)
            if not await self.config.save_quality_cache(quality_cache):
                Log.critical(Label.QUALITY, f"{runtime_info_log}: Failed to save quality cache when adding new runtime.")
                return
            self._build_quality_scores(QualityCacheDataImpl({source_id: source_entry}))
            Log.debug(Label.QUALITY, f"{runtime_info_log}: Updated last runtime.")

    def _build_quality_scores(self, quality_cache: QualityCacheData) -> None:
        """Calculates quality scores and updates the internal state."""
        new_quality_scores = QualityScoresImpl({**self._quality_scores})
        for source_id, source_entry in quality_cache.items():
            avg_width = Width(sum(source_entry["widths"]) / len(source_entry["widths"]) if source_entry["widths"] else 0)
            avg_height = Height(sum(source_entry["heights"]) / len(source_entry["heights"]) if source_entry["heights"] else 0)
            avg_bitrate = Bitrate(sum(source_entry["bitrates"]) / len(source_entry["bitrates"]) if source_entry["bitrates"] else 0)
            avg_framerate = Framerate(sum(source_entry["framerates"]) / len(source_entry["framerates"]) if source_entry["framerates"] else 0)
            uptime = Uptime(sum(1 for s in source_entry["statuses"] if s == "online") / len(source_entry["statuses"]) if source_entry["statuses"] else 0)
            
            height_score = ResolutionScore(RESOLUTION_WEIGHT * min(avg_height / RESOLUTION_NORM, 1.0))
            bitrate_score = BitrateScore(BITRATE_WEIGHT * min(avg_bitrate / BITRATE_NORM, 1.0))
            framerate_score = FramerateScore(FRAMERATE_WEIGHT * min(avg_framerate / FRAMERATE_NORM, 1.0))
            uptime_score = UptimeScore(UPTIME_WEIGHT * uptime)

            # Think of the avg_runtime as total_play_duration / total_num_failures
            # The reason why we don't store that is to only consider recent history rather than the entire lifetime
            if any(r["stop_reason"] in FAILED_STOP_REASONS for r in source_entry["runtimes"]):
                if source_entry["runtimes"][-1]["stop_reason"] in FAILED_STOP_REASONS:
                    avg_runtime = Runtime(sum(r["runtime"] for r in source_entry["runtimes"]) / len(source_entry["runtimes"]))
                else:
                    avg_runtime = Runtime(sum(r["runtime"] for r in source_entry["runtimes"]) / (len(source_entry["runtimes"])-1))
                runtime_score = RuntimeScore(RUNTIME_WEIGHT * min(avg_runtime / RUNTIME_NORM, 1.0))
            else:
                avg_runtime = None
                runtime_score = RuntimeScore(RUNTIME_WEIGHT)  # Default to max so all streams eventually get a score

            new_quality_scores[source_id] = {
                "width": avg_width, "height": avg_height, "bitrate": avg_bitrate,
                "framerate": avg_framerate, "runtime": avg_runtime, "uptime": uptime,
                "resolution_score": height_score, "bitrate_score": bitrate_score,
                "framerate_score": framerate_score, "uptime_score": uptime_score, "runtime_score": runtime_score,
                "total_score": TotalScore(height_score + bitrate_score + framerate_score + uptime_score + runtime_score),
            }
        self._quality_scores = new_quality_scores

    def _load_audio(self, path: Path) -> np.ndarray:
        data, sr = sf.read(path, dtype='float32')  # type: ignore
        if sr != OFFSET_SR:
            raise ValueError(f"Expected {OFFSET_SR}Hz audio, got {sr}Hz")
        y = np.asarray(data, dtype=np.float32)
        if y.ndim > 1:
            y = np.mean(y, axis=1, dtype=np.float32)
        return cast(np.ndarray, np.atleast_1d(np.nan_to_num(y, nan=0.0, posinf=0.0, neginf=0.0)).astype(np.float32, copy=False))

    def _prepare_alignment_audio(self, samples: np.ndarray) -> np.ndarray:
        y = np.asarray(samples, dtype=np.float32)
        if y.size == 0:
            return y
        y = y - float(np.mean(y))
        if y.size >= OFFSET_FRAME_LEN * 2:
            y = sosfiltfilt(OFFSET_BANDPASS_SOS, y).astype(np.float32)
        y = np.diff(y, prepend=y[0]).astype(np.float32)
        norm = float(np.sqrt(np.mean(y * y) + 1e-12))
        if norm <= 1e-9:
            return y
        return y / norm

    def _build_alignment_features(self, samples: np.ndarray) -> np.ndarray:
        if samples.size < OFFSET_FRAME_LEN:
            value = float(np.sqrt(np.mean(samples * samples) + 1e-12)) if samples.size else 0.0
            return np.array([[value]], dtype=np.float32)
        freqs, _, spectrum = cast(
            tuple[np.ndarray, np.ndarray, np.ndarray],
            stft(
                samples,
                fs=OFFSET_SR,
                window='hann',
                nperseg=OFFSET_FRAME_LEN * 2,
                noverlap=(OFFSET_FRAME_LEN * 2) - OFFSET_HOP,
                boundary='zeros',
                padded=False,
            )
        )
        if spectrum.size == 0:
            value = float(np.sqrt(np.mean(samples * samples) + 1e-12))
            return np.array([[value]], dtype=np.float32)
        magnitude = np.abs(spectrum).astype(np.float32)
        log_magnitude = np.log1p(magnitude)

        features: list[np.ndarray] = []
        for low_hz, high_hz in ((80, 250), (250, 500), (500, 1000), (1000, 2000), (2000, 3500)):
            band_mask = (freqs >= low_hz) & (freqs < high_hz)
            if np.any(band_mask):
                features.append(np.mean(log_magnitude[band_mask], axis=0).astype(np.float32))
            else:
                features.append(cast(np.ndarray, np.zeros(log_magnitude.shape[1], dtype=np.float32)))

        flux = np.zeros(log_magnitude.shape[1], dtype=np.float32)
        if log_magnitude.shape[1] > 1:
            flux[1:] = np.mean(np.maximum(np.diff(log_magnitude, axis=1), 0.0), axis=0).astype(np.float32)
        centroid_den = np.sum(magnitude, axis=0) + 1e-12
        centroid = (np.sum(freqs[:, None] * magnitude, axis=0) / centroid_den).astype(np.float32)
        energy = np.sqrt(np.mean(magnitude * magnitude, axis=0) + 1e-12).astype(np.float32)
        features.extend((flux, centroid, energy))

        feature_matrix = np.vstack(features).astype(np.float32)
        for idx in range(feature_matrix.shape[0]):
            row = cast(np.ndarray, feature_matrix[idx])
            mean = float(np.mean(row))
            std = float(np.std(row))
            if std > 1e-6:
                feature_matrix[idx] = (row - mean) / (std + 1e-12)
            else:
                feature_matrix[idx].fill(0.0)
        return feature_matrix

    def _normalized_cross_correlation(self, template: np.ndarray, reference: np.ndarray) -> np.ndarray:
        template = np.asarray(template, dtype=np.float32)
        reference = np.asarray(reference, dtype=np.float32)
        template_len = len(template)
        reference_len = len(reference)
        if template_len == 0 or reference_len == 0 or reference_len < template_len:
            return np.zeros(0, dtype=np.float32)
        template_energy = float(np.sum(template * template))
        if template_energy < 1e-12:
            return np.zeros(reference_len - template_len + 1, dtype=np.float32)
        corr = fftconvolve(reference, template[::-1], mode='valid')
        reference_sq = reference * reference
        reference_cumsum = np.concatenate((np.array([0.0], dtype=np.float64), np.cumsum(reference_sq, dtype=np.float64)))
        reference_window_energy = reference_cumsum[template_len:] - reference_cumsum[:-template_len]
        denom = np.sqrt(template_energy * np.maximum(reference_window_energy, 1e-12))
        return np.nan_to_num(corr / denom, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)

    def _second_peak(self, values: np.ndarray, peak_idx: int, exclusion_radius: int) -> float:
        if values.size == 0:
            return 0.0
        keep = np.ones(values.shape[0], dtype=bool)
        start = max(0, peak_idx - exclusion_radius)
        end = min(values.shape[0], peak_idx + exclusion_radius + 1)
        keep[start:end] = False
        if not np.any(keep):
            return 0.0
        return float(np.max(values[keep]))

    def _match_feature_sequence(self, clip_features: np.ndarray, reference_features: np.ndarray) -> tuple[float, float]:
        if clip_features.size == 0 or reference_features.size == 0:
            return float("nan"), 0.0
        max_rows = min(clip_features.shape[0], reference_features.shape[0])
        correlations = [
            self._normalized_cross_correlation(clip_features[row_idx], reference_features[row_idx])
            for row_idx in range(max_rows)
        ]
        correlations = [corr for corr in correlations if corr.size > 0]
        if not correlations:
            return float("nan"), 0.0
        min_len = min(len(corr) for corr in correlations)
        composite = np.mean(np.vstack([corr[:min_len] for corr in correlations]), axis=0)
        peak_idx = int(np.argmax(composite))
        peak_val = float(composite[peak_idx])
        second_peak = self._second_peak(composite, peak_idx, max(1, int(1.0 / (OFFSET_HOP / OFFSET_SR))))
        prominence = max(0.0, peak_val - second_peak)
        confidence = min(1.0, max(0.0, peak_val) * (1.0 + prominence))
        return (peak_idx * OFFSET_HOP) / float(OFFSET_SR), confidence

    def _refine_match_time(self, clip: np.ndarray, reference: np.ndarray, coarse_time: float) -> tuple[float, float]:
        clip_len = len(clip)
        reference_len = len(reference)
        if clip_len == 0 or reference_len < clip_len:
            return float("nan"), 0.0
        coarse_index = int(round(coarse_time * OFFSET_SR))
        pad_samples = int(round(OFFSET_FINE_SEARCH_PAD * OFFSET_SR))
        search_start = max(0, coarse_index - pad_samples)
        search_end = min(reference_len, coarse_index + clip_len + pad_samples)
        local_reference = reference[search_start:search_end]
        corr = self._normalized_cross_correlation(clip, local_reference)
        if corr.size == 0:
            return float("nan"), 0.0
        peak_idx = int(np.argmax(corr))
        peak_val = float(corr[peak_idx])
        if 0 < peak_idx < (len(corr) - 1):
            y0 = float(corr[peak_idx - 1])
            y1 = float(corr[peak_idx])
            y2 = float(corr[peak_idx + 1])
            denom = y0 - (2.0 * y1) + y2
            delta = 0.5 * (y0 - y2) / denom if abs(denom) > 1e-12 else 0.0
        else:
            delta = 0.0
        refined_index = search_start + peak_idx + delta
        second_peak = self._second_peak(corr, peak_idx, max(1, int(0.5 * OFFSET_SR)))
        prominence = max(0.0, peak_val - second_peak)
        confidence = min(1.0, max(0.0, peak_val) * (1.0 + prominence))
        return refined_index / float(OFFSET_SR), confidence

    def _match_clip_in_reference(self, clip: np.ndarray, reference: np.ndarray) -> tuple[float, float]:
        """Returns (clip_start_time_seconds_in_reference, confidence) or (nan, 0.0) if no match found."""
        clip_wave = self._prepare_alignment_audio(clip)
        reference_wave = self._prepare_alignment_audio(reference)
        if clip_wave.size == 0 or reference_wave.size < clip_wave.size:
            return float("nan"), 0.0

        reference_features = self._build_alignment_features(reference_wave)
        window_samples = min(len(clip_wave), max(int(round(OFFSET_ANALYSIS_WINDOW * OFFSET_SR)), OFFSET_FRAME_LEN * 2))
        step_samples = max(1, int(round(OFFSET_ANALYSIS_STEP * OFFSET_SR)))
        if len(clip_wave) <= window_samples:
            window_starts = [0]
        else:
            window_starts = list(range(0, len(clip_wave) - window_samples + 1, step_samples))
            last_start = len(clip_wave) - window_samples
            if window_starts[-1] != last_start:
                window_starts.append(last_start)

        estimates: list[tuple[float, float]] = []
        for window_start in window_starts:
            window_end = window_start + window_samples
            clip_window = clip_wave[window_start:window_end]
            if clip_window.size < OFFSET_FRAME_LEN:
                continue
            clip_features = self._build_alignment_features(clip_window)
            coarse_time, coarse_confidence = self._match_feature_sequence(clip_features, reference_features)
            if not math.isfinite(coarse_time) or coarse_confidence <= 0.0:
                continue
            refined_time, fine_confidence = self._refine_match_time(clip_window, reference_wave, coarse_time)
            if not math.isfinite(refined_time) or fine_confidence <= 0.0:
                continue
            clip_start_estimate = refined_time - (window_start / float(OFFSET_SR))
            combined_confidence = min(1.0, coarse_confidence * fine_confidence)
            estimates.append((clip_start_estimate, combined_confidence))

        if not estimates:
            return float("nan"), 0.0

        best_group: list[tuple[float, float]] = []
        best_weight = -1.0
        for center_time, _ in estimates:
            group = [(time, confidence) for time, confidence in estimates if abs(time - center_time) <= OFFSET_MATCH_CLUSTER_TOLERANCE]
            weight = sum(confidence for _, confidence in group)
            if weight > best_weight:
                best_group = group
                best_weight = weight
        if not best_group:
            return float("nan"), 0.0

        total_weight = sum(confidence for _, confidence in best_group)
        if total_weight <= 0.0:
            return float("nan"), 0.0
        match_time = sum(time * confidence for time, confidence in best_group) / total_weight
        match_confidence = min(1.0, max(confidence for _, confidence in best_group) * (0.75 + (0.25 * len(best_group) / len(estimates))))
        return match_time, match_confidence

    async def measure_relative_offset_for_channel(self, logical_channel_id: LogicalChannelId) -> bool | None:
        """Measures relative delay between sources mapped to a logical channel."""
        loop = asyncio.get_running_loop()
        logical_channel = await self.handler.get_logical_channel_by_id(logical_channel_id)
        if not logical_channel:
            Log.error(Label.QUALITY, f"Logical Channel ID {logical_channel_id} not found for offset measurement.")
            return
        channel_num = logical_channel["channel_num"]
        stream_name = create_stream_name(logical_channel["logical_channel_title"], channel_num)
        tvg_logo = logical_channel["tvg_logo"]
        mappings = await self.handler.get_channel_mappings_for_ui(logical_channel_id)
        if len(mappings) < 2:
            Log.error(Label.QUALITY, f"{stream_name}: At least two mapped sources are required for offset measurement.")
            return
        missing_offset_source_ids = {mapping["source_id"] for mapping in mappings if mapping["offset"] is None}

        def get_ffmpeg_cmd(url: StreamURL, out_path: Path, duration: float = 0, realtime: bool = False) -> list[str]:
            cmd: list[str] = [
                str(self.config.ffmpeg_path),
                "-hide_banner", "-loglevel", "info",
                "-user_agent", NEXUS_TUNER_USER_AGENT,
                "-fflags", "+genpts", "-copyts",
                "-probesize", "1M", "-analyzeduration", "1M", "-reconnect", "1",
                "-reconnect_delay_max", "3", "-reconnect_streamed", "1", "-reconnect_at_eof", "1",
                "-reconnect_on_network_error", "1", "-reconnect_on_http_error", "5xx",
            ]
            if realtime:
                cmd.append("-re")
            if duration > 0:
                cmd.extend(["-t", str(duration)])
            cmd.extend([
                "-i", url, "-vn",
                "-af", "aresample=async=1:first_pts=0",
                "-ac", "1", "-ar", str(OFFSET_SR),
                "-c:a", "pcm_s16le",
            ])
            cmd.extend(["-y", str(out_path)])
            return cmd

        audio_files: list[Path] = []
        video_names: dict[SourceId, VideoName] = {}
        try:
            discovered_sources = {m["source_id"]: ds for m in mappings if (ds := await self.handler.get_discovered_source(m["source_id"]))}
            valid_source_ids: list[SourceId] = []
            for source_id, ds in discovered_sources.items():
                provider_alias = ds["provider_alias"]
                if await self.stream_manager.is_provider_available_soon(provider_alias):
                    valid_source_ids.append(source_id)
                    self._full_providers[provider_alias] = False
                else:
                    self._full_providers[provider_alias] = True
            offset_source_ids = {m["source_id"]: m["offset"] for m in mappings if m["offset"] is not None}
            if offset_source_ids:
                if all(source_id not in offset_source_ids for source_id in valid_source_ids):
                    if not missing_offset_source_ids:
                        Log.debug(Label.QUALITY, f"{stream_name}: Keeping existing offsets because no anchored source is currently available to remeasure against.")
                        return True
                    Log.warn(Label.QUALITY, f"{stream_name}: No currently available source has an existing offset measurement to anchor new sources against, retrying later.")
                    return False
            else:
                if len(valid_source_ids) < 2:
                    Log.warn(Label.QUALITY, f"{stream_name}: Not enough sources with available provider slots for initial offset measurement, retrying later.")
                    return False
            if len(valid_source_ids) < 2:
                if not missing_offset_source_ids:
                    Log.debug(Label.QUALITY, f"{stream_name}: Keeping existing offsets because fewer than two sources are currently available.")
                    return True
                Log.warn(Label.QUALITY, f"{stream_name}: Fewer than two sources are currently available for offset measurement, retrying later.")
                return False
            ref_source_id = random.choice(valid_source_ids)
            other_source_ids = [source_id for source_id in valid_source_ids if source_id != ref_source_id]
            if not other_source_ids:
                if not missing_offset_source_ids:
                    Log.debug(Label.QUALITY, f"{stream_name}: Keeping existing offsets because no additional available sources need measuring.")
                    return True
                Log.warn(Label.QUALITY, f"{stream_name}: No unmeasured sources are currently available to compare against the reference, retrying later.")
                return False
            ref_discovered_source = discovered_sources[ref_source_id]
            ref_provider_alias = ref_discovered_source["provider_alias"]
            ref_source_title = ref_discovered_source['display_title'] or ref_discovered_source['tvg_name']
            ref_video_name = create_video_name(stream_name, ref_source_title, ref_source_id)
            video_names[ref_source_id] = ref_video_name

            async def capture_short_clip(source_id: SourceId, round: int) -> dict[tuple[SourceId, int], tuple[VideoName, Path, float]] | None:
                discovered_source = discovered_sources[source_id]
                source_title = discovered_source['display_title'] or discovered_source['tvg_name']
                video_name = create_video_name(stream_name, source_title, source_id)
                video_names[source_id] = video_name
                provider_alias = discovered_source["provider_alias"]
                while True:
                    if self.handler.get_pending_stream_count() > 0:
                        return
                    provider_slots = await self.handler.get_provider_slots(provider_alias)
                    if not provider_slots:
                        Log.error(Label.QUALITY, f"{video_name}: Provider slots not found for {provider_alias}, skipping.")
                        self._full_providers[provider_alias] = True
                        return
                    if provider_slots.get_total_slots() <= 0:
                        Log.warn(Label.QUALITY, f"{video_name}: Provider {provider_slots.get_alias()} configured with 0 slots, skipping.")
                        self._full_providers[provider_alias] = True
                        return

                    if await provider_slots.try_acquire():
                        break
                    await self.stream_manager.prune_processes(provider_alias)
                    soon = await self.stream_manager.is_provider_available_soon(provider_alias, 1 if provider_alias == ref_provider_alias else 0)
                    if not soon:
                        if soon is False or all(v for k, v in self._full_providers.items() if k != provider_alias):
                            self._full_providers[provider_alias] = True
                        return
                    await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
                process: asyncio.subprocess.Process | None = None
                try:
                    Log.debug(Label.QUALITY, f"{video_name}: Capturing {OFFSET_CLIP_DURATION}s clip...")
                    audio_path = self.config.get_offset_path(VideoKey(f"{logical_channel_id}_{source_id}_round{round}"))
                    audio_files.append(audio_path)

                    process = await asyncio.create_subprocess_exec(*get_ffmpeg_cmd(discovered_source["stream_url"], audio_path, OFFSET_CLIP_DURATION), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    clip_start = loop.time()
                    async with self.quality_process_lock:
                        cast(QualityProcessInfosImpl, self.processes)[source_id] = {
                            "provider_alias": provider_alias,
                            "tvg_logo": discovered_source["tvg_logo"] or tvg_logo,
                            "source_title": source_title,
                            "logical_channel_id": logical_channel_id,
                            "channel_num": channel_num,
                            "type": "Offset",
                            "started_at": datetime.now(),
                        }
                    task = asyncio.create_task(asyncio.wait_for(process.communicate(), timeout=OFFSET_CLIP_DURATION + OFFSET_GRACE))
                    try:
                        provider_slots.add_background_task(task)
                        _, stderr = await task
                    except asyncio.CancelledError:
                        Log.debug(Label.QUALITY, f"{video_name}: ffmpeg task was cancelled.")
                        if provider_slots.pop_cancelled_task(task):
                            return  # Cancelled for user stream
                        raise
                    if process.returncode != 0:
                        Log.debug(Label.QUALITY, f"{video_name}: ffmpeg failed with code {process.returncode} - {stderr.decode()}".replace(discovered_source["stream_url"], "{{stream_url}}").strip())
                        return
                    return {(source_id, round): (video_name, audio_path, clip_start)}
                except BaseException as e:
                    Log.error(Label.QUALITY, f"{video_name}: Error capturing clip - {e}")
                    if isinstance(e, Exception):
                        return
                    raise
                finally:
                    await run_bg(self._cleanup_process(process, provider_slots, source_id, video_name))

            while True:
                if self.handler.get_pending_stream_count() > 0:
                    return False
                ref_provider_slots = await self.handler.get_provider_slots(ref_provider_alias)
                if not ref_provider_slots:
                    Log.error(Label.QUALITY, f"{ref_video_name}: Provider slots not found for {ref_provider_alias}.")
                    self._full_providers[ref_provider_alias] = True
                    return False
                if ref_provider_slots.get_total_slots() <= 0:
                    Log.warn(Label.QUALITY, f"{ref_video_name}: Provider {ref_provider_slots.get_alias()} configured with 0 slots, cannot measure offset.")
                    self._full_providers[ref_provider_alias] = True
                    return False

                if await ref_provider_slots.try_acquire():
                    break
                await self.stream_manager.prune_processes(ref_provider_alias)
                if not await self.stream_manager.is_provider_available_soon(ref_provider_alias):
                    self._full_providers[ref_provider_alias] = True
                    return False
                await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)

            clip_infos: dict[tuple[SourceId, int], tuple[VideoName, Path, float]] = {}
            ref_process: asyncio.subprocess.Process | None = None
            try:
                Log.debug(Label.QUALITY, f"{ref_video_name}: Capturing as reference...")
                ref_audio_path = self.config.get_offset_path(VideoKey(f"{logical_channel_id}_{ref_source_id}_reference"))
                audio_files.append(ref_audio_path)
                ref_process = await asyncio.create_subprocess_exec(*get_ffmpeg_cmd(ref_discovered_source["stream_url"], ref_audio_path, 0, realtime=True), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                t0 = loop.time()
                async with self.quality_process_lock:
                    cast(QualityProcessInfosImpl, self.processes)[ref_source_id] = {
                        "provider_alias": ref_provider_alias,
                        "tvg_logo": ref_discovered_source["tvg_logo"] or tvg_logo,
                        "source_title": ref_source_title,
                        "logical_channel_id": logical_channel_id,
                        "channel_num": channel_num,
                        "type": "Offset",
                        "started_at": datetime.now(),
                    }

                results = await asyncio.gather(*(capture_short_clip(source_id, 1) for source_id in other_source_ids), return_exceptions=True)
                for res in results:
                    if not res:
                        continue
                    if isinstance(res, Exception):
                        continue
                    if isinstance(res, BaseException):
                        raise res
                    clip_infos |= res
                if not clip_infos:
                    Log.error(Label.QUALITY, f"{stream_name}: No valid clips captured, cannot measure offset.")
                    return False  # Since the first round failed, just retry instead of only doing the second round
                if ref_process.returncode is not None:
                    stderr = (await ref_process.stderr.read()).decode().replace(ref_discovered_source["stream_url"], "{{stream_url}}").strip() if ref_process.stderr else ""
                    raise RuntimeError(f"reference ffmpeg {ref_video_name} exited prematurely with code {ref_process.returncode} - {stderr}")

                Log.debug(Label.QUALITY, f"{stream_name}: Waiting {OFFSET_WAIT_INTERVAL}s between rounds.")
                task = asyncio.create_task(asyncio.sleep(OFFSET_WAIT_INTERVAL))
                try:
                    ref_provider_slots.add_background_task(task)
                    await task
                except asyncio.CancelledError:
                    Log.debug(Label.QUALITY, f"{ref_video_name}: ffmpeg task was cancelled.")
                    if ref_provider_slots.pop_cancelled_task(task):
                        return False  # Cancelled for user stream
                    raise

                if ref_process.returncode is not None:
                    stderr = (await ref_process.stderr.read()).decode().replace(ref_discovered_source["stream_url"], "{{stream_url}}").strip() if ref_process.stderr else ""
                    raise RuntimeError(f"reference ffmpeg {ref_video_name} exited prematurely with code {ref_process.returncode} - {stderr}")
                results = await asyncio.gather(*(capture_short_clip(source_id, 2) for source_id in other_source_ids), return_exceptions=True)
                for res in results:
                    if not res:
                        continue
                    if isinstance(res, Exception):
                        continue
                    if isinstance(res, BaseException):
                        raise res
                    clip_infos |= res
                if ref_process.returncode is not None:
                    stderr = (await ref_process.stderr.read()).decode().replace(ref_discovered_source["stream_url"], "{{stream_url}}").strip() if ref_process.stderr else ""
                    raise RuntimeError(f"reference ffmpeg {ref_video_name} exited prematurely with code {ref_process.returncode} - {stderr}")

                task = asyncio.create_task(ref_process.wait())
                try:
                    ref_provider_slots.add_background_task(task)
                    try:
                        await asyncio.wait_for(task, timeout=OFFSET_GRACE)
                    except asyncio.TimeoutError:
                        pass
                except asyncio.CancelledError:
                    Log.debug(Label.QUALITY, f"{ref_video_name}: ffmpeg task was cancelled.")
                    if not ref_provider_slots.pop_cancelled_task(task):
                        raise  # Cancel wasn't for user stream
            except BaseException as e:
                Log.error(Label.QUALITY, f"{stream_name}: Error during offset measurement - {e}")
                if isinstance(e, Exception):
                    return False
                raise
            finally:
                await run_bg(self._cleanup_process(ref_process, ref_provider_slots, ref_source_id, ref_video_name))
            max_offset = (loop.time() - t0) + ((OFFSET_CLIP_DURATION + OFFSET_GRACE) * len(other_source_ids))

            try:
                ref_envelope = await asyncio.to_thread(self._load_audio, ref_audio_path)
            except Exception as e:
                Log.error(Label.QUALITY, f"{ref_video_name}: Failed to load reference audio - {e}")
                return False

            raw_offsets: dict[tuple[SourceId, int], Offset] = {}
            raw_confidences: dict[tuple[SourceId, int], float] = {}
            raw_match_times: dict[tuple[SourceId, int], float] = {}
            for (source_id, round), (video_name, audio_path, start_offset) in clip_infos.items():
                try:
                    clip_envelope = await asyncio.to_thread(self._load_audio, audio_path)
                except Exception as e:
                    Log.error(Label.QUALITY, f"{video_name}: Failed to load clip audio - {e}")
                    continue
                match_time, peak_val = await asyncio.to_thread(self._match_clip_in_reference, clip_envelope, ref_envelope)
                if not math.isfinite(match_time):
                    Log.warn(Label.QUALITY, f"{video_name}: Invalid match time computed - {match_time}")
                    continue
                raw_offset = Offset((start_offset - t0) - match_time)
                if peak_val < OFFSET_CONF_THRESH:
                    Log.warn(Label.QUALITY, f"{video_name}: Low confidence {peak_val:.3f} (min: {OFFSET_CONF_THRESH}) in offset measurement of {raw_offset:.3f}s for clip #{round}, skipping.")
                    continue
                if abs(raw_offset) > max_offset:
                    Log.warn(Label.QUALITY, f"{video_name}: Unreasonable offset measurement of {raw_offset:.3f}s for clip #{round} (worst case should be ±{max_offset:.3f}s), skipping.")
                    continue
                raw_offsets[(source_id, round)] = raw_offset
                raw_confidences[(source_id, round)] = peak_val
                raw_match_times[(source_id, round)] = match_time

            best_offsets: dict[SourceId, Offset] = {}
            best_confidences: dict[SourceId, float] = {}
            for source_id in other_source_ids:
                source_measurements = [(round, raw_offsets[(source_id, round)], raw_confidences[(source_id, round)])
                                       for round in (1, 2) if (source_id, round) in raw_offsets]
                if not source_measurements:
                    continue
                if len(source_measurements) == 2:
                    round_1_match_time = raw_match_times[(source_id, 1)]
                    round_2_match_time = raw_match_times[(source_id, 2)]
                    round_1_start = clip_infos[(source_id, 1)][2]
                    round_2_start = clip_infos[(source_id, 2)][2]
                    round_variance = abs((round_2_match_time - round_1_match_time) - (round_2_start - round_1_start))
                    if round_2_match_time <= round_1_match_time:
                        Log.warn(Label.QUALITY, f"{video_names[source_id]}: Clip #2 matched before clip #1 in the reference timeline, using the later-matching result only.")
                    elif round_variance > OFFSET_ROUND_VARIANCE_WARN:
                        Log.warn(Label.QUALITY, f"{video_names[source_id]}: Clip matches differ by {round_variance:.3f}s between rounds, using the least delayed measurement.")
                # Probe startup can only make a short capture appear earlier in the reference, so keep the
                # largest surviving offset across rounds rather than whichever round had the higher confidence.
                best_round, best_offset, best_confidence = max(source_measurements, key=lambda measurement: (measurement[1], measurement[2]))
                if len(source_measurements) == 1:
                    Log.debug(Label.QUALITY, f"{video_names[source_id]}: Only clip #{best_round} produced a usable offset measurement.")
                best_offsets[source_id] = best_offset
                best_confidences[source_id] = best_confidence
            if not best_offsets:
                if not missing_offset_source_ids:
                    Log.warn(Label.QUALITY, f"{stream_name}: No new high confidence offset measurements computed, keeping existing offsets.")
                    return True
                Log.error(Label.QUALITY, f"{stream_name}: No high confidence offset measurements computed.")
                return False
          
            if offset_source_ids:
                if ref_source_id in offset_source_ids:
                    anchor = (ref_source_id, offset_source_ids[ref_source_id])
                    Log.debug(Label.QUALITY, f"{stream_name}: Using reference source {ref_video_name} with existing offset as anchor for new measurements.")
                else:
                    for source_id in best_offsets:
                        if source_id in offset_source_ids:
                            anchor = (source_id, offset_source_ids[source_id])
                            Log.debug(Label.QUALITY, f"{stream_name}: Using source {video_names[source_id]} with existing offset as anchor for new measurements.")
                            break
                    else:
                        Log.error(Label.QUALITY, f"{stream_name}: No recorded sources have existing offset measurements to use as an anchor for new measurements.")
                        return False
            else:
                anchor = None
                Log.debug(Label.QUALITY, f"{stream_name}: No existing offset measurements to use as an anchor for new measurements.")

            best_offsets[ref_source_id] = Offset(0)
            best_confidences[ref_source_id] = 1.0
            if not anchor:
                real_offsets = best_offsets.copy()
            else:
                real_offsets = {source_id: Offset(anchor[1] + (offset - best_offsets[anchor[0]])) for source_id, offset in best_offsets.items()}

            for mapping in mappings:
                if mapping["source_id"] in real_offsets:
                    offset = real_offsets[mapping["source_id"]]
                    cast(SourceMappingInfoMutable, mapping)["offset"] = offset
                    Log.debug(Label.QUALITY, f"{video_names[mapping['source_id']]}: Assigned offset {offset:.3f}s with confidence {best_confidences[mapping['source_id']]:.2%}")
            min_offset = min((m["offset"] for m in mappings if m["offset"] is not None))
            if min_offset:
                for mapping in mappings:
                    if mapping["offset"] is not None:
                        cast(SourceMappingInfoMutable, mapping)["offset"] = Offset(mapping["offset"] - min_offset)
                Log.debug(Label.QUALITY, f"{stream_name}: Normalized offsets so minimum is zero based on minimum measured offset {min_offset:.3f}s.")
            
            if not await self.handler.update_mappings_for_logical_channel(logical_channel_id, mappings):
                Log.error(Label.QUALITY, f"{stream_name}: Failed to update mappings with new offsets.")
                return False
            if any(mapping["offset"] is None for mapping in mappings):
                Log.warn(Label.QUALITY, f"{stream_name}: Some mappings are still missing offset values after measurement.")
                return False
            return True
        except BaseException as e:
            Log.error(Label.QUALITY, f"{stream_name}: Unexpected error during offset measurement - {e}")
            if isinstance(e, Exception):
                return False
            raise
        finally:
            for audio_file in audio_files:
                try:
                    if audio_file.exists():
                        audio_file.unlink()
                except Exception as e:
                    Log.error(Label.QUALITY, f"{stream_name}: Failed to delete temporary file {audio_file} - {e}")
