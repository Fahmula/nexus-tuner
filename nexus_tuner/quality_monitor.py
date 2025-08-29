import asyncio
import json
from datetime import datetime, timedelta
from typing import Coroutine, Final, Any, Self

from nexus_tuner.config import Config
from nexus_tuner.handler import ChannelHandler
from nexus_tuner.slots import ProviderSlots
from nexus_tuner.utils import (FAILED_STOP_REASONS, PROCESS_TERMINATE_TIMEOUT, NEXUS_TUNER_USER_AGENT, Bitrate, BitrateScore, DateTimeISO, Framerate, FramerateScore, Height,
                                Label, Log, LogicalChannelId, Percent, ProbeInfo, ProbeSuccess, ProviderAlias, QualityInfoImpl, QualityScores,
                                QualityScoresImpl, ResolutionScore, QualityCacheData, QualityCacheDataImpl, Runtime, RuntimeInfo, RuntimeScore, SourceId, StopReason, StreamName,
                                StreamURL, TotalScore, UptimeScore, VideoName, Width, create_stream_name, create_video_name, run_bg)

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
MAX_HISTORY_PER_SOURCE: Final[int] = 10
MIN_DAYS_AT_MAX_HISTORY: Final[int] = 7
MIN_DAYS_AT_NON_MAX_HISTORY: Final[int] = 1


class QualityMonitor:
    __slots__ = ('config', 'handler', '_mutex', '_quality_scores')
    
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self._mutex: asyncio.Lock = asyncio.Lock()
        self._quality_scores: QualityScores = QualityScoresImpl({})

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler) -> Self:
        """Asynchronous factory for creating and initializing a QualityMonitor instance."""
        instance = cls(config, handler)
        quality_cache = await config.get_quality_cache(label=Label.STARTUP)
        if quality_cache:
            instance._build_quality_scores(quality_cache)
        return instance

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

    async def _get_stream_info(self, stream_url: StreamURL, provider_slots: ProviderSlots, video_name: VideoName) -> ProbeSuccess | None:
        """
        Extracts stream information using ffprobe, ensuring the subprocess is
        terminated on timeout or cancellation.
        """
        cmd: list[str] = [
            str(self.config.ffprobe_path),
            "-v", "error",
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
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=QUALITY_MONITOR_TIMEOUT + 3)

            if process.returncode != 0:
                Log.debug(Label.QUALITY, f"{video_name}: ffprobe failed with code {process.returncode} - {stderr.decode()}".replace(stream_url, "{{stream_url}}").strip())
                return
            info = json.loads(stdout)

        except asyncio.TimeoutError:
            Log.debug(Label.QUALITY, f"{video_name}: ffprobe timed out.")
            return
        except Exception as e:
            Log.error(Label.QUALITY, f"{video_name}: Failed to parse ffprobe output - {e}")
            return
        finally:
            async def bg_cleanup() -> None:
                try:
                    if process and process.returncode is None:
                        try:
                            process.terminate()
                            if process.stdout:
                                process.stdout._transport.close()  # type: ignore[reportAttributeAccessIssue]
                            await asyncio.wait_for(process.wait(), timeout=PROCESS_TERMINATE_TIMEOUT)
                            Log.debug(Label.QUALITY, f"{video_name}: ffprobe process terminated successfully.")
                        except asyncio.TimeoutError:
                            Log.warn(Label.QUALITY, f"{video_name}: Killing unresponsive ffprobe process.")
                            process.kill()
                        except Exception as e:
                            Log.error(Label.QUALITY, f"{video_name}: Error terminating ffprobe process - {e}")
                            process.kill()
                    if process and process.returncode is None:
                        Log.critical(Label.STREAM, f"{video_name}: ffprobe was not terminated properly, cannot release slot.")
                        return
                    run_bg(provider_slots.release())
                except BaseException as e:
                    Log.critical(Label.QUALITY, f"{video_name}: Error during stopping ffprobe process, cannot release slot - {e}")
                    raise
            run_bg(bg_cleanup())  # Prevent asyncio.CancelledError from interrupting cleanup

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

    async def _run_single_probe(self, provider_alias: ProviderAlias, stream_url: StreamURL, source_id: SourceId, video_name: VideoName) -> tuple[SourceId, ProbeInfo, str]:
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
                if not await provider_slots.try_acquire():
                    await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
                    continue

                task = asyncio.create_task(self._get_stream_info(stream_url, provider_slots, video_name))
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
            Log.error(Label.QUALITY, f"{video_name}: Unexpected error during probe: {e}")
            return source_id, {"status": "offline", "reason": f"Probe failed: {e}"}, video_name
    
    async def analyze_mapped_sources(self, input_lc_id: LogicalChannelId | None = None) -> None:
        """Finds and probes all mapped sources concurrently."""
        valid_mappings: list[tuple[DateTimeISO, LogicalChannelId, list[SourceId], StreamName]] = []
        if input_lc_id:
            logical_channel = await self.handler.get_logical_channel_by_id(input_lc_id)
            if not logical_channel:
                Log.error(Label.QUALITY, f"Logical Channel ID {input_lc_id} not found.")
                return
            stream_name = create_stream_name(logical_channel["logical_channel_title"], logical_channel["channel_num"])
            Log.info(Label.QUALITY, f"{stream_name}: Starting stream quality analysis.")
            mappings = await self.handler.get_mappings_for_logical_channel(input_lc_id)
            if not mappings:
                Log.error(Label.QUALITY, f"{stream_name}: No mapped sources found.")
                return
            valid_mappings.append((DateTimeISO("0001-01-01"), input_lc_id, [source_id for source_id in mappings], stream_name))
        else:
            Log.info(Label.QUALITY, "Starting stream quality analysis cycle.")
            all_mappings = await self.handler.copy_channel_mappings_data()
            if not all_mappings:
                Log.warn(Label.QUALITY, "No mapped sources to analyze.")
                return

            quality_cache = await self.config.get_quality_cache()
            if quality_cache is None:
                Log.critical(Label.QUALITY, "Quality cache was removed/corrupted after startup, cannot analyze sources.")
                return
            now = datetime.now()
            for logical_channel_id, mappings in all_mappings.items():
                logical_channel = await self.handler.get_logical_channel_by_id(logical_channel_id)
                if not logical_channel:
                    Log.error(Label.QUALITY, f"Logical Channel ID {logical_channel_id} not found in mappings.")
                    continue
                stream_name = create_stream_name(logical_channel["logical_channel_title"], logical_channel["channel_num"])
                if not mappings:
                    Log.debug(Label.QUALITY, f"{stream_name}: No valid sources found.")
                    continue
                min_updated_at = min([quality_cache.get(source_id, {}).get("updated_at", "0001-01-01") for source_id in mappings])
                at_max_history = all(len(quality_cache.get(source_id, {}).get("statuses", [])) >= MAX_HISTORY_PER_SOURCE for source_id in mappings)
                delta = timedelta(days=MIN_DAYS_AT_MAX_HISTORY) if at_max_history else timedelta(days=MIN_DAYS_AT_NON_MAX_HISTORY)
                if datetime.fromisoformat(min_updated_at) > now - delta:
                    continue
                valid_mappings.append((min_updated_at, logical_channel_id, [source_id for source_id in mappings], stream_name))
            if not valid_mappings:
                Log.info(Label.QUALITY, "No sources are due for quality probing.")
                return
            valid_mappings.sort(key=lambda x: x[0])

        for _, logical_channel_id, source_ids, stream_name in valid_mappings:
            tasks: list[Coroutine[Any, Any, tuple[SourceId, ProbeInfo, str]]] = []
            for source_id in source_ids:
                discovered_source = await self.handler.get_discovered_source(source_id)
                if not discovered_source:
                    Log.debug(Label.QUALITY, f"{stream_name} [{source_id}]: Not found in discovered sources.")
                    continue
                video_name = create_video_name(stream_name, discovered_source['display_title'] or discovered_source['tvg_name'], source_id)
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
                        source_id,
                        video_name,
                    )
                )
            if not tasks:
                Log.debug(Label.QUALITY, f"{stream_name} No valid sources found to probe.")
                continue

            raw_results = await asyncio.gather(*tasks, return_exceptions=True)
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
                    return
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
                    return
                self._build_quality_scores(modified_cache)
        if input_lc_id:
            Log.info(Label.QUALITY, f"{valid_mappings[0][3]}: Completed analysis for {len(valid_mappings[0][2])} mappings(s).")

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
            uptime = Percent(sum(1 for s in source_entry["statuses"] if s == "online") / len(source_entry["statuses"]) if source_entry["statuses"] else 0)
            
            height_score = ResolutionScore(RESOLUTION_WEIGHT * min(avg_height / RESOLUTION_NORM, 1.0))
            bitrate_score = BitrateScore(BITRATE_WEIGHT * min(avg_bitrate / BITRATE_NORM, 1.0))
            framerate_score = FramerateScore(FRAMERATE_WEIGHT * min(avg_framerate / FRAMERATE_NORM, 1.0))
            uptime_score = UptimeScore(UPTIME_WEIGHT * uptime)

            # Think of the avg_runtime as total_play_duration / total_num_failres
            # The reason why we don't store that is to only consider recent history rather than the entire lifetime
            if any(r["stop_reason"] in FAILED_STOP_REASONS for r in source_entry["runtimes"]):
                if source_entry["runtimes"][-1]["stop_reason"] in FAILED_STOP_REASONS:
                    avg_runtime = Runtime(sum([r["runtime"] for r in source_entry["runtimes"]]) / len(source_entry["runtimes"]))
                else:
                    avg_runtime = Runtime(sum([r["runtime"] for r in source_entry["runtimes"]]) / (len(source_entry["runtimes"])-1))
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
