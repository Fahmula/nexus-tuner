import asyncio
import json
from datetime import datetime, timedelta
from typing import Coroutine, Final, Any, Self

from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.utils import (NEXUS_STREAM_USER_AGENT, Bitrate, BitrateScore, ChannelNum, DateTimeISO, Framerate, FramerateScore, Height,
                                Label, LogicalChannelId, LogicalChannelTitle, Percent, ProbeInfo, ProbeSuccess, ProviderAlias, QualityInfoImpl, QualityScores,
                                QualityScoresImpl, ResolutionScore, QualityCacheData, QualityCacheDataImpl, SourceId,
                                StreamURL, TotalScore, UptimeScore, Width, run_bg)

# --- Constants ---
RESOLUTION_WEIGHT: Final[int] = 50
BITRATE_WEIGHT: Final[int] = 30
FRAMERATE_WEIGHT: Final[int] = 20
UPTIME_WEIGHT: Final[int] = 0

RESOLUTION_NORM: Final[int] = 2160
BITRATE_NORM: Final[int] = 12_000_000
FRAMERATE_NORM: Final[int] = 60

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

    async def remove_source(self, source_id: SourceId) -> bool:
        """Removes a source from the quality scores and cache."""
        async with self._mutex:
            quality_cache = await self.config.get_quality_cache()
            if quality_cache is None:
                self.config.critical(Label.QUALITY, f"Quality cache was removed/corrupted after startup, cannot remove {source_id}.")
                return False
            if source_id in quality_cache:
                del quality_cache[source_id]
                if not await self.config.save_quality_cache(quality_cache):
                    self.config.critical(Label.QUALITY, f"Failed to save quality cache after removing {source_id}.")
                    return False
            if source_id in self._quality_scores:
                new_quality_scores = QualityScoresImpl({**self._quality_scores})
                del new_quality_scores[source_id]
                self._quality_scores = new_quality_scores
            return True

    async def _get_stream_info(self, logical_channel_title: LogicalChannelTitle, channel_num: ChannelNum, source_id: SourceId, stream_url: StreamURL) -> ProbeSuccess | None:
        """
        Extracts stream information using ffprobe, ensuring the subprocess is
        terminated on timeout or cancellation.
        """
        cmd: list[str] = [
            "ffprobe",
            "-v", "error",
            "-user_agent", NEXUS_STREAM_USER_AGENT,
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,r_frame_rate",
            "-show_entries", "packet=pts_time,size",
            "-read_intervals", f"%+{QUALITY_MONITOR_TIMEOUT}",
            "-of", "json",
            stream_url
        ]

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=QUALITY_MONITOR_TIMEOUT + 3)

            if proc.returncode != 0:
                self.config.warn(Label.QUALITY, f"ffprobe for {source_id} in '{logical_channel_title}' ({channel_num}) failed with code {proc.returncode}: {stderr.decode()}".replace(stream_url, "{{stream_url}}").strip())
                return
            info = json.loads(stdout)

        except asyncio.TimeoutError:
            self.config.warn(Label.QUALITY, f"ffprobe for {source_id} in '{logical_channel_title}' ({channel_num}) timed out.")
            return
        except Exception as e:
            self.config.error(Label.QUALITY, f"Failed to parse ffprobe output for {source_id} in '{logical_channel_title}' ({channel_num}): {e}")
            return
        finally:
            async def bg_cleanup() -> None:
                if proc and proc.returncode is None:
                    try:
                        proc.kill()
                        await proc.wait()
                    except ProcessLookupError:
                        pass
            run_bg(bg_cleanup())

        stream = info.get('streams', [{}])[0]
        width: Width = Width(int(stream.get('width', 0)))
        height: Height = Height(int(stream.get('height', 0)))
        fr_str = stream.get('r_frame_rate', '0/1')
        nums = fr_str.split('/')
        framerate: Framerate = Framerate(float(nums[0]) / float(nums[1]) if len(nums) == 2 and nums[1] != '0' else float(nums[0]))

        packets = info.get('packets', [])
        if not packets: return

        sizes, times = zip(*((float(pkt['size']), float(pkt['pts_time'])) for pkt in packets))
        duration_s = max(times) - min(times)
        if duration_s <= 0: return
        
        total_bytes = sum(sizes)
        bitrate: Bitrate = Bitrate((total_bytes * 8) / duration_s)

        return ProbeSuccess({"status": "online", "width": width, "height": height, "bitrate": bitrate, "framerate": framerate})

    async def _run_single_probe(self, logical_channel_title: LogicalChannelTitle, channel_num: ChannelNum, source_id: SourceId, stream_url: StreamURL, provider_alias: ProviderAlias) -> tuple[SourceId, ProbeInfo]:
        """
        Probes a single stream, persistently trying to acquire a slot, and ensures
        all resources are cleaned up upon completion, failure, or cancellation.
        """
        current_task = asyncio.current_task()
        if not current_task:
            msg = "Current task is None, cannot run probe without a task context"
            self.config.error(Label.QUALITY, msg)
            raise RuntimeError(msg)

        try:
            paused = False
            while True:
                if self.handler.get_pending_stream_count() > 0:
                    if not paused:
                        self.config.debug(Label.QUALITY, f"Pausing probe for {source_id} in '{logical_channel_title}' ({channel_num}) for pending user streams...")
                        paused = True
                    await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
                    continue
                if paused:
                    self.config.debug(Label.QUALITY, f"Resuming probe for {source_id} in '{logical_channel_title}' ({channel_num}) after pending user streams.")
                    paused = False

                provider_slots = await self.handler.get_provider_slots(provider_alias)
                if not provider_slots:
                    msg = f"Provider slot manager for {provider_alias} not found, cannot run probe for {source_id} in '{logical_channel_title}' ({channel_num})."
                    self.config.error(Label.QUALITY, msg)
                    raise RuntimeError(msg)
                if provider_slots.get_total_slots() <= 0:
                    msg = f"Provider {provider_alias} is configured with 0 slots, cannot run probe for {source_id} in '{logical_channel_title}' ({channel_num})."
                    self.config.warn(Label.QUALITY, msg)
                    raise ValueError(msg)
                if not await provider_slots.try_acquire():
                    await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
                    continue

                task = asyncio.create_task(self._get_stream_info(logical_channel_title, channel_num, source_id, stream_url))
                try:
                    provider_slots.add_background_task(task)
                    stream_info = await task
                except asyncio.CancelledError:
                    self.config.warn(Label.QUALITY, f"ffprobe task for {source_id} in '{logical_channel_title}' ({channel_num}) was cancelled.")
                    if provider_slots.pop_cancelled_task(task):
                        continue  # Retry since we cancelled for a user stream
                    raise
                finally:
                    run_bg(provider_slots.release())
                if not stream_info:
                    return source_id, {"status": "offline", "reason": "No stream info available"}
                return source_id, stream_info

        except asyncio.CancelledError:
            self.config.info(Label.QUALITY, f"Probe task for {source_id} in '{logical_channel_title}' ({channel_num}) was cancelled by slot manager.")
            raise
        except Exception as e:
            self.config.error(Label.QUALITY, f"Unexpected error during probe for {source_id} in '{logical_channel_title}' ({channel_num}): {e}")
            return source_id, {"status": "offline", "reason": f"Probe failed: {e}"}
    
    async def analyze_mapped_sources(self, input_lc_id: LogicalChannelId | None = None) -> None:
        """Finds and probes all mapped sources concurrently."""
        valid_mappings: list[tuple[LogicalChannelId, LogicalChannelTitle, ChannelNum, list[SourceId], DateTimeISO]] = []
        if input_lc_id:
            logical_channel = await self.handler.get_logical_channel_by_id(input_lc_id)
            if not logical_channel:
                self.config.error(Label.QUALITY, f"Logical Channel ID {input_lc_id} not found.")
                return
            logical_channel_title = logical_channel["logical_channel_title"]
            channel_num = logical_channel["channel_num"]
            self.config.info(Label.QUALITY, f"Starting stream quality analysis for '{logical_channel_title}' ({channel_num}).")
            sources = await self.handler.get_mappings_for_logical_channel(input_lc_id)
            if not sources:
                self.config.error(Label.QUALITY, f"No mapped sources found for '{logical_channel_title}' ({channel_num}).")
                return
            valid_mappings.append((input_lc_id, logical_channel_title, channel_num, [source_id for source_id in sources], DateTimeISO("0001-01-01")))
        else:
            self.config.info(Label.QUALITY, "Starting stream quality analysis cycle.")
            all_mappings = await self.handler.copy_channel_mappings_data()
            if not all_mappings:
                self.config.warn(Label.QUALITY, "No mapped sources to analyze.")
                return

            quality_cache = await self.config.get_quality_cache()
            if quality_cache is None:
                self.config.critical(Label.QUALITY, "Quality cache was removed/corrupted after startup, cannot analyze sources.")
                return
            now = datetime.now()
            for logical_channel_id, sources in all_mappings.items():
                logical_channel = await self.handler.get_logical_channel_by_id(logical_channel_id)
                if not logical_channel:
                    self.config.error(Label.QUALITY, f"Logical Channel ID {logical_channel_id} not found in mappings.")
                    continue
                logical_channel_title = logical_channel["logical_channel_title"]
                channel_num = logical_channel["channel_num"]
                if not sources:
                    self.config.debug(Label.QUALITY, f"No valid sources found for '{logical_channel_title}' ({channel_num}).")
                    continue
                min_updated_at = min([quality_cache.get(source_id, {}).get("updated_at", "0001-01-01") for source_id in sources])
                at_max_history = all(len(quality_cache.get(source_id, {}).get("statuses", [])) >= MAX_HISTORY_PER_SOURCE for source_id in sources)
                delta = timedelta(days=MIN_DAYS_AT_MAX_HISTORY) if at_max_history else timedelta(days=MIN_DAYS_AT_NON_MAX_HISTORY)
                if datetime.fromisoformat(min_updated_at) > now - delta:
                    continue
                valid_mappings.append((logical_channel_id, logical_channel_title, channel_num, [source_id for source_id in sources], min_updated_at))
            if not valid_mappings:
                self.config.info(Label.QUALITY, "No sources are due for quality probing.")
                return
            valid_mappings.sort(key=lambda x: x[4])

        for logical_channel_id, logical_channel_title, channel_num, source_ids, _ in valid_mappings:
            tasks: list[Coroutine[Any, Any, tuple[SourceId, ProbeInfo]]] = []
            for source_id in source_ids:
                discovered_source = await self.handler.get_discovered_source(source_id)
                if not discovered_source:
                    self.config.debug(Label.QUALITY, f"'{logical_channel_title}' ({channel_num}) source {source_id} not found in discovered sources.")
                    continue
                provider_slots = await self.handler.get_provider_slots(discovered_source["provider_alias"])
                if not provider_slots:
                    self.config.error(Label.QUALITY, f"Provider slots for {discovered_source['provider_alias']} not found while probing source {source_id} in '{logical_channel_title}' ({channel_num}).")
                    continue
                if provider_slots.get_total_slots() <= 0:
                    self.config.warn(Label.QUALITY, f"Provider {provider_slots.get_alias()} is configured with 0 slots, skipping probing for source {source_id} in '{logical_channel_title}' ({channel_num}).")
                    continue
                tasks.append(
                    self._run_single_probe(
                        logical_channel_title,
                        channel_num,
                        source_id, 
                        discovered_source["stream_url"], 
                        discovered_source["provider_alias"]
                    )
                )
            if not tasks:
                self.config.debug(Label.QUALITY, f"No valid sources found to probe for '{logical_channel_title}' ({channel_num}).")
                continue

            raw_results = await asyncio.gather(*tasks, return_exceptions=True)
            stream_infos: list[tuple[SourceId, ProbeInfo]] = []
            for raw_result in raw_results:
                if isinstance(raw_result, BaseException):
                    if not isinstance(raw_result, asyncio.CancelledError) and not isinstance(raw_result, Exception):
                        self.config.error(Label.QUALITY, f"Error probing source for '{logical_channel_title}' ({channel_num}): {raw_result}")
                    continue
                stream_infos.append(raw_result)

            async with self._mutex:
                quality_cache = await self.config.get_quality_cache()
                if quality_cache is None:
                    self.config.critical(Label.QUALITY, f"Quality cache was removed/corrupted after startup, stopping analysis at '{logical_channel_title}' ({channel_num}).")
                    return
                modified_cache = QualityCacheDataImpl({})
                for source_id, result in stream_infos:
                    if source_id not in quality_cache:
                        if not await self.handler.get_discovered_source(source_id):
                            self.config.warn(Label.QUALITY, f"'{logical_channel_title}' ({channel_num}) source {source_id} not found in discovered sources, skipping.")
                            continue  # Dead mapping that was removed after the start of this analysis
                        quality_cache[source_id] = QualityInfoImpl({
                            "updated_at": DateTimeISO(datetime.now().isoformat()), "statuses": [], "widths": [],
                            "heights": [], "bitrates": [], "framerates": []
                        })
                    source_entry = quality_cache[source_id]

                    source_entry["updated_at"] = DateTimeISO(datetime.now().isoformat())
                    if result["status"] == "online":
                        source_entry["statuses"].append("online")
                        source_entry["widths"].append(result["width"])
                        source_entry["heights"].append(result["height"])
                        source_entry["bitrates"].append(result["bitrate"])
                        source_entry["framerates"].append(result["framerate"])
                    else:
                        self.config.warn(Label.QUALITY, f"'{logical_channel_title}' ({channel_num}) source {source_id} is offline: {result["reason"]}")
                        source_entry["statuses"].append("offline")

                    if len(source_entry["statuses"]) > MAX_HISTORY_PER_SOURCE:
                        source_entry["statuses"] = source_entry["statuses"][-MAX_HISTORY_PER_SOURCE:]
                        source_entry["widths"] = source_entry["widths"][-MAX_HISTORY_PER_SOURCE:]
                        source_entry["heights"] = source_entry["heights"][-MAX_HISTORY_PER_SOURCE:]
                        source_entry["bitrates"] = source_entry["bitrates"][-MAX_HISTORY_PER_SOURCE:]
                        source_entry["framerates"] = source_entry["framerates"][-MAX_HISTORY_PER_SOURCE:]
                    modified_cache[source_id] = source_entry
                if not await self.config.save_quality_cache(quality_cache):
                    self.config.critical(Label.QUALITY, f"Failed to save quality cache, stopping analysis at '{logical_channel_title}' ({channel_num}).")
                    return
                self._build_quality_scores(modified_cache)
        if input_lc_id:
            self.config.info(Label.QUALITY, f"Completed analysis for {len(valid_mappings[0][3])} mappings(s) in '{valid_mappings[0][1]}' ({valid_mappings[0][2]}).")

    def _build_quality_scores(self, quality_cache: QualityCacheData) -> None:
        """Calculates quality scores and updates the internal state."""
        for source_id, cache_entry in quality_cache.items():
            avg_width = Width(sum(cache_entry["widths"]) / len(cache_entry["widths"]) if cache_entry["widths"] else 0)
            avg_height = Height(sum(cache_entry["heights"]) / len(cache_entry["heights"]) if cache_entry["heights"] else 0)
            avg_bitrate = Bitrate(sum(cache_entry["bitrates"]) / len(cache_entry["bitrates"]) if cache_entry["bitrates"] else 0)
            avg_framerate = Framerate(sum(cache_entry["framerates"]) / len(cache_entry["framerates"]) if cache_entry["framerates"] else 0)
            uptime = Percent(sum(1 for s in cache_entry["statuses"] if s == "online") / len(cache_entry["statuses"]) if cache_entry["statuses"] else 0)
            
            height_score = ResolutionScore(RESOLUTION_WEIGHT * min(avg_height / float(RESOLUTION_NORM), 1.0))
            bitrate_score = BitrateScore(BITRATE_WEIGHT * min(avg_bitrate / float(BITRATE_NORM), 1.0))
            framerate_score = FramerateScore(FRAMERATE_WEIGHT * min(avg_framerate / float(FRAMERATE_NORM), 1.0))
            uptime_score = UptimeScore(UPTIME_WEIGHT * uptime)

            new_quality_scores = QualityScoresImpl({**self._quality_scores})
            new_quality_scores[source_id] = {
                "width": avg_width, "height": avg_height, "bitrate": avg_bitrate,
                "framerate": avg_framerate, "uptime": uptime, "resolution_score": height_score,
                "bitrate_score": bitrate_score, "framerate_score": framerate_score,
                "uptime_score": uptime_score,
                "total_score": TotalScore(height_score + bitrate_score + framerate_score + uptime_score),
            }
            self._quality_scores = new_quality_scores
