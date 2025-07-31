import asyncio
import json
from datetime import datetime, timedelta
from typing import Coroutine, Final, NoReturn, Any, Self, cast

from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.utils import (NEXUS_STREAM_USER_AGENT, Bitrate, BitrateScore, DateTimeISO, Framerate, FramerateScore, Height, Label, LogicalChannelId,
                                Percent, ProbeInfo, ProbeSuccess, ProviderAlias, QualityInfo, QualityInfoMutable, QualityScores, QualityScoresMutable, ResolutionScore, ServiceQualityCacheData,
                                ServiceQualityCacheDataMutable, SourceServiceId, StreamURL, TotalScore, UptimeScore, Width, run_bg)

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
MAX_HISTORY_PER_SERVICE: Final[int] = 10
MIN_DAYS_AT_MAX_HISTORY: Final[int] = 7
MIN_DAYS_AT_NON_MAX_HISTORY: Final[int] = 1


class QualityMonitor:
    __slots__ = (
        'config', 'handler', '_mutex', '_quality_scores',
        'quality_monitor_task',
    )
    
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self._mutex: asyncio.Lock = asyncio.Lock()
        self._quality_scores: QualityScores = QualityScores({})
        self.quality_monitor_task: asyncio.Task[NoReturn]

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler) -> Self:
        """Asynchronous factory for creating and initializing a QualityMonitor instance."""
        instance = cls(config, handler)
        instance._build_quality_scores(await config.get_service_quality_cache())
        return instance

    async def get_quality_scores(self) -> QualityScores:
        """Returns the current quality scores for all services asynchronously."""
        async with self._mutex:
            return QualityScores(cast(QualityScoresMutable, self._quality_scores).copy())

    async def remove_source_service(self, service_id: SourceServiceId) -> None:
        """Removes a source service from the quality scores and cache."""
        async with self._mutex:
            if service_id in self._quality_scores:
                del cast(QualityScoresMutable, self._quality_scores)[service_id]
            quality_cache = await self.config.get_service_quality_cache()
            if service_id in quality_cache:
                del cast(ServiceQualityCacheDataMutable, quality_cache)[service_id]
                await self.config.save_service_quality_cache(quality_cache)

    async def _get_stream_info(self, service_id: SourceServiceId, stream_url: StreamURL) -> ProbeSuccess | None:
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
                self.config.warn(Label.QUALITY, f"ffprobe for {service_id} failed with code {proc.returncode}: {stderr.decode()}".replace(stream_url, "{{stream_url}}").strip())
                return
            info = json.loads(stdout)

        except asyncio.TimeoutError:
            self.config.warn(Label.QUALITY, f"ffprobe for {service_id} timed out.")
            return
        except Exception as e:
            self.config.error(Label.QUALITY, f"Failed to parse ffprobe output for {service_id}: {e}")
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

    async def _run_single_probe(self, service_id: SourceServiceId, stream_url: StreamURL, provider_alias: ProviderAlias) -> tuple[SourceServiceId, ProbeInfo]:
        """
        Probes a single stream, persistently trying to acquire a slot, and ensures
        all resources are cleaned up upon completion, failure, or cancellation.
        """
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            msg = f"Provider slot manager for {provider_alias} not found."
            self.config.error(Label.QUALITY, msg)
            raise RuntimeError(msg)
            
        current_task = asyncio.current_task()
        if not current_task:
            msg = "Current task is None, cannot run probe without a task context"
            self.config.error(Label.QUALITY, msg)
            raise RuntimeError(msg)

        try:
            paused = False
            while True:
                if await self.handler.get_pending_stream_count() > 0:
                    if not paused:
                        self.config.debug(Label.QUALITY, f"Pausing probe for {service_id} for pending user streams...")
                        paused = True
                    await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
                    continue
                if paused:
                    self.config.debug(Label.QUALITY, f"Resuming probe for {service_id} after pending user streams.")
                    paused = False

                if not await provider_slots.try_acquire():
                    await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
                    continue

                task = asyncio.create_task(self._get_stream_info(service_id, stream_url))
                try:
                    provider_slots.add_background_task(task)
                    stream_info = await task
                except asyncio.CancelledError:
                    self.config.warn(Label.QUALITY, f"ffprobe task for {service_id} was cancelled.")
                    if provider_slots.pop_cancelled_task(task):
                        continue  # Retry since we cancelled for a user stream
                    raise
                finally:
                    run_bg(provider_slots.release())
                if not stream_info:
                    return service_id, {"status": "offline", "reason": "No stream info available"}
                return service_id, stream_info

        except asyncio.CancelledError:
            self.config.info(Label.QUALITY, f"Probe task for {service_id} was cancelled by slot manager.")
            raise
        except Exception as e:
            self.config.error(Label.QUALITY, f"Unexpected error during probe for {service_id}: {e}")
            return service_id, {"status": "offline", "reason": f"Probe failed: {e}"}
    
    async def analyze_mapped_services(self, input_lc_id: LogicalChannelId | None = None) -> None:
        """Finds and probes all mapped services concurrently."""
        valid_mappings: list[tuple[LogicalChannelId, list[SourceServiceId], DateTimeISO]] = []
        if input_lc_id:
            self.config.info(Label.QUALITY, f"Starting stream quality analysis for Logical Channel ID {input_lc_id}.")
            services = self.handler.get_mappings_for_logical_channel(input_lc_id)
            if not services:
                self.config.error(Label.QUALITY, f"No mapped services found for Logical Channel ID {input_lc_id}.")
                return
            valid_mappings.append((input_lc_id, [service["source_service_id"] for service in services], DateTimeISO("0001-01-01")))
        else:
            self.config.info(Label.QUALITY, "Starting stream quality analysis cycle.")
            all_mappings = self.handler.channel_mappings_data
            if not all_mappings:
                self.config.warn(Label.QUALITY, "No mapped services to analyze.")
                return

            quality_cache = await self.config.get_service_quality_cache()
            now = datetime.now()
            for logical_channel_id, services in all_mappings.items():
                if not services:
                    self.config.debug(Label.QUALITY, f"No valid services found for Logical Channel ID {logical_channel_id}.")
                    continue
                min_updated_at = min([quality_cache.get(service["source_service_id"], {}).get("updated_at", "0001-01-01") for service in services])
                at_max_history = all(len(quality_cache.get(service["source_service_id"], {}).get("statuses", [])) >= MAX_HISTORY_PER_SERVICE for service in services)
                delta = timedelta(days=MIN_DAYS_AT_MAX_HISTORY) if at_max_history else timedelta(days=MIN_DAYS_AT_NON_MAX_HISTORY)
                if datetime.fromisoformat(min_updated_at) > now - delta:
                    continue
                valid_mappings.append((logical_channel_id, [service["source_service_id"] for service in services], min_updated_at))
            if not valid_mappings:
                self.config.info(Label.QUALITY, "No services are due for quality probing.")
                return
            valid_mappings.sort(key=lambda x: x[2])

        for logical_channel_id, service_ids, _ in valid_mappings:
            tasks: list[Coroutine[Any, Any, tuple[SourceServiceId, ProbeInfo]]] = []
            for service_id in service_ids:                
                service_details = self.handler.discovered_source_services_data.get(service_id)
                if not service_details:
                    self.config.debug(Label.QUALITY, f"Service {service_id} not found in discovered services.")
                    continue
                tasks.append(
                    self._run_single_probe(
                        service_id, 
                        service_details["actual_stream_url"], 
                        ProviderAlias(service_details["provider_alias"])
                    )
                )
            if not tasks:
                self.config.debug(Label.QUALITY, f"No valid services found to probe for Logical Channel ID {logical_channel_id}.")
                continue

            raw_results = await asyncio.gather(*tasks, return_exceptions=True)
            stream_infos: list[tuple[SourceServiceId, ProbeInfo]] = []
            for raw_result in raw_results:
                if isinstance(raw_result, BaseException):
                    if not isinstance(raw_result, asyncio.CancelledError) and not isinstance(raw_result, Exception):
                        self.config.error(Label.QUALITY, f"Error probing service: {raw_result}")
                    continue
                stream_infos.append(raw_result)

            async with self._mutex:
                quality_cache = cast(ServiceQualityCacheDataMutable, await self.config.get_service_quality_cache())
                modified_cache: ServiceQualityCacheDataMutable = ServiceQualityCacheDataMutable({})
                for service_id, result in stream_infos:
                    if service_id not in quality_cache:
                        quality_cache[service_id] = cast(QualityInfo, QualityInfoMutable({
                            "updated_at": DateTimeISO(datetime.now().isoformat()), "statuses": [], "widths": [],
                            "heights": [], "bitrates": [], "framerates": []
                        }))
                    service_entry = cast(QualityInfoMutable, quality_cache[service_id])

                    service_entry["updated_at"] = DateTimeISO(datetime.now().isoformat())
                    if result["status"] == "online":
                        service_entry["statuses"].append("online")
                        service_entry["widths"].append(result["width"])
                        service_entry["heights"].append(result["height"])
                        service_entry["bitrates"].append(result["bitrate"])
                        service_entry["framerates"].append(result["framerate"])
                    else:
                        self.config.warn(Label.QUALITY, f"Logical Channel ID {logical_channel_id} service {service_id} is offline: {result.get('reason', 'Unknown')}")
                        service_entry["statuses"].append("offline")

                    if len(service_entry["statuses"]) > MAX_HISTORY_PER_SERVICE:
                        service_entry["statuses"] = service_entry["statuses"][-MAX_HISTORY_PER_SERVICE:]
                        service_entry["widths"] = service_entry["widths"][-MAX_HISTORY_PER_SERVICE:]
                        service_entry["heights"] = service_entry["heights"][-MAX_HISTORY_PER_SERVICE:]
                        service_entry["bitrates"] = service_entry["bitrates"][-MAX_HISTORY_PER_SERVICE:]
                        service_entry["framerates"] = service_entry["framerates"][-MAX_HISTORY_PER_SERVICE:]
                    modified_cache[service_id] = cast(QualityInfo, service_entry)

                await self.config.save_service_quality_cache(cast(ServiceQualityCacheData, quality_cache))
                self._build_quality_scores(cast(ServiceQualityCacheData, modified_cache))
        if input_lc_id:
            self.config.info(Label.QUALITY, f"Completed analysis for {len(valid_mappings[0][1])} mappings(s) in Logical Channel ID {input_lc_id}.")

    def _build_quality_scores(self, quality_cache: ServiceQualityCacheData) -> None:
        """Calculates quality scores and updates the internal state."""
        for service_id, cache_entry in quality_cache.items():
            avg_width = Width(sum(cache_entry.get("widths", [])) / len(cache_entry["widths"]) if cache_entry.get("widths") else 0)
            avg_height = Height(sum(cache_entry.get("heights", [])) / len(cache_entry["heights"]) if cache_entry.get("heights") else 0)
            avg_bitrate = Bitrate(sum(cache_entry.get("bitrates", [])) / len(cache_entry["bitrates"]) if cache_entry.get("bitrates") else 0)
            avg_framerate = Framerate(sum(cache_entry.get("framerates", [])) / len(cache_entry["framerates"]) if cache_entry.get("framerates") else 0)
            statuses = cache_entry.get("statuses", [])
            uptime = Percent(sum(1 for s in statuses if s == "online") / len(statuses) if statuses else 0)
            
            height_score = ResolutionScore(RESOLUTION_WEIGHT * min(avg_height / float(RESOLUTION_NORM), 1.0))
            bitrate_score = BitrateScore(BITRATE_WEIGHT * min(avg_bitrate / float(BITRATE_NORM), 1.0))
            framerate_score = FramerateScore(FRAMERATE_WEIGHT * min(avg_framerate / float(FRAMERATE_NORM), 1.0))
            uptime_score = UptimeScore(UPTIME_WEIGHT * uptime)
            
            cast(QualityScoresMutable, self._quality_scores)[service_id] = {
                "width": avg_width, "height": avg_height, "bitrate": avg_bitrate,
                "framerate": avg_framerate, "uptime": uptime, "resolution_score": height_score,
                "bitrate_score": bitrate_score, "framerate_score": framerate_score,
                "uptime_score": uptime_score,
                "total_score": TotalScore(height_score + bitrate_score + framerate_score + uptime_score),
            }
