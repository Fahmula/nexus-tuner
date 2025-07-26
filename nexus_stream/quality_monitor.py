import asyncio
import json
from datetime import datetime, timedelta
from typing import Coroutine, NoReturn, Any, Self

from nexus_stream.config import Config, NEXUS_STREAM_USER_AGENT
from nexus_stream.handler import ChannelHandler
from nexus_stream.slots import ProviderName

# --- Constants ---
RESOLUTION_WEIGHT = 50
BITRATE_WEIGHT = 30
FRAMERATE_WEIGHT = 20
UPTIME_WEIGHT = 0

RESOLUTION_NORM = 2160
BITRATE_NORM = 12_000_000
FRAMERATE_NORM = 60

QUALITY_MONITOR_INTERVAL = 86400
QUALITY_MONITOR_TIMEOUT = 5
MAX_HISTORY_PER_SERVICE = 10
MIN_DAYS_AT_MAX_HISTORY = 7
MIN_DAYS_AT_NON_MAX_HISTORY = 1


class QualityMonitor:
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        self.config = config
        self.handler = handler
        self._mutex = asyncio.Lock()
        self._quality_scores: dict[str, dict[str, float]] = {}

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler) -> Self:
        """Asynchronous factory for creating and initializing a QualityMonitor instance."""
        instance = cls(config, handler)
        config.log_message("Quality Monitor: Building quality scores from cache.", level="DEBUG")
        instance._build_quality_scores(await config.get_service_quality_cache())
        config.log_message("Quality Monitor: Quality scores built successfully.", level="DEBUG")
        return instance

    async def get_quality_scores(self) -> dict[str, dict[str, float]]:
        """Returns the current quality scores for all services asynchronously."""
        async with self._mutex:
            return self._quality_scores.copy()

    async def remove_source_service(self, service_id: str) -> None:
        """Removes a source service from the quality scores and cache."""
        async with self._mutex:
            if service_id in self._quality_scores:
                del self._quality_scores[service_id]
            quality_cache = await self.config.get_service_quality_cache()
            if service_id in quality_cache:
                del quality_cache[service_id]
                await self.config.save_service_quality_cache(quality_cache)

    async def analyze_logical_channel(self, input_lc_id: str) -> None:
        """Analyzes the quality of services mapped to a specific logical channel."""
        await self._analyze_mapped_services(input_lc_id)

    async def run(self) -> NoReturn:
        """The main execution loop for the monitor, run as an asyncio task."""
        self.config.log_message("Quality Monitor: Task started.", level="INFO")
        while True:
            try:
                await self._analyze_mapped_services()
            except Exception as e:
                self.config.log_message(f"Quality Monitor: Unhandled exception in main check loop: {e}", level="CRITICAL")
            self.config.log_message(f"Quality Monitor: Cycle complete. Sleeping for {QUALITY_MONITOR_INTERVAL} seconds.", level="INFO")
            await asyncio.sleep(QUALITY_MONITOR_INTERVAL)

    async def _get_stream_info(self, service_id: str, service_url: str) -> dict[str, Any] | None:
        """
        Extracts stream information using ffprobe, ensuring the subprocess is
        terminated on timeout or cancellation.
        """
        cmd = [
            "ffprobe",
            "-v", "error",
            "-user_agent", NEXUS_STREAM_USER_AGENT,
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,r_frame_rate",
            "-show_entries", "packet=pts_time,size",
            "-read_intervals", f"%+{QUALITY_MONITOR_TIMEOUT}",
            "-of", "json",
            service_url
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
                self.config.log_message(f"Quality Monitor: ffprobe for {service_id} failed with code {proc.returncode}: {stderr.decode()}".replace(service_url, "{{service_url}}").strip(), level="WARN")
                return None
            info = json.loads(stdout)

        except asyncio.TimeoutError:
            self.config.log_message(f"Quality Monitor: ffprobe for {service_id} timed out.", level="WARN")
            return None
        except asyncio.CancelledError:
            self.config.log_message(f"Quality Monitor: ffprobe task for {service_id} was cancelled.", level="WARN")
            raise
        except Exception as e:
            self.config.log_message(f"Quality Monitor: Failed to parse ffprobe output for {service_id}: {e}", level="ERROR")
            return None
        finally:
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                    await proc.wait()
                except ProcessLookupError:
                    pass

        stream = info.get('streams', [{}])[0]
        width = int(stream.get('width', 0))
        height = int(stream.get('height', 0))
        fr_str = stream.get('r_frame_rate', '0/1')
        nums = fr_str.split('/')
        frame_rate = float(nums[0]) / float(nums[1]) if len(nums) == 2 and nums[1] != '0' else float(nums[0])

        packets = info.get('packets', [])
        if not packets: return None

        sizes, times = zip(*((float(pkt['size']), float(pkt['pts_time'])) for pkt in packets))
        duration_s = max(times) - min(times)
        if duration_s <= 0: return None
        
        total_bytes = sum(sizes)
        bitrate = (total_bytes * 8) / duration_s

        return {"status": "online", "width": width, "height": height, "bitrate": bitrate, "framerate": frame_rate}

    async def _run_single_probe(self, service_id: str, service_url: str, provider_alias: ProviderName) -> tuple[str, dict[str, str | float | int]]:
        """
        Probes a single stream, persistently trying to acquire a slot, and ensures
        all resources are cleaned up upon completion, failure, or cancellation.
        """
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            return service_id, {"status": "error", "reason": "Provider slot manager not found"}

        current_task = asyncio.current_task()
        if not current_task:
            raise RuntimeError("Current task is None, cannot run probe without a task context")

        slot_acquired = False
        try:
            paused = False
            while True:
                if await self.handler.get_pending_stream_count() > 0:
                    if not paused:
                        self.config.log_message(f"Quality Monitor: Pausing probe for {service_id} for user streams...", level="DEBUG")
                        paused = True
                    await asyncio.sleep(1)
                    continue
                if paused:
                    self.config.log_message(f"Quality Monitor: Resuming probe for {service_id} after user streams.", level="DEBUG")
                    paused = False

                try:
                    await provider_slots.acquire_background_slot(current_task)
                    slot_acquired = True
                    break
                except asyncio.TimeoutError:
                    await asyncio.sleep(1)

            stream_info = await self._get_stream_info(service_id, service_url)
            
            if not stream_info:
                return service_id, {"status": "offline", "reason": "No stream info available"}
            return service_id, stream_info

        except asyncio.CancelledError:
            self.config.log_message(f"Quality Monitor: Probe task for {service_id} was cancelled by slot manager.", level="INFO")
            raise
        except Exception as e:
            self.config.log_message(f"Quality Monitor: Unexpected error during probe for {service_id}: {e}", level="ERROR")
            return service_id, {"status": "offline", "reason": f"Probe failed: {e}"}
        finally:
            if slot_acquired:
                await provider_slots.release_background_slot(current_task)
    
    async def _analyze_mapped_services(self, input_lc_id: str | None = None) -> None:
        """Finds and probes all mapped services concurrently."""
        valid_mappings: list[tuple[str, list[str], str]] = []
        if input_lc_id:
            self.config.log_message(f"Quality Monitor: Starting stream quality analysis for Logical Channel ID {input_lc_id}.", level="INFO")
            services = self.handler.get_mappings_for_logical_channel(input_lc_id)
            if not services:
                self.config.log_message(f"Quality Monitor: No mapped services found for Logical Channel ID {input_lc_id}.", level="ERROR")
                return
            valid_mappings.append((input_lc_id, [service["source_service_id"] for service in services], "0001-01-01"))
        else:
            self.config.log_message("Quality Monitor: Starting stream quality analysis cycle.", level="INFO")
            all_mappings = self.handler.channel_mappings_data
            if not all_mappings:
                self.config.log_message("Quality Monitor: No mapped services to analyze.", level="WARN")
                return

            quality_cache = await self.config.get_service_quality_cache()
            now = datetime.now()
            for logical_channel_id, services in all_mappings.items():
                if not services:
                    self.config.log_message(f"Quality Monitor: No valid services found for Logical Channel ID {logical_channel_id}.", level="DEBUG")
                    continue
                min_updated_at = min([quality_cache.get(service["source_service_id"], {}).get("updated_at", "0001-01-01") for service in services])
                at_max_history = all(len(quality_cache.get(service["source_service_id"], {}).get("statuses", [])) >= MAX_HISTORY_PER_SERVICE for service in services)
                delta = timedelta(days=MIN_DAYS_AT_MAX_HISTORY) if at_max_history else timedelta(days=MIN_DAYS_AT_NON_MAX_HISTORY)
                if datetime.fromisoformat(min_updated_at) > now - delta:
                    continue
                valid_mappings.append((logical_channel_id, [service["source_service_id"] for service in services], min_updated_at))
            if not valid_mappings:
                self.config.log_message("Quality Monitor: No services are due for quality probing.")
                return
            valid_mappings.sort(key=lambda x: x[2])

        for logical_channel_id, service_ids, _ in valid_mappings:
            tasks: list[Coroutine[Any, Any, tuple[str, dict[str, str | float | int]]]] = []
            for service_id in service_ids:                
                service_details = self.handler.discovered_source_services_data.get(service_id)
                if not service_details:
                    self.config.log_message(f"Quality Monitor: Service {service_id} not found in discovered services.", level="DEBUG")
                    continue
                tasks.append(
                    self._run_single_probe(
                        service_id, 
                        service_details["actual_stream_url"], 
                        ProviderName(service_details["provider_alias"])
                    )
                )
            if not tasks:
                self.config.log_message(f"Quality Monitor: No valid services found to probe for Logical Channel ID {logical_channel_id}.", level="DEBUG")
                continue

            stream_infos = await asyncio.gather(*tasks)
            async with self._mutex:
                quality_cache = await self.config.get_service_quality_cache()
                modified_cache: dict[str, dict[str, list[Any]]] = {}
                for service_id, result in stream_infos:
                    if service_id not in quality_cache:
                        quality_cache[service_id] = {
                            "updated_at": datetime.now().isoformat(), "statuses": [], "widths": [],
                            "heights": [], "bitrates": [], "framerates": []
                        }
                    service_entry = quality_cache[service_id]

                    service_entry["updated_at"] = datetime.now().isoformat()
                    if result['status'] == 'online':
                        service_entry["statuses"].append("online")
                        for key in ["widths", "heights", "bitrates", "framerates"]:
                            if key not in service_entry:
                                service_entry[key] = []
                            service_entry[key].append(result[key[:-1]])
                    else:
                        self.config.log_message(f"Quality Monitor: Logical Channel ID {logical_channel_id} service {service_id} is offline: {result.get('reason', 'Unknown')}", level="WARN")
                        service_entry["statuses"].append("offline")

                    for key in ["statuses", "widths", "heights", "bitrates", "framerates"]:
                        if len(service_entry[key]) > MAX_HISTORY_PER_SERVICE:
                            service_entry[key] = service_entry[key][-MAX_HISTORY_PER_SERVICE:]
                    modified_cache[service_id] = service_entry

                await self.config.save_service_quality_cache(quality_cache)
                self._build_quality_scores(modified_cache)
        if input_lc_id:
            self.config.log_message(f"Quality Monitor: Completed analysis for Logical Channel ID {input_lc_id}.", level="INFO")

    def _build_quality_scores(self, quality_cache: dict[str, dict[str, list[Any]]]) -> None:
        """Calculates quality scores and updates the internal state."""
        for service_id, cache_entry in quality_cache.items():
            avg_width = sum(cache_entry.get("widths", [])) / len(cache_entry["widths"]) if cache_entry.get("widths") else 0.0
            avg_height = sum(cache_entry.get("heights", [])) / len(cache_entry["heights"]) if cache_entry.get("heights") else 0.0
            avg_bitrate = sum(cache_entry.get("bitrates", [])) / len(cache_entry["bitrates"]) if cache_entry.get("bitrates") else 0.0
            avg_framerate = sum(cache_entry.get("framerates", [])) / len(cache_entry["framerates"]) if cache_entry.get("framerates") else 0.0
            statuses = cache_entry.get("statuses", [])
            uptime = sum(1 for s in statuses if s == "online") / len(statuses) if statuses else 0.0
            
            height_score = RESOLUTION_WEIGHT * min(avg_height / float(RESOLUTION_NORM), 1.0)
            bitrate_score = BITRATE_WEIGHT * min(avg_bitrate / float(BITRATE_NORM), 1.0)
            framerate_score = FRAMERATE_WEIGHT * min(avg_framerate / float(FRAMERATE_NORM), 1.0)
            uptime_score = UPTIME_WEIGHT * uptime
            
            self._quality_scores[service_id] = {
                "width": avg_width, "height": avg_height, "bitrate": avg_bitrate,
                "framerate": avg_framerate, "uptime": uptime, "resolution_score": height_score,
                "bitrate_score": bitrate_score, "framerate_score": framerate_score,
                "uptime_score": uptime_score,
                "total_score": height_score + bitrate_score + framerate_score + uptime_score,
            }
