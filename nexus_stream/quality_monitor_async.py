import asyncio
import json
from datetime import datetime
from typing import NoReturn, Any

# Refactor Note: Replaced threading with asyncio for non-blocking concurrency.
# The ChannelHandler and Config imports point to the async versions.
from nexus_stream.config_async import Config
from nexus_stream.handler_async import ChannelHandler
from nexus_stream.slots_async import ProviderName

# --- Constants ---
RESOLUTION_WEIGHT = 50
BITRATE_WEIGHT = 30
FRAMERATE_WEIGHT = 20
UPTIME_WEIGHT = 0

RESOLTUION_NORM = 2160
BITRATE_NORM = 12_000_000
FRAMERATE_NORM = 60

QUALITY_MONITOR_INTERVAL = 86400
QUALITY_MONITOR_TIMEOUT = 5
MAX_HISTORY_PER_SERVICE = 10


class QualityMonitor:
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        self.config = config
        self.handler = handler
        # Refactor Note: Replaced threading.Lock with asyncio.Lock for use in async contexts.
        self._mutex = asyncio.Lock()
        self._quality_scores: dict[str, dict[str, float]] = {}
        # Refactor Note: The background task is no longer a thread started in __init__.
        # It will be launched as an asyncio.Task by the main application.

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler) -> "QualityMonitor":
        """Asynchronous factory for creating and initializing a QualityMonitor instance."""
        instance = cls(config, handler)
        # Refactor Note: Initial cache loading is now an async operation.
        initial_cache = await config.get_service_quality_cache()
        await instance._build_quality_scores(initial_cache)
        return instance

    async def get_quality_scores(self) -> dict[str, dict[str, float]]:
        """Returns the current quality scores for all services asynchronously."""
        async with self._mutex:
            return self._quality_scores.copy()

    # Refactor Note: Renamed from _run to run and made async. This is the main entry point for the background task.
    async def run(self) -> NoReturn:
        """The main execution loop for the monitor, run as an asyncio task."""
        self.config.log_message("Quality Monitor task started.", level="INFO")
        while True:
            # Refactor Note: Replaced time.sleep with await asyncio.sleep for non-blocking delay.
            await asyncio.sleep(QUALITY_MONITOR_INTERVAL)
            try:
                # Refactor Note: The main analysis logic is now an awaitable coroutine.
                await self._analyze_mapped_services()
            except Exception as e:
                self.config.log_message(f"Quality Monitor: Unhandled exception in main check loop: {e}", level="CRITICAL")
            self.config.log_message(f"Quality Monitor: Cycle complete. Sleeping for {QUALITY_MONITOR_INTERVAL} seconds.", level="INFO")

    # Refactor Note: This method is now async and uses asyncio.create_subprocess_exec.
    async def _get_stream_info(self, service_url: str) -> dict[str, Any] | None:
        """Extracts stream information using ffprobe asynchronously."""
        duration = QUALITY_MONITOR_TIMEOUT
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,r_frame_rate",
            "-show_entries", "packet=pts_time,size",
            "-read_intervals", f"%+{duration}",
            "-of", "json",
            service_url
        ]

        # Refactor Note: Replaced blocking subprocess.run with non-blocking asyncio subprocess.
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            # Refactor Note: await proc.communicate() waits for the process to finish without blocking the event loop.
            # asyncio.wait_for is used to enforce a timeout.
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=duration + 3)
            if proc.returncode != 0:
                self.config.log_message(f"ffprobe for {service_url} failed with code {proc.returncode}: {stderr.decode()}", level="WARN")
                return None
            info = json.loads(stdout)
        except asyncio.TimeoutError:
            self.config.log_message(f"ffprobe for {service_url} timed out.", level="WARN")
            proc.kill() # Ensure the process is terminated
            await proc.wait()
            return None
        except (json.JSONDecodeError, IndexError) as e:
            self.config.log_message(f"Failed to parse ffprobe output for {service_url}: {e}", level="ERROR")
            return None

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

    # Refactor Note: This method is now async.
    async def _run_single_probe(self, service_id: str, service_url: str, provider_alias: ProviderName) -> tuple[str, dict[str, str | float | int]]:
        """Probes a single stream URL asynchronously after acquiring a provider slot."""
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            return service_id, {"status": "error", "reason": "Slot not found"}

        current_task = asyncio.current_task()
        
        slot_acquired = False
        while True:
            # Non-blocking sleep to avoid overwhelming the provider.
            await asyncio.sleep(3)
            if await self.handler.get_pending_stream_count() == 0:
                try:
                    slot_acquired = await provider_slots.acquire_background_slot(current_task)
                    if slot_acquired:
                        break
                except asyncio.TimeoutError:
                    pass 
            await asyncio.sleep(1)

        try:
            # Refactor Note: Awaiting the async _get_stream_info method.
            stream_info = await self._get_stream_info(service_url)
            if not stream_info:
                return service_id, {"status": "offline", "reason": "No stream info available"}
            return service_id, stream_info
        except Exception as e:
            # Catch any unexpected errors during the probe itself.
            return service_id, {"status": "offline", "reason": f"Probe failed: {e}"}
        finally:
            if slot_acquired:
                await provider_slots.release_background_slot(current_task)
    
    # Refactor Note: This method is now fully async.
    async def _analyze_mapped_services(self) -> None:
        """Finds and probes all mapped services concurrently using asyncio.gather."""
        self.config.log_message("Quality Monitor: Starting stream quality analysis cycle.", level="INFO")
        
        all_mappings = self.handler.channel_mappings_data_from_json.values()
        if not all_mappings:
            self.config.log_message("Quality Monitor: No mapped services to analyze.", level="INFO")
            return

        tasks = []
        seen_tasks = set()
        for services in all_mappings:
            for service in services:
                service_id = service["source_service_id"]
                if service_id in seen_tasks:
                    continue
                
                service_details = self.handler.discovered_source_services.get(service_id)
                if not service_details:
                    continue
                
                seen_tasks.add(service_id)
                # Refactor Note: Create a coroutine object for each probe task.
                tasks.append(
                    self._run_single_probe(
                        service_id, 
                        service_details['actual_stream_url'], 
                        ProviderName(service_details['provider_alias'])
                    )
                )
        
        if not tasks:
            self.config.log_message("Quality Monitor: No valid services found to probe.", level="INFO")
            return

        # Refactor Note: Replaced ThreadPoolExecutor with asyncio.gather for non-blocking concurrency.
        # This runs all probe tasks concurrently on the single event loop.
        all_results = await asyncio.gather(*tasks)

        self.config.log_message(f"Quality Monitor: {len(all_results)} probes complete. Processing results.", level="DEBUG")

        # Refactor Note: Awaiting the async config methods.
        quality_cache = await self.config.get_service_quality_cache()

        for service_id, result in all_results:
            service_entry = quality_cache.setdefault(service_id, {
                "updated_at": datetime.now().isoformat(), "statuses": [], "widths": [],
                "heights": [], "bitrates": [], "framerates": []
            })

            service_entry["updated_at"] = datetime.now().isoformat()
            if result['status'] == 'online':
                service_entry["statuses"].append("online")
                for key in ["widths", "heights", "bitrates", "framerates"]:
                    metric_key = key[:-1] # e.g., "widths" -> "width"
                    if value := result.get(metric_key):
                        service_entry[key].append(value)
            else:
                self.config.log_message(f"Quality Monitor: Service {service_id} is offline: {result.get('reason', 'Unknown')}", level="WARN")
                service_entry["statuses"].append("offline")

            # Prune history for all metrics
            for key in ["statuses", "widths", "heights", "bitrates", "framerates"]:
                if len(service_entry[key]) > MAX_HISTORY_PER_SERVICE:
                    service_entry[key] = service_entry[key][-MAX_HISTORY_PER_SERVICE:]

        await self.config.save_service_quality_cache(quality_cache)
        await self._build_quality_scores(quality_cache)

    # Refactor Note: This method is now async to use the async lock.
    async def _build_quality_scores(self, quality_cache: dict[str, dict[str, list]]) -> None:
        """Calculates quality scores asynchronously and updates the internal state."""
        self.config.log_message("Quality Monitor: Building quality scores from cache.", level="DEBUG")
        
        quality_scores: dict[str, dict[str, float]] = {}
        for service_id, cache_entry in quality_cache.items():
            # This logic is CPU-bound and remains unchanged.
            avg_width = sum(cache_entry.get("widths", [])) / len(cache_entry["widths"]) if cache_entry.get("widths") else 0.0
            avg_height = sum(cache_entry.get("heights", [])) / len(cache_entry["heights"]) if cache_entry.get("heights") else 0.0
            avg_bitrate = sum(cache_entry.get("bitrates", [])) / len(cache_entry["bitrates"]) if cache_entry.get("bitrates") else 0.0
            avg_framerate = sum(cache_entry.get("framerates", [])) / len(cache_entry["framerates"]) if cache_entry.get("framerates") else 0.0
            statuses = cache_entry.get("statuses", [])
            uptime = sum(1 for s in statuses if s == "online") / len(statuses) if statuses else 0.0
            
            height_score = RESOLUTION_WEIGHT * min(avg_height / float(RESOLTUION_NORM), 1.0)
            bitrate_score = BITRATE_WEIGHT * min(avg_bitrate / float(BITRATE_NORM), 1.0)
            framerate_score = FRAMERATE_WEIGHT * min(avg_framerate / float(FRAMERATE_NORM), 1.0)
            uptime_score = UPTIME_WEIGHT * uptime
            
            quality_scores[service_id] = {
                "width": avg_width, "height": avg_height, "bitrate": avg_bitrate,
                "framerate": avg_framerate, "uptime": uptime, "resolution_score": height_score,
                "bitrate_score": bitrate_score, "framerate_score": framerate_score,
                "uptime_score": uptime_score,
                "total_score": height_score + bitrate_score + framerate_score + uptime_score,
            }

        # Refactor Note: Using an async context manager for the asyncio.Lock.
        async with self._mutex:
            self._quality_scores = quality_scores
        self.config.log_message("Quality Monitor: Quality scores built successfully.", level="DEBUG")