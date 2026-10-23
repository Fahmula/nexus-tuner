"""Audio offset measurement: alignment, delivery clocks, capture and retry recovery.

Positions are seconds into reference PCM. Pairwise delays need independent
confirmation before they connect groups or update existing channel mappings.
The caller owns the background job, progress display and shared process cleanup.
"""

import asyncio
from collections import deque
from collections.abc import Callable, Coroutine, Hashable, Mapping, Set
from dataclasses import dataclass, field
from datetime import datetime
import math
from typing import TYPE_CHECKING, Any, Final, cast

import numpy as np
from scipy.signal import fftconvolve, stft

from nexus_tuner.utils import (
    NEXUS_TUNER_USER_AGENT, DiscoveredSource, Label, Log, LogicalChannelId,
    LogicalChannelInfo, Offset, ProviderAlias, QualityProcessInfos,
    QualityProcessInfosImpl, SourceId, StreamEngine, VideoName,
    create_stream_name, create_video_name, run_bg,
)

if TYPE_CHECKING:
    from nexus_tuner.config import Config
    from nexus_tuner.handler import ChannelHandler
    from nexus_tuner.slots import ProviderSlots
    from nexus_tuner.stream import StreamManager

SAMPLE_RATE = 16_000
HOP = 160
FRAME = 1024
MIN_CLIP_SECONDS = 9
BACKGROUND_SLOT_WAIT_INTERVAL = 1

# A continuous reference brackets both rounds so sources up to two minutes
# ahead OR behind can overlap. Every measurement is manually requested.
OFFSET_CLIP_DURATION: Final[int] = 24
OFFSET_SEARCH_RANGE: Final[int] = 120
OFFSET_ROUND_INTERVAL: Final[int] = 15
OFFSET_SESSION_TIMEOUT: Final[int] = 600
OFFSET_JOB_TIMEOUT: Final[int] = 30 * 60
OFFSET_RETRY_INTERVAL: Final[float] = 5
OFFSET_MAX_RETRY_INTERVAL: Final[float] = 30
OFFSET_AUDIO_IDLE_TIMEOUT: Final[float] = 30
OFFSET_ROUND_TOLERANCE: Final[float] = 1.0


@dataclass(frozen=True)
class AudioMatch:
    position: float
    similarity: float
    agreeing_windows: int


def _features(audio: np.ndarray) -> np.ndarray:
    frequencies, _, spectrum = stft(
        audio, fs=SAMPLE_RATE, nperseg=FRAME, noverlap=FRAME - HOP,
        boundary=None, padded=False,  # type: ignore[arg-type] -- SciPy supports None; its stub does not.
    )
    power = np.abs(spectrum) ** 2
    edges = np.geomspace(100, 6500, 17)
    return np.asarray([
        np.log(np.maximum(np.mean(power[(frequencies >= lo) & (frequencies < hi)], axis=0), 1e-10))
        for lo, hi in zip(edges[:-1], edges[1:])
    ], dtype=np.float32)


def _correlation(template: np.ndarray, reference: np.ndarray) -> np.ndarray:
    """Pearson correlation at every valid lag, independently in each band."""
    length = template.shape[-1]
    template = template - template.mean(axis=-1, keepdims=True)
    energy = np.sum(template.astype(np.float64) ** 2, axis=-1, keepdims=True)
    sums = np.pad(np.cumsum(reference, axis=-1, dtype=np.float64), ((0, 0), (1, 0)))
    squares = np.pad(np.cumsum(reference.astype(np.float64) ** 2, axis=-1), ((0, 0), (1, 0)))
    local_sum = sums[:, length:] - sums[:, :-length]
    variance = squares[:, length:] - squares[:, :-length] - local_sum ** 2 / length
    numerator = fftconvolve(reference, template[:, ::-1], mode="valid", axes=-1)
    return np.clip(numerator / np.sqrt(np.maximum(energy * variance, 1e-15)), -1, 1)


class AudioReference:
    """Prepare a reference once and locate multiple independently encoded clips."""

    def __init__(self, audio: np.ndarray):
        self.audio = np.nan_to_num(np.asarray(audio, dtype=np.float32))
        self.features = _features(self.audio) if len(self.audio) >= FRAME else np.empty((16, 0))

    def match(self, clip: np.ndarray) -> AudioMatch | None:
        clip = np.nan_to_num(np.asarray(clip, dtype=np.float32))
        if len(clip) < MIN_CLIP_SECONDS * SAMPLE_RATE or len(clip) > len(self.audio):
            return None
        if np.sqrt(np.mean(clip ** 2)) < 0.001:
            return None
        features = _features(clip)
        # Stationary tones/silence carry no reliable timing information.
        active = np.std(features, axis=-1) > 0.15
        if np.count_nonzero(active) < 6:
            return None
        reference = self.features[active]
        features = features[active]
        scores = _correlation(features, reference).mean(axis=0)
        peak = int(np.argmax(scores))
        similarity = float(scores[peak])
        competitors = scores.copy()
        competitors[max(0, peak - 50):peak + 51] = -1
        if similarity < 0.60 or similarity - float(np.max(competitors)) < 0.08:
            return None

        # Independent sections must identify the same lag. One lucky transient
        # or a shared jingle is insufficient, even if its correlation is high.
        votes = 0
        sections = np.array_split(np.arange(features.shape[-1]), 3)
        for section in sections:
            local = _correlation(features[:, section], reference).mean(axis=0)
            expected = peak + int(section[0])
            best = int(np.argmax(local))
            if abs(best - expected) <= 8 and float(local[best]) >= 0.55:
                votes += 1
        if votes < 3:
            return None

        position = peak * HOP / SAMPLE_RATE
        # Waveform refinement is optional: independent encoders can destroy
        # phase correlation while preserving the spectral timing information.
        start = max(0, peak * HOP - HOP * 6)
        end = min(len(self.audio), peak * HOP + len(clip) + HOP * 6)
        fine = np.abs(_correlation(clip[None, :], self.audio[None, start:end])[0])
        fine_peak = int(np.argmax(fine))
        if float(fine[fine_peak]) >= 0.35:
            position = (start + fine_peak) / SAMPLE_RATE
        return AudioMatch(position, similarity, votes)


@dataclass
class AudioCapture:
    data: bytearray = field(default_factory=bytearray)
    arrivals: list[tuple[float, float]] = field(default_factory=list)
    ready: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def duration(self) -> float:
        return len(self.data) / (SAMPLE_RATE * 2)

    async def read(self, stream: asyncio.StreamReader, seconds: float, *, idle_timeout: float | None = None) -> None:
        """Drain continuously so PCM output cannot stall FFmpeg."""
        limit = round(seconds * SAMPLE_RATE) * 2
        loop = asyncio.get_running_loop()
        while len(self.data) < limit:
            chunk = await asyncio.wait_for(stream.read(min(8192, limit - len(self.data))), timeout=idle_timeout)
            if not chunk:
                break
            self.data.extend(chunk)
            self.arrivals.append((self.duration, loop.time()))
            self.ready.set()

    async def wait_for_duration(self, seconds: float) -> None:
        while self.duration < seconds:
            self.ready.clear()
            await self.ready.wait()

    def samples(self) -> np.ndarray:
        return np.frombuffer(self.data, dtype='<i2').astype(np.float32) / 32768

    def clock_origin(self) -> float:
        """Estimate sample zero's delivery time after initial probing has drained.

        Live sources supply their own pacing. The lower envelope of receipt time
        minus decoded duration ignores decoder stalls and buffered startup bursts.
        Two later sections must agree, otherwise no stable delay is measurable.
        """
        if self.duration < 12:
            raise ValueError("Too little audio to establish a delivery clock")
        points = np.asarray(self.arrivals)
        origins = []
        for low, high in ((0.4, 0.7), (0.7, 1.01)):
            subset = points[(points[:, 0] >= self.duration * low) & (points[:, 0] < self.duration * high)]
            if len(subset) < 2:
                raise ValueError("Insufficient arrival timestamps")
            origins.append(float(np.quantile(subset[:, 1] - subset[:, 0], 0.1)))
        if abs(origins[0] - origins[1]) > 1.0:
            raise ValueError("Audio arrival clock is unstable (buffering, discontinuity, or non-live input)")
        return min(origins)


def capture_command(ffmpeg: str, url: str, user_agent: str) -> list[str]:
    # Preserve sample order, not arbitrary transport PTS. In particular, copyts
    # plus aresample:first_pts=0 can synthesize hours of leading silence for TS.
    # Do not use -re: it hides a live source's real delivery pacing/buffering.
    return [
        ffmpeg, '-nostdin', '-hide_banner', '-loglevel', 'error',
        '-user_agent', user_agent, '-rw_timeout', '15000000',
        '-probesize', '1M', '-analyzeduration', '1M',
        '-i', url, '-map', '0:a:0', '-vn', '-sn', '-dn',
        '-ac', '1', '-ar', str(SAMPLE_RATE), '-c:a', 'pcm_s16le',
        '-f', 's16le', '-flush_packets', '1', 'pipe:1',
    ]


@dataclass(frozen=True)
class OffsetPlan:
    reference: str
    peers: tuple[str, ...]


class OffsetProgress:
    """A graph of verified relative delays, with pending evidence kept on retries.

    An edge a -> b stores delay(b) - delay(a). Disconnected groups stay separate
    until real audio evidence bridges them; each group's local zero is arbitrary.
    """

    def __init__(self, sources: Set[str], tolerance: float = 1.0):
        self.sources = frozenset(sources)
        self.tolerance = tolerance
        self._edges: dict[str, dict[str, float]] = {sid: {} for sid in sorted(sources)}
        self._evidence: dict[tuple[str, str], dict[Hashable, float]] = {}
        self._reference_attempts: dict[str, int] = {sid: 0 for sid in sources}
        self.revision = 0

    def offsets_from(self, source: str) -> dict[str, float]:
        offsets = {source: 0.0}
        pending = deque([source])
        while pending:
            current = pending.popleft()
            for peer, delta in self._edges[current].items():
                if peer not in offsets:
                    offsets[peer] = offsets[current] + delta
                    pending.append(peer)
        return offsets

    def components(self) -> list[dict[str, float]]:
        remaining = set(self.sources)
        groups = []
        while remaining:
            group = self.offsets_from(min(remaining))
            remaining.difference_update(group)
            if len(group) > 1:
                groups.append(group)
        return groups

    @property
    def complete(self) -> bool:
        return len(self.sources) > 1 and len(self.offsets_from(min(self.sources))) == len(self.sources)

    @property
    def observation_count(self) -> int:
        return sum(len(values) for values in self._evidence.values())

    def observe(self, reference: str, peer: str, delay: float, observation: Hashable) -> bool:
        """Confirm an edge from two distinct, consistent audio observations.

        Reversed reference roles use the same evidence bucket with opposite sign.
        Duplicate observations, outliers and contradictory cycles cannot merge groups.
        """
        if reference == peer or reference not in self.sources or peer not in self.sources or not math.isfinite(delay):
            return False
        a, b = sorted((reference, peer))
        delta = delay if reference == a else -delay
        evidence = self._evidence.setdefault((a, b), {})
        if observation in evidence:
            return False
        evidence[observation] = delta
        # Bounded history; a lone outlier cannot evict all subsequent good evidence.
        if len(evidence) > 16:
            del evidence[next(iter(evidence))]
        clusters = [[value for value in evidence.values() if low <= value <= low + self.tolerance]
                    for low in evidence.values() if low <= delta <= low + self.tolerance]
        cluster = max(clusters, key=lambda values: (len(values), -(max(values) - min(values))))
        if len(cluster) < 2:
            return False
        estimate = sum(cluster) / len(cluster)
        known = self.offsets_from(a)
        if b in known:
            return False  # Keep the established path; never silently distort a cycle.
        self._edges[a][b] = estimate
        self._edges[b][a] = -estimate
        self.revision += 1
        return True

    def plan(self, providers: Mapping[str, str], shared_slots: Mapping[str, bool]) -> OffsetPlan | None:
        """Rotate references and compare across groups that still need a bridge."""
        candidates = []
        for reference in sorted(providers):
            connected = self.offsets_from(reference)
            peers = tuple(peer for peer in sorted(providers) if peer not in connected and
                          (providers[peer] != providers[reference] or shared_slots[providers[reference]]))
            if peers:
                candidates.append((self._reference_attempts[reference], -len(peers), reference, peers))
        if not candidates:
            return None
        _, _, reference, peers = min(candidates)
        self._reference_attempts[reference] += 1
        return OffsetPlan(reference, peers)


class OffsetMeasurement:
    """Measure one channel using shared provider slots and process ownership.

    Partial audio and verified edges survive source failures within this job.
    Process cleanup belongs to the caller so quality probes and offset captures
    follow the same slot-release and shutdown rules.
    """

    def __init__(self, config: "Config", handler: "ChannelHandler", stream_manager: "StreamManager", *,
                 processes: QualityProcessInfos, process_lock: asyncio.Lock,
                 cleanup_process: Callable[
                     [asyncio.subprocess.Process | None, "ProviderSlots", SourceId, VideoName],
                     Coroutine[Any, Any, None],
                 ],
                 report_progress: Callable[[str], None] | None = None) -> None:
        self.config = config
        self.handler = handler
        self.stream_manager = stream_manager
        self.processes = processes
        self.process_lock = process_lock
        self._cleanup_process = cleanup_process
        self._report_progress = report_progress

    async def measure(self, logical_channel_id: LogicalChannelId, *,
                      measured_sources: set[SourceId] | None = None) -> bool | None:
        """Connect intermittent sources through verified pairwise delay measurements."""
        if (problem := self.config.get_engine_problem(StreamEngine.FFMPEG)):
            Log.error(Label.QUALITY, f"Skipping offset measurement: {problem}")
            return None
        channel = await self.handler.get_logical_channel_by_id(logical_channel_id)
        if not channel:
            return None
        mappings = await self.handler.get_channel_mappings_for_ui(logical_channel_id)
        expected = {m["source_id"] for m in mappings}
        if len(expected) < 2:
            return None
        stream_name = create_stream_name(channel["logical_channel_title"], channel["channel_num"])
        progress = OffsetProgress(expected, OFFSET_ROUND_TOLERANCE)
        saved_sources: set[SourceId] = set()
        saved_revision = -1
        loop = asyncio.get_running_loop()
        deadline = loop.time() + OFFSET_JOB_TIMEOUT
        backoff = OFFSET_RETRY_INTERVAL
        attempt = 0

        async def persist_progress() -> bool:
            nonlocal saved_revision
            current = await self.handler.get_channel_mappings_for_ui(logical_channel_id)
            if {m["source_id"] for m in current} != expected:
                Log.warn(Label.QUALITY, f"{stream_name}: Mappings changed; stopping offset analysis.")
                return False
            # Independent groups have independent zeros. Save only groups with
            # a common coordinate with saved offsets; retain the rest for bridging.
            if progress.revision != saved_revision:
                groups = progress.components()
                anchors = {m["source_id"] for m in current if m["offset"] is not None}
                groups = [g for g in groups if anchors.intersection(g)] if anchors else sorted(groups, key=len, reverse=True)[:1]
                saved_all = True
                for group in groups:
                    result = await self.handler.update_mapping_offsets(
                        logical_channel_id, expected, {SourceId(sid): Offset(value) for sid, value in group.items()},
                    )
                    if result is None:
                        saved_all = False
                    else:
                        saved_sources.update(SourceId(sid) for sid in group)
                if groups and saved_all:
                    saved_revision = progress.revision
                if measured_sources is not None:
                    measured_sources.update(saved_sources)
            return True

        while loop.time() < deadline:
            if not await persist_progress():
                return False
            if progress.complete and saved_revision == progress.revision and expected <= saved_sources:
                return True

            sources = {sid: ds for sid in sorted(expected)
                       if (ds := await self.handler.get_discovered_source(sid))}
            slots: dict[SourceId, ProviderSlots] = {}
            shared_slots: dict[str, bool] = {}
            for sid, ds in sources.items():
                provider = ds["provider_alias"]
                provider_slots = await self.handler.get_provider_slots(provider)
                available = bool(provider_slots and provider_slots.get_total_slots() > 0 and
                                 await self.stream_manager.is_provider_available_soon(provider))
                if available and provider_slots is not None:
                    slots[sid] = provider_slots
                    if provider not in shared_slots:
                        shared_slots[provider] = bool(provider_slots.get_total_slots() >= 2 and
                            await self.stream_manager.is_provider_available_soon(provider, 1))
            plan = progress.plan({sid: sources[sid]["provider_alias"] for sid in slots}, shared_slots)
            previous = (progress.revision, progress.observation_count)
            if plan is not None:
                attempt += 1
                ref_id = SourceId(plan.reference)
                peers = [SourceId(sid) for sid in plan.peers]
                Log.info(Label.QUALITY, f"{stream_name}: Audio attempt {attempt}, reference {ref_id}, comparing {len(peers)} sources.")
                await self._measure_session(logical_channel_id, channel, sources, slots,
                                            ref_id, peers, progress, attempt, deadline)
            if progress.revision != previous[0]:
                backoff = OFFSET_RETRY_INTERVAL
                continue  # Persist new measurements before any sleep or retry.
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            if self._report_progress is not None:
                self._report_progress(
                    f"Audio analysis: {len(saved_sources)}/{len(expected)} sources saved; "
                    "retrying unavailable sources and connecting matching groups (you can leave this page)."
                )
            Log.debug(Label.QUALITY, f"{stream_name}: Retrying audio offsets in {min(backoff, remaining):.1f}s; {len(saved_sources)}/{len(expected)} saved.")
            await asyncio.sleep(min(backoff, remaining))
            if progress.observation_count == previous[1]:
                backoff = min(OFFSET_MAX_RETRY_INTERVAL, backoff * 2)
            else:
                backoff = OFFSET_RETRY_INTERVAL

        # An attempt may confirm a bridge just as its deadline expires. Persist it
        # before returning a partial result, even when the reference has failed.
        if (await persist_progress() and progress.complete and saved_revision == progress.revision and
                expected <= saved_sources):
            return True
        Log.warn(Label.QUALITY, f"{stream_name}: Audio retry window ended with {len(saved_sources)}/{len(expected)} sources saved. Partial offsets have been kept.")
        return False

    async def _capture_audio(self, source_id: SourceId, ds: DiscoveredSource, provider_slots: ProviderSlots,
                             logical_channel_id: LogicalChannelId, channel: LogicalChannelInfo,
                             recording: AudioCapture, seconds: float, reference_provider: ProviderAlias,
                             stop: asyncio.Event | None = None) -> None:
        """External decoder boundary. Keep usable PCM even on EOF or preemption."""
        if self.handler.get_pending_stream_count() > 0:
            return
        stream_name = create_stream_name(channel["logical_channel_title"], channel["channel_num"])
        title = ds["display_title"] or ds["tvg_name"]
        video_name = create_video_name(stream_name, title, source_id)
        while not await provider_slots.try_acquire():
            if self.handler.get_pending_stream_count() > 0:
                return
            await self.stream_manager.prune_processes(ds["provider_alias"])
            if not await self.stream_manager.is_provider_available_soon(
                ds["provider_alias"], 1 if ds["provider_alias"] == reference_provider else 0
            ):
                return
            await asyncio.sleep(BACKGROUND_SLOT_WAIT_INTERVAL)
        process = None
        children: list[asyncio.Task[Any]] = []
        task = None
        try:
            process = await asyncio.create_subprocess_exec(
                *capture_command(str(self.config.ffmpeg_path), ds["stream_url"], NEXUS_TUNER_USER_AGENT),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            async with self.process_lock:
                cast(QualityProcessInfosImpl, self.processes)[source_id] = {
                    "provider_alias": ds["provider_alias"], "tvg_logo": ds["tvg_logo"] or channel["tvg_logo"],
                    "source_title": title, "logical_channel_id": logical_channel_id,
                    "channel_num": channel["channel_num"], "type": "Offset", "started_at": datetime.now(),
                }
            stdout = process.stdout
            assert stdout is not None

            async def collect() -> None:
                reader = asyncio.create_task(recording.read(stdout, seconds, idle_timeout=OFFSET_AUDIO_IDLE_TIMEOUT))
                children.append(reader)
                if stop is None:
                    await reader
                else:
                    stopper = asyncio.create_task(stop.wait())
                    children.append(stopper)
                    done, _ = await asyncio.wait((reader, stopper), return_when=asyncio.FIRST_COMPLETED)
                    for completed in done:
                        completed.result()

            task = asyncio.create_task(asyncio.wait_for(collect(), seconds + 30))
            provider_slots.add_background_task(task)
            await task
            if recording.duration < 12:
                Log.warn(Label.QUALITY, f"{video_name}: Audio ended before enough samples were captured.")
        except asyncio.CancelledError:
            if task and provider_slots.pop_cancelled_task(task):
                Log.info(Label.QUALITY, f"{video_name}: Offset capture yielded its slot to a viewer.")
                return
            raise
        except Exception as exc:
            Log.warn(Label.QUALITY, f"{video_name}: Audio capture failed ({type(exc).__name__}); retaining usable samples.")
        finally:
            for child in children:
                child.cancel()
            await asyncio.gather(*children, return_exceptions=True)
            await run_bg(self._cleanup_process(process, provider_slots, source_id, video_name))

    async def _measure_session(self, logical_channel_id: LogicalChannelId, channel: LogicalChannelInfo,
                               sources: dict[SourceId, DiscoveredSource], slots: dict[SourceId, ProviderSlots],
                               ref_id: SourceId, peers: list[SourceId], progress: OffsetProgress,
                               attempt: int, deadline: float) -> None:
        """Harvest partial captures even when a decoder or the reference exits."""
        reference = AudioCapture()
        stop = asyncio.Event()
        ref_provider = sources[ref_id]["provider_alias"]
        duration = min(OFFSET_SESSION_TIMEOUT, max(0, deadline - asyncio.get_running_loop().time()))
        ref_task = asyncio.create_task(self._capture_audio(
            ref_id, sources[ref_id], slots[ref_id], logical_channel_id, channel, reference, duration, ref_provider, stop,
        ))
        peer_tasks: list[asyncio.Task[Any]] = []
        clips: dict[SourceId, list[AudioCapture]] = {sid: [] for sid in peers}

        async def wait_reference(seconds: float) -> None:
            waiter = asyncio.create_task(reference.wait_for_duration(seconds))
            try:
                done, _ = await asyncio.wait((ref_task, waiter), return_when=asyncio.FIRST_COMPLETED)
                if ref_task in done:
                    await ref_task
                    raise RuntimeError("Reference capture ended; retaining partial matches and rotating reference")
                await waiter
            finally:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)

        async def rounds(source_id: SourceId) -> None:
            for round_number in range(2):
                recording = AudioCapture()
                clips[source_id].append(recording)
                await self._capture_audio(source_id, sources[source_id], slots[source_id], logical_channel_id,
                                          channel, recording, OFFSET_CLIP_DURATION, ref_provider)
                if round_number == 0:
                    await asyncio.sleep(OFFSET_ROUND_INTERVAL)

        async def sample_peers() -> None:
            peer_tasks.extend(asyncio.create_task(rounds(sid)) for sid in peers)
            group = asyncio.gather(*peer_tasks)
            try:
                done, _ = await asyncio.wait((ref_task, group), return_when=asyncio.FIRST_COMPLETED)
                if ref_task in done:
                    await ref_task
                    raise RuntimeError("Reference capture ended during peer sampling")
                await group
            finally:
                if not group.done():
                    group.cancel()
                await asyncio.gather(group, return_exceptions=True)

        stream_name = create_stream_name(channel["logical_channel_title"], channel["channel_num"])
        try:
            async with asyncio.timeout(duration):
                await wait_reference(OFFSET_SEARCH_RANGE)
                await sample_peers()
                await wait_reference(reference.duration + OFFSET_SEARCH_RANGE)
                stop.set()
                await ref_task
        except (TimeoutError, ValueError, RuntimeError) as exc:
            Log.warn(Label.QUALITY, f"{stream_name}: {exc}")
        finally:
            ref_task.cancel()
            for task in peer_tasks:
                task.cancel()
            await asyncio.gather(ref_task, *peer_tasks, return_exceptions=True)

        try:
            reference_origin = reference.clock_origin()
        except ValueError:
            return
        matcher = await asyncio.to_thread(AudioReference, reference.samples())
        for sid, recordings in clips.items():
            for round_number, recording in enumerate(recordings):
                try:
                    origin = recording.clock_origin()
                except ValueError:
                    continue
                match = await asyncio.to_thread(matcher.match, recording.samples())
                if match is not None:
                    delay = origin - reference_origin - match.position
                    if abs(delay) <= OFFSET_SEARCH_RANGE:
                        linked = progress.observe(ref_id, sid, delay, (attempt, sid, round_number))
                        if linked:
                            Log.info(Label.QUALITY, f"{stream_name}: Confirmed {ref_id} ↔ {sid}: {delay:.3f}s; retained across retries.")
