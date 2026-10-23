"""Offset matching, capture, retries and persistence without an app lifespan.

Recovery tests replace only the decoder boundary. Session tests use a real,
paced PCM subprocess and the same process cleanup as quality probes.
"""
import asyncio
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import wave

import numpy as np
from scipy.signal import butter, sosfiltfilt

os.environ.setdefault('NEXUS_CONFIG_DIR', '/tmp/nexus-offset-tests')

from nexus_tuner.offset_measurement import AudioCapture, AudioReference, OffsetMeasurement, OffsetProgress, SAMPLE_RATE
from nexus_tuner.quality_monitor import QualityMonitor
from nexus_tuner.slots import ProviderSlots
from nexus_tuner.utils import Log
from helpers import MemoryConfig, MemoryHandler


def process_resources(config, handler):
    """Use production process ownership and cleanup for decoder integration."""
    owner = QualityMonitor(config, handler)
    return dict(processes=owner.processes, process_lock=owner.quality_process_lock,
                cleanup_process=owner._cleanup_process)


def programme(seconds=40, seed=7):
    """Non-repeating band-limited programme with changing spectral balance."""
    rng = np.random.default_rng(seed)
    count = seconds * SAMPLE_RATE
    audio = np.zeros(count, dtype=np.float32)
    for low, high in ((130, 350), (350, 800), (800, 1600), (1600, 3000), (3000, 6000)):
        carrier = sosfiltfilt(butter(2, (low, high), btype='bandpass', fs=SAMPLE_RATE, output='sos'), rng.normal(0, 1, count))
        envelope = np.interp(np.arange(count), np.arange(0, count + SAMPLE_RATE, SAMPLE_RATE // 4),
                             rng.uniform(0.02, 1, len(np.arange(0, count + SAMPLE_RATE, SAMPLE_RATE // 4))))
        audio += (carrier * envelope * 0.08).astype('f4')
    return audio


class AlignmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.audio = programme()
        cls.reference = AudioReference(cls.audio)

    def test_known_positions_and_independent_channel_processing(self):
        for position in (0, 5.347, 28):
            start = round(position * SAMPLE_RATE)
            clip = self.audio[start:start + 12*SAMPLE_RATE].copy()
            for processed in (clip, -clip, clip * 0.1 + 0.08,
                              np.tanh(clip * 9) * 0.2):
                with self.subTest(position=position):
                    match = self.reference.match(processed)
                    self.assertIsNotNone(match)
                    self.assertAlmostEqual(match.position, position, delta=0.015)

    def test_unrelated_or_uninformative_audio_is_unknown(self):
        for clip in (np.array([]), np.zeros(12*SAMPLE_RATE), np.full(12*SAMPLE_RATE, np.nan),
                     programme(12, 99), self.audio[:4*SAMPLE_RATE], self.audio.repeat(2)):
            with self.subTest(length=len(clip)):
                self.assertIsNone(self.reference.match(clip))
        self.assertIsNone(AudioReference(np.array([])).match(self.audio[:12*SAMPLE_RATE]))

    def test_repeated_material_cannot_determine_offset(self):
        clip = self.audio[3*SAMPLE_RATE:15*SAMPLE_RATE]
        repeated = self.audio.copy()
        repeated[23*SAMPLE_RATE:35*SAMPLE_RATE] = clip
        self.assertIsNone(AudioReference(repeated).match(clip))

    def test_changed_middle_section_is_not_a_whole_clip_match(self):
        clip = self.audio[5*SAMPLE_RATE:17*SAMPLE_RATE].copy()
        clip[4*SAMPLE_RATE:8*SAMPLE_RATE] = self.audio[28*SAMPLE_RATE:32*SAMPLE_RATE]
        self.assertIsNone(self.reference.match(clip))

    def test_tone_is_not_timing_information(self):
        tone = (0.1*np.sin(np.arange(40*SAMPLE_RATE)*2*np.pi*440/SAMPLE_RATE)).astype('f4')
        self.assertIsNone(AudioReference(tone).match(tone[:12*SAMPLE_RATE]))


class CaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_stalled_decoder_times_out_and_preserves_received_audio(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b'\0\0' * SAMPLE_RATE)
        recording = AudioCapture()
        with self.assertRaises(TimeoutError):
            await recording.read(reader, 24, idle_timeout=0.01)
        self.assertEqual(recording.duration, 1)

    async def test_reads_pcm_to_a_bounded_duration(self):
        reader = asyncio.StreamReader()
        recording = AudioCapture()
        task = asyncio.create_task(recording.read(reader, 1))
        reader.feed_data(b'\x01\x00' * (SAMPLE_RATE * 2))
        reader.feed_eof()
        await task
        self.assertEqual(recording.duration, 1)
        self.assertEqual(len(recording.samples()), SAMPLE_RATE)
        self.assertAlmostEqual(float(recording.samples()[0]), 1/32768)
        await recording.wait_for_duration(1)

    async def test_truncated_input_stops_reading(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b'\0\0' * 10)
        reader.feed_eof()
        recording = AudioCapture()
        await recording.read(reader, 12)
        with self.assertRaises(ValueError): recording.clock_origin()

    def test_clock_ignores_startup_buffer_and_delivery_jitter(self):
        recording = AudioCapture(data=bytearray(24 * SAMPLE_RATE * 2))
        recording.arrivals = [(i/4, 100 + i/4 + (3 if i < 20 else (i % 4)*0.08)) for i in range(1, 97)]
        self.assertAlmostEqual(recording.clock_origin(), 100, delta=0.08)

    def test_unpaced_file_or_stalling_source_has_no_stable_clock(self):
        for arrivals in (
            [(i/4, 100+i/400) for i in range(1, 97)],
            [(i/4, 100+i/4+(3 if i>60 else 0)) for i in range(1, 97)],
        ):
            recording = AudioCapture(data=bytearray(24*SAMPLE_RATE*2), arrivals=arrivals)
            with self.assertRaises(ValueError): recording.clock_origin()


class OffsetProgressTests(unittest.TestCase):
    def confirm(self, progress, a, b, delay):
        self.assertFalse(progress.observe(a, b, delay, (a,b,0)))
        return progress.observe(a, b, delay, (a,b,1))

    def test_empty_single_source_and_one_observation_are_not_complete(self):
        for sources in (set(), {'a'}, {'a','b'}):
            self.assertFalse(OffsetProgress(sources).complete)
        progress = OffsetProgress({'a','b'})
        self.assertFalse(progress.observe('a','b',3,'one'))
        self.assertFalse(progress.observe('a','b',3,'one'))
        self.assertEqual(progress.components(), [])
        self.assertFalse(progress.complete)

    def test_reversed_reference_combines_pending_evidence_across_attempts(self):
        progress = OffsetProgress({'a','b'})
        self.assertFalse(progress.observe('a','b',3, 'attempt1'))
        self.assertTrue(progress.observe('b','a',-3.1, 'attempt2'))
        self.assertAlmostEqual(progress.offsets_from('a')['b'], 3.05)
        self.assertTrue(progress.complete)

    def test_disconnected_groups_are_preserved_until_bridge_exists(self):
        progress = OffsetProgress({'a','b','c','d'})
        self.confirm(progress, 'a','b', 3)
        self.confirm(progress, 'c','d', -7)
        self.assertEqual(len(progress.components()), 2)
        self.assertFalse(progress.complete)
        self.assertTrue(self.confirm(progress, 'b','d', 5))
        self.assertEqual(progress.offsets_from('a'), {'a':0, 'b':3, 'd':8, 'c':15})
        self.assertTrue(progress.complete)

    def test_outliers_and_conflicting_cycles_do_not_change_verified_delays(self):
        progress = OffsetProgress({'a','b','c'})
        progress.observe('a','b',100,'bad')
        self.assertFalse(progress.observe('a','b',3,'good1'))
        self.assertTrue(progress.observe('a','b',3.1,'good2'))
        self.confirm(progress,'b','c',2)
        before = progress.offsets_from('a')
        self.assertFalse(self.confirm(progress,'a','c',20))
        self.assertEqual(progress.offsets_from('a'), before)
        for value in (float('nan'), float('inf')):
            self.assertFalse(progress.observe('a','c',value, value))

    def test_references_rotate_and_completed_pairs_are_not_recaptured(self):
        progress = OffsetProgress({'a','b','c'})
        providers = {s:s for s in 'abc'}
        first = progress.plan(providers, {s:False for s in 'abc'})
        second = progress.plan(providers, {s:False for s in 'abc'})
        self.assertNotEqual(first.reference, second.reference)
        self.confirm(progress, 'a','b',3)
        plan = progress.plan({'a':'one','b':'two'}, {'one':False,'two':False})
        self.assertIsNone(plan)
        self.assertIsNone(progress.plan({'a':'one','c':'one'}, {'one':False}))
        self.assertIsNotNone(progress.plan({'a':'one','c':'one'}, {'one':True}))


class SessionConfig:
    def __init__(self, executable): self.ffmpeg_path = executable
    def get_engine_problem(self, engine): return None


class SessionHandler:
    def __init__(self, audio, epoch):
        self.saved = None
        self.extra_cached_mapping = False
        self.slots = {name: ProviderSlots(name, '', 1) for name in ('a', 'b')}
        self.sources = {name: dict(provider_alias=name, display_title=name, tvg_name=name, tvg_logo='',
                                  stream_url=f'{audio}|{epoch}|{delay}') for name, delay in (('a',0), ('b',1.25))}

    async def get_logical_channel_by_id(self, channel_id):
        return dict(channel_num='1', logical_channel_title='Test', tvg_logo='')
    async def get_channel_mappings_for_ui(self, channel_id):
        mappings = [dict(source_id=s, priority=1, offset=0 if self.extra_cached_mapping and s == 'a' else None)
                    for s in self.sources]
        if self.extra_cached_mapping:
            mappings.append(dict(source_id='unavailable', priority=1, offset=20))
        return mappings
    async def get_discovered_source(self, source_id): return self.sources.get(source_id)
    async def get_provider_slots(self, alias): return self.slots[alias]
    def get_pending_stream_count(self): return 0
    async def update_mapping_offsets(self, channel_id, expected, offsets):
        self.saved = offsets
        return True


class AvailableStreams:
    async def is_provider_available_soon(self, alias, offset=0): return True
    async def prune_processes(self, alias): pass


class SessionTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.scratch = tempfile.TemporaryDirectory(prefix='nexus-session-test-')
        root = Path(cls.scratch.name)
        Log.initialize_logger(root, 1)
        cls.audio = root / 'audio.wav'
        with wave.open(str(cls.audio), 'wb') as stream:
            stream.setparams((1, 2, 16000, 0, 'NONE', 'not compressed'))
            stream.writeframes((programme(90)*32767).astype('<i2').tobytes())
        cls.executable = root / 'pcm-source'
        cls.executable.write_text(f'#!{sys.executable}\n' + '''import sys,time,wave
path,epoch,delay=sys.argv[sys.argv.index('-i')+1].split('|')
with wave.open(path,'rb') as audio:
    start=time.monotonic()
    audio.setpos(round((start-float(epoch)-float(delay))*16000))
    sent=0
    while True:
        data=audio.readframes(1024)
        if not data: break
        sys.stdout.buffer.write(data);sys.stdout.buffer.flush()
        sent+=len(data)//2
        time.sleep(max(0,start+sent/16000-time.monotonic()))
''')
        cls.executable.chmod(0o700)

    @classmethod
    def tearDownClass(cls): cls.scratch.cleanup()

    def measurement(self):
        handler = SessionHandler(self.audio, time.monotonic()-40)
        config = SessionConfig(self.executable)
        measurement = OffsetMeasurement(config, handler, AvailableStreams(), **process_resources(config, handler))
        return measurement, handler

    async def test_two_captures_measure_signed_delay_and_release_every_slot(self):
        measurement, handler = self.measurement()
        handler.extra_cached_mapping = True
        coverage = set()
        with patch.multiple('nexus_tuner.offset_measurement', OFFSET_SEARCH_RANGE=3, OFFSET_CLIP_DURATION=12,
                            OFFSET_ROUND_INTERVAL=0, OFFSET_SESSION_TIMEOUT=45, OFFSET_JOB_TIMEOUT=32):
            result = await measurement.measure('channel', measured_sources=coverage)
        # A cached offset on an unavailable mapping cannot claim a fresh success.
        self.assertFalse(result)
        self.assertEqual(coverage, {'a', 'b'})
        self.assertAlmostEqual(handler.saved['b'] - handler.saved['a'], 1.25, delta=0.1)
        await asyncio.sleep(0)
        self.assertEqual([await s.get_active_slots() for s in handler.slots.values()], [0,0])
        self.assertEqual(dict(measurement.processes), {})

    async def test_viewer_preemption_cleans_up_without_saving_partial_measurement(self):
        measurement, handler = self.measurement()
        deadline = patch('nexus_tuner.offset_measurement.OFFSET_JOB_TIMEOUT',0.5)
        deadline.start()
        task = asyncio.create_task(measurement.measure('channel'))
        try:
            for _ in range(100):
                if await handler.slots['a'].get_active_slots(): break
                await asyncio.sleep(0.01)
            # Wait until the slot has registered the subprocess's cancellable work.
            await asyncio.sleep(0.1)
            handler.slots['a'].cancel_background_tasks()
            self.assertFalse(await asyncio.wait_for(task, 5))
            await asyncio.sleep(0)
            self.assertEqual([await s.get_active_slots() for s in handler.slots.values()], [0,0])
            self.assertIsNone(handler.saved)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            deadline.stop()


class RecoveryHandler(MemoryHandler):
    def __init__(self, config, names):
        super().__init__(config)
        self.sources = {name: dict(provider_alias=name, display_title=name, tvg_name=name, tvg_logo='',stream_url='unused') for name in names}
        self.provider_slots = {name: ProviderSlots(name,'',1) for name in names}
        self.saves = []

    async def get_discovered_source(self, source_id): return self.sources.get(source_id)
    async def get_provider_slots(self, alias): return self.provider_slots[alias]

    async def update_mapping_offsets(self, channel, expected, offsets):
        result = await super().update_mapping_offsets(channel,expected,offsets)
        if result is not None:
            self.saves.append(await self.get_mappings_for_logical_channel(channel))
        return result


class OfflineProbeMonitor(QualityMonitor):
    """Replace the ffprobe I/O boundary, retaining real job orchestration."""
    async def _run_single_probe(self, provider, url, logo, source_id, title, channel, number, video_name):
        return source_id, dict(status='offline', reason='source unavailable'), video_name


class MeasurementIntegrationTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.scratch = tempfile.TemporaryDirectory(prefix='nexus-measurement-integration-')
        Log.initialize_logger(Path(cls.scratch.name), 1)

    @classmethod
    def tearDownClass(cls): cls.scratch.cleanup()

    async def asyncSetUp(self):
        self.config = MemoryConfig()
        self.handler = RecoveryHandler(self.config, 'ab')
        self.channel = await self.handler.add_logical_channel(dict(
            logical_channel_title='Integration', channel_num='1', group_title='', tvg_id='', tvg_logo=''))
        await self.handler.update_mappings_for_logical_channel(self.channel, [
            dict(source_id=name, priority=1, offset=None) for name in 'ab'])
        self.monitor = OfflineProbeMonitor(self.config, self.handler)

    async def asyncTearDown(self):
        await self.monitor.shutdown()

    async def test_manual_job_forwards_measurement_progress_and_partial_result(self):
        class UnavailableStreams(AvailableStreams):
            async def is_provider_available_soon(self, alias, offset=0): return False

        self.monitor.stream_manager = UnavailableStreams()
        with patch.multiple('nexus_tuner.offset_measurement', OFFSET_JOB_TIMEOUT=0.1,
                            OFFSET_RETRY_INTERVAL=0.01, OFFSET_MAX_RETRY_INTERVAL=0.02):
            self.assertTrue(self.monitor.start_manual_analysis(self.channel))
            async with asyncio.timeout(2):
                while True:
                    status = self.monitor.get_analysis_status(self.channel)
                    if status['message'].startswith('Audio analysis:'): break
                    await asyncio.sleep(0)
                self.assertEqual(status['state'], 'running')
                self.assertIn('0/2 sources saved', status['message'])
                while self.monitor.get_analysis_status(self.channel)['state'] == 'running':
                    await asyncio.sleep(0.01)
        self.assertEqual(self.monitor.get_analysis_status(self.channel)['state'], 'partial')
        self.assertEqual(set(self.config.quality_cache), {'a', 'b'})

    async def test_scheduled_probes_do_not_start_offset_measurement(self):
        class NoOffsetStreams(AvailableStreams):
            async def is_provider_available_soon(self, alias, offset=0):
                raise AssertionError('Scheduled analysis must not capture audio')

        self.monitor.stream_manager = NoOffsetStreams()
        self.assertEqual(await self.monitor.analyze_mapped_sources(), 1)
        self.assertEqual(set(self.config.quality_cache), {'a', 'b'})
        mappings = await self.handler.get_mappings_for_logical_channel(self.channel)
        self.assertTrue(all(mapping['offset'] is None for mapping in mappings.values()))


class ScriptedDecoderMeasurement(OffsetMeasurement):
    """Only replace the external decoder: retain real clocks, matching and retries."""
    def __init__(self, handler, pcm, profile, reference_ends=None):
        config = SessionConfig('unused')
        super().__init__(config, handler, AvailableStreams(), **process_resources(config, handler))
        self.pcm = pcm
        self.profile = profile
        self.reference_ends = reference_ends or {}
        self.references = []
        self.rounds = {}
        self.active_reference = None
        self.reference_audio = None
        self.delays = dict(a=0, b=3, c=8, d=-2)

    async def _capture_audio(self, source_id, ds, slots, channel_id, channel, recording, seconds, reference_provider, stop=None):
        if stop is not None:
            self.references.append(source_id)
            self.active_reference = source_id
            self.reference_audio = recording
            limit = self.reference_ends.get(source_id, 90)
            for index in range(limit):
                if stop.is_set(): return
                recording.data.extend(self.pcm[index*SAMPLE_RATE*2:(index+1)*SAMPLE_RATE*2])
                recording.arrivals.append((recording.duration, self.origin+recording.duration))
                recording.ready.set()
                await asyncio.sleep(0)
            if limit < 90: return
            await stop.wait()
        else:
            key = (len(self.references), source_id)
            number = self.rounds.get(key, 0)
            self.rounds[key] = number + 1
            length = self.profile(self.active_reference, source_id, number, len(self.references))
            if length == 0: return
            position = 12 + number*12
            recording.data.extend(self.pcm[position*SAMPLE_RATE*2:(position+length)*SAMPLE_RATE*2])
            origin = self.origin + position + self.delays[source_id] - self.delays[self.active_reference]
            recording.arrivals = [(i/4, origin+i/4) for i in range(1, length*4+1)]
            recording.ready.set()
            await self.reference_audio.wait_for_duration(position + length)

    @property
    def origin(self): return 1000 + len(self.references)*100


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.scratch = tempfile.TemporaryDirectory(prefix='nexus-recovery-')
        Log.initialize_logger(Path(cls.scratch.name), 1)
        cls.pcm = (programme(90)*32767).astype('<i2').tobytes()

    @classmethod
    def tearDownClass(cls): cls.scratch.cleanup()

    async def setup_measurement(self, names, profile, reference_ends=None):
        config = MemoryConfig()
        handler = RecoveryHandler(config, names)
        channel = await handler.add_logical_channel(dict(logical_channel_title='Recovery', channel_num='1', group_title='', tvg_id='', tvg_logo=''))
        await handler.update_mappings_for_logical_channel(channel, [dict(source_id=name,priority=1,offset=None) for name in names])
        return ScriptedDecoderMeasurement(handler,self.pcm,profile,reference_ends), handler, config, channel

    def timing(self):
        return patch.multiple('nexus_tuner.offset_measurement', OFFSET_SEARCH_RANGE=15, OFFSET_CLIP_DURATION=24,
                              OFFSET_ROUND_INTERVAL=0, OFFSET_SESSION_TIMEOUT=2, OFFSET_JOB_TIMEOUT=10,
                              OFFSET_RETRY_INTERVAL=0.001, OFFSET_MAX_RETRY_INTERVAL=0.01)

    async def test_failed_first_reference_rotates_and_later_bridge_finishes(self):
        measurement, handler, _, channel = await self.setup_measurement('abc',lambda ref,peer,round,attempt: 12 if peer!='a' or attempt>=3 else 0, {'a':0})
        with self.timing():
            self.assertTrue(await measurement.measure(channel))
        self.assertGreater(len(set(measurement.references)), 1)
        self.assertEqual(handler.saves[0]['a']['offset'], None)
        values = await handler.get_mappings_for_logical_channel(channel)
        self.assertAlmostEqual(values['b']['offset']-values['a']['offset'],3,delta=0.01)
        self.assertAlmostEqual(values['c']['offset']-values['a']['offset'],8,delta=0.01)

    async def test_disconnected_groups_survive_until_an_overlapping_source_returns(self):
        links = {('a','b'), ('c','d'), ('d','b')}
        measurement, handler, _, channel = await self.setup_measurement('abcd',lambda ref,peer,*_:12 if (ref,peer) in links else 0)
        with self.timing():
            self.assertTrue(await measurement.measure(channel))
        self.assertIsNone(handler.saves[0]['c']['offset'])
        self.assertIsNone(handler.saves[0]['d']['offset'])
        final = await handler.get_mappings_for_logical_channel(channel)
        for sid, delta in measurement.delays.items():
            self.assertAlmostEqual(final[sid]['offset']-final['a']['offset'],delta,delta=0.01)

    async def test_single_partial_capture_is_confirmed_after_reference_swap(self):
        measurement, handler, _, channel = await self.setup_measurement('ab',lambda ref,peer,round,*_:12 if round==0 else 0)
        with self.timing():
            self.assertTrue(await measurement.measure(channel))
        self.assertEqual(measurement.references, ['a','b'])
        values = await handler.get_mappings_for_logical_channel(channel)
        self.assertAlmostEqual(values['b']['offset']-values['a']['offset'],3,delta=0.01)

    async def test_reference_dying_after_useful_audio_does_not_discard_matches(self):
        measurement, handler, _, channel = await self.setup_measurement('ab',lambda *_:12, {'a':36})
        with self.timing():
            self.assertTrue(await measurement.measure(channel))
        self.assertEqual(measurement.references, ['a'])
        self.assertTrue(handler.saves)

    async def test_retry_deadline_saves_partial_result_and_does_not_claim_success(self):
        measurement, handler, _, channel = await self.setup_measurement('abc',lambda ref,peer,*_:12 if (ref,peer)==('a','b') else 0, {'c':0})
        with self.timing(), patch('nexus_tuner.offset_measurement.OFFSET_JOB_TIMEOUT',0.35):
            self.assertFalse(await measurement.measure(channel))
        values = await handler.get_mappings_for_logical_channel(channel)
        self.assertAlmostEqual(values['b']['offset']-values['a']['offset'],3,delta=0.01)
        self.assertIsNone(values['c']['offset'])

    async def test_failed_save_is_retried_without_requiring_new_audio(self):
        measurement, handler, config, channel = await self.setup_measurement('ab',lambda *_:12)
        save = config.save_channel_mappings_config
        failures = [True]
        async def flaky_save(data):
            if failures:
                failures.pop()
                return False
            return await save(data)
        config.save_channel_mappings_config = flaky_save
        with self.timing():
            self.assertTrue(await measurement.measure(channel))
        self.assertEqual(measurement.references, ['a'])

    async def test_temporarily_full_providers_are_rechecked_instead_of_stopping(self):
        measurement, handler, _, channel = await self.setup_measurement('ab',lambda *_:12)
        class ReturningSlots(AvailableStreams):
            checks = 0
            async def is_provider_available_soon(self, alias, offset=0):
                self.checks += 1
                return self.checks > 4
        streams = ReturningSlots()
        measurement.stream_manager = streams
        with self.timing():
            self.assertTrue(await measurement.measure(channel))
        self.assertGreater(streams.checks,4)
        self.assertTrue(handler.saves)

    async def test_failed_bridge_save_cannot_claim_success_from_cached_groups(self):
        links = {('a','b'), ('c','d'), ('d','b')}
        measurement, handler, _, channel = await self.setup_measurement('abcd',lambda ref,peer,*_:12 if (ref,peer) in links else 0)
        await handler.update_mappings_for_logical_channel(channel, [dict(source_id=s,priority=1,offset=value)
            for s,value in dict(a=0,b=3,c=100,d=90).items()])
        save = handler.update_mapping_offsets
        failures = [True]
        bridge_attempts = []
        async def flaky_bridge(channel_id, expected, offsets):
            if len(offsets)==4:
                bridge_attempts.append(dict(offsets))
                if failures:
                    failures.pop()
                    return None
            return await save(channel_id,expected,offsets)
        handler.update_mapping_offsets = flaky_bridge
        with self.timing():
            self.assertTrue(await measurement.measure(channel))
        self.assertEqual(len(bridge_attempts),2)
        final = await handler.get_mappings_for_logical_channel(channel)
        self.assertAlmostEqual(final['c']['offset']-final['a']['offset'],8,delta=0.01)


if __name__ == '__main__': unittest.main()
