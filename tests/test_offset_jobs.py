"""Job/route/persistence tests with no app lifespan or production configuration."""
import asyncio
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

os.environ.setdefault('NEXUS_CONFIG_DIR', '/tmp/nexus-offset-tests')

from nexus_tuner.quality_monitor import QualityMonitor
from nexus_tuner.utils import Log
from helpers import MemoryConfig, MemoryHandler


class ControlledMonitor(QualityMonitor):
    """Replace slow provider analysis at its I/O boundary with an explicit gate."""
    def __init__(self, config, handler):
        super().__init__(config, handler)
        self.release = asyncio.Event()
        self.result = 2
        self.error = False

    async def _analyze_mapped_sources(self, input_lc_id=None):
        await self.release.wait()
        if self.error: raise RuntimeError('provider unavailable')
        return self.result


async def settle(monitor, channel_id):
    for _ in range(100):
        if monitor.get_analysis_status(channel_id)['state'] != 'running': return
        await asyncio.sleep(0)
    raise AssertionError('job did not finish')


class OffsetJobTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.logs = tempfile.TemporaryDirectory(prefix='nexus-offset-test-logs-')
        Log.initialize_logger(Path(cls.logs.name), 1)

    @classmethod
    def tearDownClass(cls):
        cls.logs.cleanup()

    async def asyncSetUp(self):
        self.config = MemoryConfig()
        self.handler = MemoryHandler(self.config)
        self.channel = await self.handler.add_logical_channel(dict(
            logical_channel_title='Test', channel_num='1', group_title='', tvg_id='', tvg_logo=''))
        await self.handler.update_mappings_for_logical_channel(self.channel, [
            dict(source_id='a', priority=1, offset=None), dict(source_id='b', priority=2, offset=None)])
        self.monitor = ControlledMonitor(self.config, self.handler)

    async def asyncTearDown(self):
        await self.monitor.shutdown()

    async def test_duplicate_manual_job_and_scheduler_cannot_take_reserved_job(self):
        self.assertTrue(self.monitor.start_manual_analysis(self.channel))
        self.assertFalse(self.monitor.start_manual_analysis(self.channel))
        self.assertIsInstance(await self.monitor.analyze_mapped_sources(), str)
        self.monitor.release.set()
        await settle(self.monitor, self.channel)
        self.assertEqual(self.monitor.get_analysis_status(self.channel)['state'], 'complete')

    async def test_partial_failure_and_shutdown_are_visible(self):
        for result, error, expected in ((1, False, 'partial'), (0, False, 'failed'), (2, True, 'failed')):
            monitor = ControlledMonitor(self.config, self.handler)
            monitor.result, monitor.error = result, error
            monitor.start_manual_analysis(self.channel)
            monitor.release.set()
            await settle(monitor, self.channel)
            self.assertEqual(monitor.get_analysis_status(self.channel)['state'], expected)
            await monitor.shutdown()
        self.monitor.start_manual_analysis(self.channel)
        await asyncio.sleep(0)
        await self.monitor.shutdown()
        self.assertEqual(self.monitor.get_analysis_status(self.channel)['state'], 'failed')

    async def test_request_returns_while_work_continues_and_status_survives_navigation(self):
        import app as web
        with patch.object(web, 'handler', self.handler, create=True), patch.object(web, 'quality_monitor', self.monitor, create=True), patch.object(web, 'config', self.config, create=True):
            client = web.app.test_client()
            response = await client.post(f'/ui/logical-channels/analyze-mappings/{self.channel}')
            self.assertEqual(response.status_code, 202)
            self.assertIn('leave this page', await response.get_data(as_text=True))
            self.assertEqual(self.monitor.get_analysis_status(self.channel)['state'], 'running')
            duplicate = await client.post(f'/ui/logical-channels/analyze-mappings/{self.channel}')
            self.assertEqual(duplicate.status_code, 409)
            self.monitor.release.set()
            await settle(self.monitor, self.channel)
            # A new client represents returning after navigating away.
            returned = await web.app.test_client().get(f'/ui/logical-channels/analysis-status/{self.channel}')
            self.assertIn('analysis completed', await returned.get_data(as_text=True))
            self.assertNotIn('every 3s', await returned.get_data(as_text=True))

    async def test_offset_sign_normalization_and_current_priorities(self):
        await self.handler.update_mappings_for_logical_channel(self.channel, [
            dict(source_id='a', priority=9, offset=None), dict(source_id='b', priority=2, offset=None)])
        self.assertTrue(await self.handler.update_mapping_offsets(self.channel, {'a', 'b'}, {'a': 0, 'b': -7.5}))
        result = await self.handler.get_mappings_for_logical_channel(self.channel)
        self.assertEqual(result['a'], dict(priority=9, offset=7.5))
        self.assertEqual(result['b']['offset'], 0)

    async def test_save_and_analyze_is_one_server_request(self):
        import app as web
        with patch.object(web, 'handler', self.handler, create=True), patch.object(web, 'quality_monitor', self.monitor, create=True), patch.object(web, 'config', self.config, create=True):
            response = await web.app.test_client().post(f'/ui/logical-channels/form/{self.channel}', data=urlencode({
                'logical_channel_id': self.channel, 'logical_channel_title': 'Updated', 'channel_num': '1',
                'mapping_source_id': ['a', 'b'], 'priority_a': '7', 'priority_b': '2', 'analyze_mappings': '1',
            }, doseq=True), headers={'Content-Type': 'application/x-www-form-urlencoded'})
            self.assertEqual(response.status_code, 202)
            self.assertEqual(self.monitor.get_analysis_status(self.channel)['state'], 'running')
            result = await self.handler.get_mappings_for_logical_channel(self.channel)
            self.assertEqual(result['a']['priority'], 7)

    async def test_zero_or_one_measurement_cannot_establish_relative_offsets(self):
        for offsets in ({}, {'a': 0}, {'a': 0, 'b': float('nan')}):
            self.assertFalse(await self.handler.update_mapping_offsets(self.channel, {'a','b'}, offsets))
        result = await self.handler.get_mappings_for_logical_channel(self.channel)
        self.assertIsNone(result['a']['offset'])

    async def test_partial_results_preserve_unavailable_sources_and_anchor(self):
        await self.handler.update_mappings_for_logical_channel(self.channel, [
            dict(source_id='a', priority=1, offset=10), dict(source_id='b', priority=2, offset=0),
            dict(source_id='c', priority=3, offset=None)])
        self.assertTrue(await self.handler.update_mapping_offsets(self.channel, {'a','b','c'}, {'a': 0, 'c': 5}))
        result = await self.handler.get_mappings_for_logical_channel(self.channel)
        self.assertEqual([result[s]['offset'] for s in ('a','b','c')], [10, 0, 15])

    async def test_removed_mapping_and_deleted_channel_are_not_resurrected(self):
        await self.handler.update_mappings_for_logical_channel(self.channel, [dict(source_id='a', priority=1, offset=None)])
        self.assertFalse(await self.handler.update_mapping_offsets(self.channel, {'a','b'}, {'a':0, 'b':10}))
        self.assertEqual(set(await self.handler.get_mappings_for_logical_channel(self.channel)), {'a'})
        await self.handler.delete_logical_channel(self.channel)
        self.assertFalse(await self.handler.update_mapping_offsets(self.channel, {'a','b'}, {'a':0, 'b':10}))
        self.assertIsNone(await self.handler.get_mappings_for_logical_channel(self.channel))

    async def test_failed_storage_save_preserves_existing_values(self):
        self.config.fail_save = True
        self.assertFalse(await self.handler.update_mapping_offsets(self.channel, {'a','b'}, {'a':0, 'b':10}))
        result = await self.handler.get_mappings_for_logical_channel(self.channel)
        self.assertIsNone(result['a']['offset'])
        self.assertIsNone(result['b']['offset'])

    async def test_incomplete_save_is_distinct_from_discarded_results(self):
        await self.handler.update_mappings_for_logical_channel(self.channel, [
            dict(source_id=s, priority=1, offset=None) for s in ('a','b','c')])
        self.assertIs(await self.handler.update_mapping_offsets(self.channel, {'a','b','c'}, {'a':0, 'b':10}), False)
        result = await self.handler.get_mappings_for_logical_channel(self.channel)
        self.assertEqual(result['b']['offset'], 10)
        self.assertIsNone(result['c']['offset'])
        self.assertIsNone(await self.handler.update_mapping_offsets(self.channel, {'a','b'}, {'a':0, 'b':10}))


if __name__ == '__main__': unittest.main()
