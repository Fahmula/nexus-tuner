"""In-memory storage boundaries shared by job and measurement tests."""
from copy import deepcopy

from nexus_tuner.handler import ChannelHandler


class MemoryConfig:
    """Storage boundary: saves data in memory, never reads user configuration."""
    def __init__(self):
        self.mappings = {}
        self.channels = {}
        self.quality_cache = {}
        self.fail_save = False

    def get_path_problems(self):
        return []

    def get_engine_problem(self, engine):
        return None

    async def get_quality_cache(self):
        return self.quality_cache

    async def save_quality_cache(self, data):
        self.quality_cache = deepcopy(data)
        return True

    async def save_channel_mappings_config(self, data):
        if self.fail_save: return False
        self.mappings = deepcopy(data)
        return True

    async def save_logical_channels_config(self, data):
        self.channels = deepcopy(data)
        return True


class MemoryHandler(ChannelHandler):
    async def reload_handler_config(self, **kwargs):
        # The real handler already holds the values saved to MemoryConfig.
        # Reloading the user's files is an external I/O boundary in this test.
        pass
