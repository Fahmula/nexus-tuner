import asyncio
from datetime import datetime, timedelta
from typing import Awaitable, Callable, NoReturn, Self
from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.quality_monitor import QualityMonitor
from nexus_stream.utils import JobName, Label, relative_time


SCHEDULER_TICK_INTERVAL_SECONDS = 60
BACKUP_TIME = "10:00"
CLEANUP_TIME = "00:00"
DISCOVER_TIME = "01:00"
QUALITY_TIME = "02:00"


class Job:
    """Represents a scheduled job with a name, schedules, and a function to execute."""
    
    def __init__(self, name: JobName, schedule: str, func: Callable[[], Awaitable[None]]) -> None:
        if len(schedule) != 5 or schedule[2] != ":" or not (0 <= int(schedule[:2]) <= 23) or not (0 <= int(schedule[3:]) <= 59):
            raise ValueError(f"Invalid schedule format: {schedule} (expected HH:MM)")
        self.name = name
        self.hour, self.minute = map(int, schedule.split(":"))
        self.func = func
        self.active = False

    async def run(self, config: Config, start_time: datetime, set_last_run: Callable[[JobName, datetime], Awaitable[None]]) -> None:
        """Runs the job function and updates its last run time."""
        if self.active:
            return
        self.active = True
        try:
            config.info(Label.SCHEDULER, f"starting job: {self.name}")
            await self.func()
            await set_last_run(self.name, start_time)
            self.log_next_run(config, start_time)
        except Exception as e:
            config.error(Label.SCHEDULER, f"Job failed with exception: {e}")
        finally:
            self.active = False

    def get_next_run(self, last_run: datetime | None) -> datetime:
        """Calculates the next run time based on the last run time and the job's schedule."""
        if not last_run:
            return datetime.fromtimestamp(0)  # Run immediately to get it on the books
        next_run = last_run.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0)
        if next_run <= datetime.now():
            next_run += timedelta(days=1)
        return next_run

    def log_next_run(self, config: Config, last_run: datetime | None) -> None:
        """Logs the next run time of a job."""
        next_run = self.get_next_run(last_run)
        last_run_str = relative_time(last_run) if last_run else "never"
        next_run_str = relative_time(next_run) if next_run > datetime.now() else "now"
        config.info(Label.SCHEDULER, f"{self.name}: last run {last_run_str}, next run {next_run_str}")

class Scheduler:
    """A background task for running tasks at specific times of day."""

    def __init__(self, config: Config, handler: ChannelHandler, quality_monitor: QualityMonitor) -> None:
        self.config = config
        self.handler = handler
        self.quality_monitor = quality_monitor
        self._mutex = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []

        async def scheduled_backup() -> None:
            await self.config.backup_config(scheduled=True)

        async def scheduled_cleanup() -> None:
            await self.config.cleanup_ffmpeg_logs_by_age()
            await self.config.cleanup_backups()

        async def scheduled_discover() -> None:
            await self.handler.reload_handler_config(update_providers=True, force_discover_sources=True)

        self.jobs: list[Job] = [
            Job(JobName.BACKUP, BACKUP_TIME, scheduled_backup),
            Job(JobName.CLEANUP, CLEANUP_TIME, scheduled_cleanup),
            Job(JobName.DISCOVER, DISCOVER_TIME, scheduled_discover),
            Job(JobName.QUALITY, QUALITY_TIME, self.quality_monitor.analyze_mapped_services)
        ]
        self._scheduler_task: asyncio.Task[NoReturn]

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler, quality_monitor: QualityMonitor) -> Self:
        """Asynchronous factory for creating and initializing a Scheduler instance."""
        instance = cls(config, handler, quality_monitor)
        instance._scheduler_task = asyncio.create_task(instance._run())
        config.info(Label.STARTUP, "Task Scheduler started.")
        return instance

    def shutdown(self) -> None:
        """Gracefully stops the scheduler and all its active jobs."""
        self.config.info(Label.SCHEDULER, "Stopping Task Scheduler...")
        self._scheduler_task.cancel()
        for task in self._tasks:
            task.cancel()

    async def set_last_run(self, job_name: JobName, last_run: datetime) -> None:
        """Sets the last run time of a job by its name."""
        async with self._mutex:
            jobs_data = await self.config.get_jobs_config()
            if job_name not in jobs_data:
                jobs_data[job_name] = {}
            jobs_data[job_name]["last_run"] = last_run.isoformat()
            await self.config.save_jobs_config(jobs_data)

    async def _run(self) -> NoReturn:
        """The main execution loop for the scheduler, run as an asyncio task."""
        jobs_data = await self.config.get_jobs_config()
        for job in self.jobs:
            last_run_iso = jobs_data.get(job.name, {}).get("last_run")
            job.log_next_run(self.config, datetime.fromisoformat(last_run_iso) if last_run_iso else None)
        while True:
            try:
                now = datetime.now()
                jobs_data = await self.config.get_jobs_config()
                for job in self.jobs:
                    last_run_iso = jobs_data.get(job.name, {}).get("last_run")
                    if now >= job.get_next_run(datetime.fromisoformat(last_run_iso) if last_run_iso else None):
                        self._tasks.append(asyncio.create_task(job.run(self.config, now, self.set_last_run)))
            except Exception as e:
                self.config.error(Label.SCHEDULER, f"Scheduler encountered an error: {e}")
            to_delete = [task for task in self._tasks if task.done()]
            for task in to_delete:
                e = task.exception()
                if e:
                    self.config.error(Label.SCHEDULER, f"Task failed with: {e}")
                self._tasks.remove(task)
            await asyncio.sleep(SCHEDULER_TICK_INTERVAL_SECONDS)
