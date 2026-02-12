"""
crawler/__init__.py - Crawler Orchestrator

Coordinates the multi-threaded web crawler by:
- Initializing the frontier (URL queue manager)
- Spawning worker threads
- Managing worker lifecycle

Key role: High-level coordinator that ties together frontier and workers
"""

from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker


class Crawler(object):
    """
    Multi-threaded web crawler coordinator.

    Creates a shared frontier and spawns multiple worker threads
    to crawl URLs concurrently while respecting politeness policies.
    """

    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        """
        Initialize the crawler.

        Args:
            config: Configuration object (threads, seeds, politeness, etc.)
            restart: If True, start fresh; if False, resume from save file
            frontier_factory: Factory for creating frontier (for testing)
            worker_factory: Factory for creating workers (for testing)
        """
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = []
        self.worker_factory = worker_factory

    def start_async(self):
        """Spawn worker threads without blocking."""
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)
        ]
        for worker in self.workers:
            worker.start()

    def start(self):
        """Start crawler and block until all workers complete."""
        self.start_async()
        self.join()

    def join(self):
        """Wait for all worker threads to complete."""
        for worker in self.workers:
            worker.join()
