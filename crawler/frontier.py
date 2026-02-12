"""
frontier.py - URL Queue Manager with Politeness Policy

Manages the frontier of URLs to be crawled, implementing:
- Per-domain URL queuing for organized crawling
- Politeness delay enforcement (500ms between requests to same domain)
- Persistent state using shelve for crash recovery
- Thread-safe operations for concurrent workers

Key role: Ensures crawler respects politeness and doesn't overwhelm servers
"""

import os
import shelve
import time
from collections import defaultdict, deque
from urllib.parse import urlparse
from threading import RLock

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class Frontier(object):
    """
    Thread-safe URL frontier with per-domain politeness enforcement.

    Organizes URLs by domain and ensures minimum time delay between
    requests to the same domain to be respectful of server load.
    """

    def __init__(self, config, restart):
        """
        Initialize the frontier.

        Args:
            config: Configuration object with save_file, seed_urls, time_delay
            restart: If True, start fresh; if False, resume from save file
        """
        self.logger = get_logger("FRONTIER")
        self.config = config

        # Thread synchronization
        self.lock = RLock()  # Protects all frontier data structures

        # URL organization by domain for politeness
        self.domain_queues = defaultdict(deque)  # domain -> deque of URLs
        self.last_accessed = {}  # domain -> timestamp of last access
        self.active_downloads = 0  # In-flight downloads (for shutdown logic)

        # Handle save file based on restart flag
        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        # Open persistent storage (creates new file if doesn't exist)
        self.save = shelve.open(self.config.save_file)

        if restart:
            # Fresh start: add seed URLs
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Resume: load incomplete URLs from save file
            self._parse_save_file()
            if not self.save:
                # Empty save file, add seed URLs
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        """
        Load incomplete URLs from persistent storage into domain queues.

        Reads the save file and re-queues all URLs that were discovered
        but not yet completed, allowing the crawler to resume.
        """
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                # Add incomplete URL to appropriate domain queue
                parsed = urlparse(url)
                domain = parsed.netloc
                self.domain_queues[domain].append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        """
        Get next URL to download, enforcing politeness delay (thread-safe).

        Searches domain queues for a domain that hasn't been accessed
        within the politeness delay period (500ms by default).

        Returns:
            URL string if available, None if all domains are in cooldown

        Politeness Policy:
            - Minimum 500ms between requests to same domain
            - Rotates through domains to respect server load
            - Tracks in-flight downloads for shutdown coordination
        """
        with self.lock:
            current_time = time.time()
            politeness_delay = self.config.time_delay  # Default: 0.5 seconds

            # Search for a domain ready to be crawled
            for domain in list(self.domain_queues.keys()):
                queue = self.domain_queues[domain]

                # Clean up empty queues
                if not queue:
                    del self.domain_queues[domain]
                    continue

                # Check if politeness delay has elapsed
                last_time = self.last_accessed.get(domain, 0)
                time_since_last = current_time - last_time

                if time_since_last >= politeness_delay:
                    # Domain is ready - return next URL from its queue
                    url = queue.popleft()
                    self.last_accessed[domain] = current_time
                    self.active_downloads += 1
                    return url

            # All domains are in cooldown period
            return None

    def add_url(self, url):
        """
        Add a new URL to the frontier (thread-safe).

        Normalizes the URL, checks for duplicates, persists to disk,
        and adds to the appropriate domain queue.

        Args:
            url: URL string to add
        """
        url = normalize(url)
        urlhash = get_urlhash(url)

        with self.lock:
            if urlhash not in self.save:
                # New URL - persist and enqueue
                self.save[urlhash] = (url, False)
                self.save.sync()

                # Add to domain-specific queue
                parsed = urlparse(url)
                domain = parsed.netloc
                self.domain_queues[domain].append(url)

    def mark_url_complete(self, url):
        """
        Mark a URL as completed (thread-safe).

        Updates persistent storage and decrements active download counter.

        Args:
            url: URL string that was completed
        """
        urlhash = get_urlhash(url)

        with self.lock:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()
            self.active_downloads -= 1

    def has_pending_urls(self):
        """
        Check if crawling should continue (thread-safe).

        Returns:
            True if there are queued URLs or active downloads in progress

        Note:
            Active downloads may discover new URLs, so we must wait
            for them to complete even if queues are temporarily empty.
        """
        with self.lock:
            # Check if any domain has queued URLs
            if any(len(queue) > 0 for queue in self.domain_queues.values()):
                return True
            # Check if downloads are in progress (may add new URLs)
            return self.active_downloads > 0
