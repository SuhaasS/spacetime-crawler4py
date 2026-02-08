import os
import shelve
import time
from collections import defaultdict, deque
from urllib.parse import urlparse
from threading import RLock

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config

        # Thread-safe data structures
        self.lock = RLock()  # Protects all frontier data structures

        # Organize URLs by domain for per-domain politeness
        self.domain_queues = defaultdict(deque)  # domain -> deque of URLs
        self.last_accessed = {}  # domain -> timestamp of last download

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                # Add to domain queue
                parsed = urlparse(url)
                domain = parsed.netloc
                self.domain_queues[domain].append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        """
        Get a URL to be downloaded, respecting per-domain politeness (500ms delay).
        Thread-safe implementation.
        """
        with self.lock:
            current_time = time.time()
            politeness_delay = self.config.time_delay  # 0.5 seconds

            # Find a domain that is ready (hasn't been accessed in the last 500ms)
            for domain in list(self.domain_queues.keys()):
                queue = self.domain_queues[domain]

                # Skip empty queues
                if not queue:
                    del self.domain_queues[domain]
                    continue

                # Check if enough time has passed since last access
                last_time = self.last_accessed.get(domain, 0)
                time_since_last = current_time - last_time

                if time_since_last >= politeness_delay:
                    # This domain is ready, get a URL from it
                    url = queue.popleft()
                    self.last_accessed[domain] = current_time
                    return url

            # No domain is ready yet
            return None

    def add_url(self, url):
        """Add a URL to the frontier (thread-safe)."""
        url = normalize(url)
        urlhash = get_urlhash(url)

        with self.lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()

                # Add to appropriate domain queue
                parsed = urlparse(url)
                domain = parsed.netloc
                self.domain_queues[domain].append(url)
    
    def mark_url_complete(self, url):
        """Mark a URL as completed (thread-safe)."""
        urlhash = get_urlhash(url)

        with self.lock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()

    def has_pending_urls(self):
        """Check if there are any URLs waiting to be downloaded (thread-safe)."""
        with self.lock:
            return any(len(queue) > 0 for queue in self.domain_queues.values())
