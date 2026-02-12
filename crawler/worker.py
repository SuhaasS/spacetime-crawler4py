"""
worker.py - Crawler Worker Threads

Worker threads that fetch URLs from the frontier, download pages,
extract links using the scraper module, and add discovered URLs
back to the frontier.

Key role: Executes the crawl loop with politeness enforcement
"""

from threading import Thread
from inspect import getsource
import time

from utils.download import download
from utils import get_logger
import scraper


class Worker(Thread):
    """
    Worker thread that downloads and scrapes web pages.

    Runs continuously until the frontier is empty, requesting URLs
    from the frontier, downloading them, and feeding discovered links
    back to the frontier.
    """

    def __init__(self, worker_id, config, frontier):
        """
        Initialize a worker thread.

        Args:
            worker_id: Unique identifier for logging
            config: Configuration object
            frontier: Shared frontier instance for URL management

        Raises:
            AssertionError: If scraper.py uses forbidden libraries
        """
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        # Enforce library restrictions (academic requirement)
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, \
            "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, \
            "Do not use urllib.request in scraper.py"

        super().__init__(daemon=True)
        
    def run(self):
        """
        Main crawl loop - continuously fetch, download, and scrape URLs.

        Process:
            1. Request URL from frontier (respects politeness delay)
            2. Download the page
            3. Extract links using scraper module
            4. Add discovered links to frontier
            5. Mark URL as complete
            6. Repeat until frontier is empty

        The frontier enforces politeness by only returning URLs from
        domains that haven't been accessed within the delay period.
        """
        while True:
            tbd_url = self.frontier.get_tbd_url()

            if not tbd_url:
                # No URL available - check reason
                if self.frontier.has_pending_urls():
                    # URLs exist but all domains are in politeness cooldown
                    time.sleep(0.1)
                    continue
                else:
                    # Frontier exhausted - shutdown
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break

            # Download the page
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")

            # Extract and validate links
            scraped_urls = scraper.scraper(tbd_url, resp)

            # Add discovered URLs to frontier
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)

            # Mark completion (decrements active download counter)
            self.frontier.mark_url_complete(tbd_url)
