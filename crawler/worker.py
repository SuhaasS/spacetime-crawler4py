from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        """
        Main worker loop with per-domain politeness enforcement.
        The frontier handles the 500ms delay between requests to the same domain.
        """
        while True:
            tbd_url = self.frontier.get_tbd_url()

            if not tbd_url:
                # No URL available right now - could be due to politeness delay
                # Check if there are any pending URLs at all
                if self.frontier.has_pending_urls():
                    # URLs exist but are waiting for politeness delay
                    # Sleep briefly and retry
                    time.sleep(0.1)
                    continue
                else:
                    # No URLs left at all - stop crawling
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break

            # Download and process the URL
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)

            # Note: We don't sleep here anymore because the frontier
            # handles politeness delay by tracking last access per domain
