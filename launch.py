"""
launch.py - Web Crawler Entry Point

Main entry point for the web crawler application.
Handles configuration loading, cache server registration,
and crawler initialization.

Usage:
    python launch.py                    # Resume from last state
    python launch.py --restart          # Start fresh crawl
    python launch.py --config_file path # Use custom config file
"""

import multiprocessing
multiprocessing.set_start_method("fork")

from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


def main(config_file, restart):
    """
    Initialize and start the web crawler.

    Args:
        config_file: Path to configuration file (default: config.ini)
        restart: If True, start fresh; if False, resume from save file
    """
    # Load configuration
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)

    # Register with cache server
    config.cache_server = get_cache_server(config, restart)

    # Create and start crawler
    crawler = Crawler(config, restart)
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False,
                        help="Start fresh crawl (vs. resume from save file)")
    parser.add_argument("--config_file", type=str, default="config.ini",
                        help="Path to configuration file")
    args = parser.parse_args()
    main(args.config_file, args.restart)
