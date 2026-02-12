"""
scraper.py - Web Scraper with URL Validation and Analytics

This module provides the core scraping functionality for the web crawler:
- Extracts links from HTML pages
- Validates URLs to avoid traps and stay within allowed domains
- Collects analytics: unique pages, word frequencies, longest page, subdomains
- Thread-safe statistics collection with persistent logging

Main entry point: scraper(url, resp) returns list of valid URLs to crawl next
"""

import re
import json
import sys
import atexit
import signal
from datetime import datetime
from threading import Lock
from collections import Counter, defaultdict
from urllib.parse import urlparse, urljoin, urldefrag

from bs4 import BeautifulSoup

# ============================================================================
# CONFIGURATION
# ============================================================================

ALLOWED_DOMAINS = [
    ".ics.uci.edu",
    ".cs.uci.edu",
    ".informatics.uci.edu",
    ".stat.uci.edu",
]

# English stopwords from https://www.ranks.nl/stopwords
STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any",
    "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below",
    "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did",
    "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each",
    "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have",
    "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
    "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll",
    "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself",
    "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of",
    "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves",
    "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's",
    "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the",
    "their", "theirs", "them", "themselves", "then", "there", "there's", "these",
    "they", "they'd", "they'll", "they're", "they've", "this", "those", "through",
    "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
    "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's",
    "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's",
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're",
    "you've", "your", "yours", "yourself", "yourselves",
}

# HTML/markup artifacts to exclude from word frequencies
JUNK_WORDS = {
    "html", "update", "automatic", "markdown", "rmd", "git",
    "file", "files", "store", "ds", "href", "https", "http",
    "www", "nbsp", "amp", "quot", "lt", "gt",
}

# ============================================================================
# GLOBAL STATISTICS (Thread-Safe)
# ============================================================================

# Analytics data structures
unique_pages: set[str] = set()
word_counts: Counter[str] = Counter()
longest_page = ("", 0)  # (url, word_count)
subdomain_pages: defaultdict[str, set[str]] = defaultdict(set)

# Thread synchronization locks
stats_lock = Lock()  # Protects all analytics data structures
log_lock = Lock()    # Protects log file writes

# Initialize crawl activity log
CRAWL_LOG_FILE = "crawl_log.txt"
try:
    with open(CRAWL_LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"Crawl started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
except Exception:
    pass

# ============================================================================
# LOGGING AND REPORTING
# ============================================================================

def _log_crawl(url, word_count, subdomain):
    """
    Log a crawl event to the activity log file (thread-safe).

    Args:
        url: The crawled URL
        word_count: Number of words found on the page
        subdomain: The subdomain of the URL
    """
    try:
        with log_lock:
            with open(CRAWL_LOG_FILE, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%H:%M:%S")
                with stats_lock:
                    page_count = len(unique_pages)
                f.write(f"[{timestamp}] Pages: {page_count} | Words: {word_count} | {subdomain}\n")
                f.write(f"  {url}\n\n")
    except Exception:
        pass

def _save_report():
    """
    Generate and save final crawl analytics report (thread-safe).

    Outputs:
        - report.json: Machine-readable JSON format
        - final_report_stats.txt: Human-readable text format
        - Console output: Summary to terminal

    Report includes:
        - Total unique pages crawled
        - Longest page by word count
        - Top 50 most common words (excluding stopwords)
        - Subdomain distribution
    """
    # Atomically read all statistics
    with stats_lock:
        unique_count = len(unique_pages)
        longest_url, longest_count = longest_page
        top_words = word_counts.most_common(50)
        subdomain_counts = {sub: len(urls) for sub, urls in subdomain_pages.items()}

    # Print to console
    print("\n" + "=" * 50)
    print("FINAL CRAWL ANALYTICS")
    print("=" * 50)
    print(f"Unique pages: {unique_count}")
    print(f"Longest page: {longest_url}")
    print(f"Longest page word count: {longest_count}")

    print("\nTop 50 words (stopwords removed):")
    for word, count in top_words:
        print(f"  {word}\t{count}")

    print(f"\nSubdomains ({len(subdomain_counts)} total, alphabetical):")
    for subdomain in sorted(subdomain_counts.keys()):
        print(f"  {subdomain}, {subdomain_counts[subdomain]}")

    # Write structured JSON for programmatic access
    data = {
        "unique_pages_count": unique_count,
        "longest_page": {"url": longest_url, "word_count": longest_count},
        "top_50_words": top_words,
        "subdomains": {sub: count for sub, count in sorted(subdomain_counts.items())},
    }
    with open("report.json", "w") as f:
        json.dump(data, f, indent=2)

    # Write human-readable text report
    with open("final_report_stats.txt", "w", encoding="utf-8") as f:
        f.write(f"Unique pages: {unique_count}\n")
        f.write(f"Longest page: {longest_url}\n")
        f.write(f"Longest page word count: {longest_count}\n\n")
        f.write("Top 50 words (stopwords removed):\n")
        for word, count in top_words:
            f.write(f"{word}\t{count}\n")
        f.write(f"\nSubdomains (alphabetical) with unique page counts:\n")
        for subdomain in sorted(subdomain_counts.keys()):
            f.write(f"{subdomain}, {subdomain_counts[subdomain]}\n")


# Register cleanup handlers to ensure report is saved on exit
atexit.register(_save_report)


def _handle_sigterm(signum, frame):
    """Signal handler to save report when process is terminated."""
    _save_report()
    sys.exit(0)


signal.signal(signal.SIGTERM, _handle_sigterm)

# ============================================================================
# TEXT PROCESSING
# ============================================================================


def _tokenize(text):
    """
    Tokenize text into lowercase alphanumeric words.

    Extracts contiguous sequences of ASCII alphanumeric characters,
    converting them to lowercase.

    Args:
        text: Input text string

    Returns:
        List of lowercase alphanumeric tokens

    Time Complexity: O(n) where n is the length of text
    """
    tokens = []
    current_token = ""
    for char in text:
        if char.isascii() and char.isalnum():
            current_token += char.lower()
        else:
            if current_token:
                tokens.append(current_token)
                current_token = ""
    if current_token:
        tokens.append(current_token)
    return tokens


def _compute_word_frequencies(tokens):
    """
    Count frequency of each token.

    Args:
        tokens: List of string tokens

    Returns:
        Dictionary mapping token to frequency count

    Time Complexity: O(n) where n is the number of tokens
    """
    frequencies = {}
    for token in tokens:
        frequencies[token] = frequencies.get(token, 0) + 1
    return frequencies


def _extract_visible_text(soup):
    """
    Extract main content text from HTML, filtering out boilerplate.

    Removes non-content elements like scripts, navigation, headers, footers,
    and sidebars to focus on the primary textual content of the page.

    Args:
        soup: BeautifulSoup parsed HTML document

    Returns:
        Cleaned text string with normalized whitespace
    """
    # Remove non-content tags that don't contain useful text
    for tag in soup(["script", "style", "noscript", "svg", "iframe",
                     "form", "meta", "link"]):
        tag.decompose()

    # Attempt to locate main content container
    main = None
    for selector in ["main", "article", "#content", "#main",
                     ".content", ".entry-content", ".post-content",
                     ".page-content", ".site-content"]:
        main = soup.select_one(selector)
        if main:
            break

    if not main:
        main = soup.body or soup

    # Remove boilerplate sections within the content container
    for tag in main.find_all(["header", "footer", "nav", "aside"]):
        tag.decompose()

    # Remove elements with boilerplate-related class or id attributes
    boilerplate_pattern = re.compile(
        r"(menu|nav|footer|header|sidebar|breadcrumb|cookie|popup)", re.I
    )
    for node in main.find_all(attrs={"class": boilerplate_pattern}):
        node.decompose()
    for node in main.find_all(attrs={"id": boilerplate_pattern}):
        node.decompose()

    # Extract and normalize text
    text = main.get_text(separator=" ")
    return re.sub(r"\s+", " ", text).strip()

# ============================================================================
# ANALYTICS COLLECTION
# ============================================================================


def _record_stats(url, soup):
    """
    Collect page analytics and update global statistics (thread-safe).

    Tracks:
        - Unique pages visited
        - Word frequency distribution (stopwords/junk filtered)
        - Longest page by word count
        - Pages per subdomain

    Args:
        url: Page URL
        soup: BeautifulSoup parsed HTML

    Filters:
        - Skips pages with < 50 tokens (low content)
        - Removes words < 2 or > 30 characters
        - Excludes stopwords and HTML artifacts
        - Caps per-page word contribution at 10 to prevent skew
    """
    global longest_page

    clean_url, _ = urldefrag(url)

    # Extract and tokenize visible content
    text = _extract_visible_text(soup)
    tokens = _tokenize(text)

    # Skip low-content pages
    if len(tokens) < 50:
        return

    # Extract hostname for subdomain tracking
    parsed = urlparse(clean_url)
    hostname = (parsed.hostname or "").lower()

    # Filter tokens: remove stopwords, junk, and extreme lengths
    filtered = [
        token for token in tokens
        if 2 <= len(token) <= 30
        and token not in STOP_WORDS
        and token not in JUNK_WORDS
    ]
    page_freqs = _compute_word_frequencies(filtered)
    total_word_count = len(tokens)

    # Update global statistics atomically
    with stats_lock:
        unique_pages.add(clean_url)

        # Track subdomain distribution
        if hostname.endswith(".uci.edu") or hostname == "uci.edu":
            subdomain_pages[hostname].add(clean_url)

        # Update longest page
        if total_word_count > longest_page[1]:
            longest_page = (clean_url, total_word_count)

        # Accumulate word frequencies with per-page cap to prevent dominance
        PER_PAGE_CAP = 10
        for word, count in page_freqs.items():
            word_counts[word] += min(count, PER_PAGE_CAP)

    # Log the crawl activity
    _log_crawl(clean_url, total_word_count, hostname)

# ============================================================================
# MAIN SCRAPER INTERFACE
# ============================================================================


def scraper(url, resp):
    """
    Main scraper entry point called by crawler workers.

    Extracts all links from a page and filters them through validation.

    Args:
        url: The URL that was requested
        resp: Response object from the download module

    Returns:
        List of valid, absolute URLs to crawl next
    """
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    """
    Extract all links from an HTTP response.

    Parses HTML content, collects analytics for HTML pages,
    and extracts all href attributes from anchor tags.

    Args:
        url: The requested URL
        resp: Response object with status, raw_response, and content

    Returns:
        List of absolute URLs (with fragments removed)
    """
    # Only process successful responses with content
    if resp.status != 200 or resp.raw_response is None:
        return []

    try:
        content = resp.raw_response.content
    except AttributeError:
        return []

    if not content:
        return []

    # Determine content type
    content_type = ""
    try:
        content_type = (resp.raw_response.headers.get("Content-Type") or "").lower()
    except Exception:
        pass

    # Parse HTML
    soup = BeautifulSoup(content, "lxml")
    base_url = resp.raw_response.url if resp.raw_response.url else url

    # Collect analytics for HTML pages only
    if "text/html" in content_type:
        _record_stats(base_url, soup)

    # Extract all anchor tag hrefs
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        # Skip non-navigational hrefs
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        # Convert to absolute URL and remove fragment
        absolute_url = urljoin(base_url, href)
        absolute_url, _ = urldefrag(absolute_url)
        links.append(absolute_url)

    return links

# ============================================================================
# URL VALIDATION
# ============================================================================


def is_valid(url):
    """
    Validate whether a URL should be crawled.

    Checks:
        1. Protocol must be http or https
        2. Domain must be in ALLOWED_DOMAINS list
        3. Not a known crawler trap (query params, calendars, etc.)
        4. Not a non-HTML file type (media, documents, etc.)
        5. Not excessively long or deeply nested

    Args:
        url: Absolute URL string to validate

    Returns:
        True if URL should be crawled, False otherwise
    """
    try:
        parsed = urlparse(url)

        # Only HTTP(S) protocols
        if parsed.scheme not in {"http", "https"}:
            return False

        hostname = parsed.hostname
        if hostname is None:
            return False

        # Domain restriction: only crawl allowed UCI domains
        if not any(
            hostname == domain.lstrip(".")
            or hostname.endswith(domain)
            for domain in ALLOWED_DOMAINS
        ):
            return False

        path_lower = parsed.path.lower()
        query_lower = (parsed.query or "").lower()

        # ====================================================================
        # QUERY STRING TRAPS
        # ====================================================================

        # WordPress comment/share links: infinite variations of same content
        # ?replytocom=123, ?share=twitter, etc.
        if "replytocom=" in query_lower or "share=" in query_lower:
            return False

        # oEmbed and XML format endpoints: return metadata, not HTML
        if "oembed" in query_lower or "format=xml" in query_lower:
            return False

        # DokuWiki action parameters: combinatorial explosion
        # Examples: do=media, do=revisions, tab_files=1, tab_details=1
        dokuwiki_params = [
            "do=media", "tab_files=", "tab_details=",
            "do=revisions", "do=backlink", "do=recent", "do=index"
        ]
        if any(param in query_lower for param in dokuwiki_params):
            return False

        # WordPress faceted filtering: creates exponential URL combinations
        # Examples: filter[category]=x, filter%5btag%5d=y
        if "filter%5b" in query_lower or "filter[" in query_lower:
            return False

        # ====================================================================
        # PATH-BASED FILTERS
        # ====================================================================

        # RSS/Atom feeds: XML syndication, not HTML
        # WordPress exposes /feed on every post and category
        if path_lower.endswith("/feed") or "/feed/" in path_lower:
            return False

        # XML files: sitemaps, feeds, config files (not HTML)
        if path_lower.endswith(".xml"):
            return False

        # WordPress API and uploads: non-HTML content
        # /wp-json/ = REST API (returns JSON)
        # /wp-content/uploads/ = media files (no links)
        bad_paths = ["/wp-json/", "/wp-content/uploads/"]
        if any(path in path_lower for path in bad_paths):
            return False

        # GitLab: massive trap with thousands of auto-generated pages
        # Each repo has issues, merge requests, forks, branches, commits, etc.
        # Mostly boilerplate UI with minimal unique content
        if "gitlab" in (parsed.hostname or "").lower():
            return False

        # Non-HTML file extensions: no extractable links, waste of crawl budget
        # Categories: web assets, media, documents, archives, binaries, source code
        if re.match(
            r".*\.("
            r"css|js|bmp|gif|jpe?g|ico|png|tiff?|"  # Web assets & images
            r"mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|"  # Media
            r"pdf|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|"  # Documents
            r"zip|rar|gz|bz2|tar|7z|tgz|"  # Archives
            r"exe|msi|bin|dll|dmg|iso|apk|"  # Binaries
            r"c|cc|cpp|h|hpp|java|py|r|m|mat|o|"  # Source code
            r"names|data|dat|psd|epub|cnf|sha1|thmx|mso|arff|rtf|jar|csv|"  # Data
            r"rm|smil|wmv|swf|wma|img|sql|ppsx|odc|war|db|lif|"  # Misc
            # r"ova|vmdk|vdi|qcow2"  # VM images
            r")$",
            path_lower,
        ):
            return False

        # ====================================================================
        # CRAWLER TRAP DETECTION
        # ====================================================================

        # Calendar/event pages: infinite date combinations
        # Example: /calendar/2023-01/, /calendar/2023-02/, etc.
        if re.search(r"(calendar|date|event)", path_lower) and \
           re.search(r"\d{4}[-/]\d{2}", parsed.path):
            return False

        # Recursive path segments: infinite depth loops
        # Example: /seminar-series/seminar-series/...
        if path_lower.count("seminar-series") > 1:
            return False

        # Extremely long URLs: usually dynamically generated traps
        # Legitimate content pages rarely exceed 200 characters
        if len(url) > 200:
            return False

        # Path depth and loop detection
        path_segments = [seg for seg in parsed.path.split("/") if seg]
        # Too deep: > 10 directory levels
        if len(path_segments) > 10:
            return False
        # Repeated segments indicate a loop (e.g., /a/b/a/b/...)
        if len(path_segments) != len(set(path_segments)):
            return False

        # Apache directory listing sort parameters
        # Example: ?C=N;O=A (column and order combinations)
        if re.search(r"(^|[&;])(C|O)=", parsed.query):
            return False

        # Authentication and action pages: often cause redirect loops
        # Examples: wp-login, wp-admin, action=delete, etc.
        if re.search(r"(login|logout|wp-admin|wp-login|action=)", url.lower()):
            return False

        return True

    except TypeError:
        print("TypeError for ", parsed)
        raise
