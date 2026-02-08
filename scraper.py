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


ALLOWED_DOMAINS = [
    ".ics.uci.edu",
    ".cs.uci.edu",
    ".informatics.uci.edu",
    ".stat.uci.edu",
]

# from https://www.ranks.nl/stopwords (default english list)
STOP_WORDS = {
    "a","about","above","after","again","against","all","am","an","and","any",
    "are","aren't","as","at","be","because","been","before","being","below",
    "between","both","but","by","can't","cannot","could","couldn't","did",
    "didn't","do","does","doesn't","doing","don't","down","during","each",
    "few","for","from","further","had","hadn't","has","hasn't","have",
    "haven't","having","he","he'd","he'll","he's","her","here","here's",
    "hers","herself","him","himself","his","how","how's","i","i'd","i'll",
    "i'm","i've","if","in","into","is","isn't","it","it's","its","itself",
    "let's","me","more","most","mustn't","my","myself","no","nor","not","of",
    "off","on","once","only","or","other","ought","our","ours","ourselves",
    "out","over","own","same","shan't","she","she'd","she'll","she's",
    "should","shouldn't","so","some","such","than","that","that's","the",
    "their","theirs","them","themselves","then","there","there's","these",
    "they","they'd","they'll","they're","they've","this","those","through",
    "to","too","under","until","up","very","was","wasn't","we","we'd",
    "we'll","we're","we've","were","weren't","what","what's","when","when's",
    "where","where's","which","while","who","who's","whom","why","why's",
    "with","won't","would","wouldn't","you","you'd","you'll","you're",
    "you've","your","yours","yourself","yourselves",
}

# HTML/markup artifacts that leak through as "words"
JUNK_WORDS = {
    "html", "update", "automatic", "markdown", "rmd", "git",
    "file", "files", "store", "ds", "href", "https", "http",
    "www", "nbsp", "amp", "quot", "lt", "gt",
}

# --- report data ---
unique_pages: set[str] = set()
word_counts: Counter[str] = Counter()
longest_page = ("", 0)
subdomain_pages: defaultdict[str, set[str]] = defaultdict(set)  # subdomain -> set of defragmented URLs

# Thread safety locks
stats_lock = Lock()  # Protects unique_pages, word_counts, longest_page, subdomain_pages
log_lock = Lock()    # Protects log file writes

# Initialize crawl log file
CRAWL_LOG_FILE = "crawl_log.txt"
try:
    with open(CRAWL_LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"Crawl started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
except Exception:
    pass

def _log_crawl(url, word_count, subdomain):
    """Append crawl event to log file (thread-safe)."""
    try:
        with log_lock:
            with open(CRAWL_LOG_FILE, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%H:%M:%S")
                # Read unique_pages count with stats_lock
                with stats_lock:
                    page_count = len(unique_pages)
                f.write(f"[{timestamp}] Pages: {page_count} | Words: {word_count} | {subdomain}\n")
                f.write(f"  {url}\n\n")
    except Exception:
        pass

def _save_report():
    """Print report to terminal and write to files (thread-safe)."""
    # Acquire lock to read all statistics atomically
    with stats_lock:
        unique_count = len(unique_pages)
        longest_url, longest_count = longest_page
        top_words = word_counts.most_common(50)
        subdomain_counts = {sub: len(urls) for sub, urls in subdomain_pages.items()}

    print("\n" + "=" * 50)
    print("FINAL CRAWL ANALYTICS")
    print("=" * 50)
    print(f"Unique pages: {unique_count}")
    print(f"Longest page: {longest_url}")
    print(f"Longest page word count: {longest_count}")

    print("\nTop 50 words (stopwords removed):")
    for w, c in top_words:
        print(f"  {w}\t{c}")

    print(f"\nSubdomains ({len(subdomain_counts)} total, alphabetical):")
    for sub in sorted(subdomain_counts.keys()):
        print(f"  {sub}, {subdomain_counts[sub]}")

    # Write structured JSON
    data = {
        "unique_pages_count": unique_count,
        "longest_page": {"url": longest_url, "word_count": longest_count},
        "top_50_words": top_words,
        "subdomains": {sub: count for sub, count in sorted(subdomain_counts.items())},
    }
    with open("report.json", "w") as f:
        json.dump(data, f, indent=2)

    # Write human-readable report
    with open("final_report_stats.txt", "w", encoding="utf-8") as f:
        f.write(f"Unique pages: {unique_count}\n")
        f.write(f"Longest page: {longest_url}\n")
        f.write(f"Longest page word count: {longest_count}\n\n")
        f.write("Top 50 words (stopwords removed):\n")
        for w, c in top_words:
            f.write(f"{w}\t{c}\n")
        f.write(f"\nSubdomains (alphabetical) with unique page counts:\n")
        for sub in sorted(subdomain_counts.keys()):
            f.write(f"{sub}, {subdomain_counts[sub]}\n")

atexit.register(_save_report)

def _handle_sigterm(signum, frame):
    """Ensure report is saved when process is killed."""
    _save_report()
    sys.exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)


def _tokenize(text):
    """
    Tokenize text into alphanumeric tokens (adapted from PartA.py).
    Runtime Complexity: O(n) where n is total characters in text.
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
    Compute word frequencies from a list of tokens (from PartA.py).
    Runtime Complexity: O(T) where T is the total number of tokens.
    """
    frequencies = {}
    for token in tokens:
        if token in frequencies:
            frequencies[token] += 1
        else:
            frequencies[token] = 1
    return frequencies


def _extract_visible_text(soup):
    """Extract main content text, stripping boilerplate (nav, footer, sidebar, etc.)."""
    # Remove non-content tags
    for tag in soup(["script", "style", "noscript", "svg", "iframe",
                     "form", "meta", "link"]):
        tag.decompose()

    # Try to focus on main content area
    main = None
    for selector in ["main", "article", "#content", "#main",
                     ".content", ".entry-content", ".post-content",
                     ".page-content", ".site-content"]:
        main = soup.select_one(selector)
        if main:
            break

    if not main:
        main = soup.body or soup

    # Remove boilerplate within chosen container
    for tag in main.find_all(["header", "footer", "nav", "aside"]):
        tag.decompose()

    for node in main.find_all(
        attrs={"class": re.compile(
            r"(menu|nav|footer|header|sidebar|breadcrumb|cookie|popup)", re.I
        )}
    ):
        node.decompose()
    for node in main.find_all(
        attrs={"id": re.compile(
            r"(menu|nav|footer|header|sidebar|breadcrumb|cookie|popup)", re.I
        )}
    ):
        node.decompose()

    text = main.get_text(separator=" ")
    return re.sub(r"\s+", " ", text).strip()


def _record_stats(url, soup):
    """Collect analytics: unique pages, word freq, longest page, subdomains (thread-safe)."""
    global longest_page

    clean_url, _ = urldefrag(url)

    # Extract visible text (strips boilerplate)
    text = _extract_visible_text(soup)
    tokens = _tokenize(text)

    # Skip low-content pages
    if len(tokens) < 50:
        return

    # Parse hostname
    parsed = urlparse(clean_url)
    hostname = (parsed.hostname or "").lower()

    # Filter tokens, then compute frequencies using PartA logic
    filtered = [t for t in tokens
                if 2 <= len(t) <= 30
                and t not in STOP_WORDS
                and t not in JUNK_WORDS]
    page_freqs = _compute_word_frequencies(filtered)
    wc = len(tokens)

    # Update global statistics with lock
    with stats_lock:
        # Track unique pages
        unique_pages.add(clean_url)

        # Track subdomains (unique URLs per subdomain)
        if hostname.endswith(".uci.edu") or hostname == "uci.edu":
            subdomain_pages[hostname].add(clean_url)

        # Longest page by word count
        if wc > longest_page[1]:
            longest_page = (clean_url, wc)

        # Cap per-page contribution to avoid single-page skew
        PER_PAGE_CAP = 10
        for w, c in page_freqs.items():
            word_counts[w] += min(c, PER_PAGE_CAP)

    # Log this crawl event
    _log_crawl(clean_url, wc, hostname)


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    if resp.status != 200 or resp.raw_response is None:
        return list()

    try:
        content = resp.raw_response.content
    except AttributeError:
        return list()

    if not content:
        return list()

    # Only run analytics on actual HTML pages
    content_type = ""
    try:
        content_type = (resp.raw_response.headers.get("Content-Type") or "").lower()
    except Exception:
        pass

    soup = BeautifulSoup(content, "lxml")
    base_url = resp.raw_response.url if resp.raw_response.url else url

    # Record stats only for HTML pages
    if "text/html" in content_type:
        _record_stats(base_url, soup)

    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        absolute = urljoin(base_url, href)
        absolute, _ = urldefrag(absolute)
        links.append(absolute)

    return links


def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        hostname = parsed.hostname
        if hostname is None:
            return False

        # check allowed domains
        if not any(
            hostname == domain.lstrip(".")
            or hostname.endswith(domain)
            for domain in ALLOWED_DOMAINS
        ):
            return False

        path_lower = parsed.path.lower()
        query_lower = (parsed.query or "").lower()

        # --- query-string traps ---

        # wordpress comment reply links (?replytocom=) and social share links
        # (?share=) generate infinite unique urls that all point to the same
        # page content, creating a crawler trap
        if "replytocom=" in query_lower or "share=" in query_lower:
            return False

        # oembed endpoints and xml format params return machine-readable
        # metadata (json/xml) instead of crawlable html pages
        if "oembed" in query_lower or "format=xml" in query_lower:
            return False

        # dokuwiki (e.g. intranet.ics.uci.edu/doku.php) generates combinatorial
        # urls from action params like do=media, do=revisions, do=backlink,
        # do=recent, do=index, and tab params (tab_files, tab_details). each
        # combination creates a unique url with no new content, producing
        # thousands of trap urls from a single wiki page
        if "do=media" in query_lower or "tab_files=" in query_lower \
           or "tab_details=" in query_lower or "do=revisions" in query_lower \
           or "do=backlink" in query_lower or "do=recent" in query_lower \
           or "do=index" in query_lower:
            return False

        # wordpress news/listing pages use filter[] query params for faceted
        # filtering (e.g. ?filter[category]=x&filter[tag]=y). the brackets
        # get url-encoded as %5b/%5d, and each combination creates a new url
        # pointing to the same or near-identical filtered listing
        if "filter%5b" in query_lower or "filter[" in query_lower:
            return False

        # --- path-based filters ---

        # rss/atom feed endpoints return xml syndication data, not html pages.
        # wordpress sites expose /feed and /feed/ on every post and category
        if path_lower.endswith("/feed") or "/feed/" in path_lower:
            return False

        # xml files (sitemaps, rss feeds, config files) are not crawlable
        # html content
        if path_lower.endswith(".xml"):
            return False

        # /wp-json/ is the wordpress rest api — returns json, not html pages.
        # /wp-content/uploads/ links directly to uploaded media files (pdfs,
        # images, docs) which have no links to extract
        bad_paths = ["/wp-json/", "/wp-content/uploads/"]
        if any(bp in path_lower for bp in bad_paths):
            return False

        # gitlab.ics.uci.edu hosts thousands of git repos, each with
        # sub-pages for issues, merge requests, forks, starrers, branches,
        # commits, etc. this creates an enormous trap of tens of thousands
        # of urls with mostly boilerplate ui content and no useful text
        if "gitlab" in (parsed.hostname or "").lower():
            return False

        # skip non-html file extensions — binary files, media, archives,
        # documents, and source code files contain no extractable links
        # and would waste crawl budget
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz"
            + r"|img|sql|apk|ppsx|odc|war|db|lif"
            + r"|c|cc|cpp|h|hpp|java|py|r|m|mat|o)$",
            path_lower,
        ):
            return False

        # --- trap detection ---

        # calendar/event pages with date patterns (e.g. /calendar/2023-01/)
        # generate infinite urls — one for every possible date combination.
        # if the path contains calendar/date/event keywords AND a yyyy-mm
        # date pattern, it's almost certainly a calendar trap
        if re.search(r"(calendar|date|event)", path_lower) and re.search(
            r"\d{4}[-/]\d{2}", parsed.path
        ):
            return False

        # some sites nest the same path segment recursively
        # (e.g. /seminar-series/seminar-series/...) creating infinite depth.
        # if "seminar-series" appears more than once in the path, it's a loop
        if path_lower.count("seminar-series") > 1:
            return False

        # extremely long urls are almost always dynamically generated trap
        # urls or deeply nested paths — real content pages rarely exceed
        # 200 characters
        if len(url) > 200:
            return False

        # repeated directory segments (e.g. /a/b/a/b/...) indicate a path
        # loop trap. also cap total depth at 10 segments since legitimate
        # site structures rarely go that deep
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) > 10:
            return False
        if len(parts) != len(set(parts)):
            return False

        # apache directory listings add sort params like ?C=N;O=A (column
        # and order). each sort combination creates a unique url for the
        # same directory listing. uses ; as separator instead of &, and
        # parsed.query doesn't include the leading ? so we match from ^
        if re.search(r"(^|[&;])(C|O)=", parsed.query):
            return False

        # login, logout, and admin pages (wp-admin, wp-login) require
        # authentication and often redirect in loops. action= params
        # trigger server-side actions rather than serving content
        if re.search(r"(login|logout|wp-admin|wp-login|action=)", url.lower()):
            return False

        return True

    except TypeError:
        print("TypeError for ", parsed)
        raise
