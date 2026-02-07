import re
import json
import sys
import atexit
import signal
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

def _save_report():
    """Print report to terminal and write to files."""
    print("\n" + "=" * 50)
    print("FINAL CRAWL ANALYTICS")
    print("=" * 50)
    print(f"Unique pages: {len(unique_pages)}")
    print(f"Longest page: {longest_page[0]}")
    print(f"Longest page word count: {longest_page[1]}")

    print("\nTop 50 words (stopwords removed):")
    for w, c in word_counts.most_common(50):
        print(f"  {w}\t{c}")

    print(f"\nSubdomains ({len(subdomain_pages)} total, alphabetical):")
    for sub in sorted(subdomain_pages.keys()):
        print(f"  {sub}, {len(subdomain_pages[sub])}")

    # Write structured JSON
    data = {
        "unique_pages_count": len(unique_pages),
        "longest_page": {"url": longest_page[0], "word_count": longest_page[1]},
        "top_50_words": word_counts.most_common(50),
        "subdomains": {sub: len(urls) for sub, urls in sorted(subdomain_pages.items())},
    }
    with open("report.json", "w") as f:
        json.dump(data, f, indent=2)

    # Write human-readable report
    with open("final_report_stats.txt", "w", encoding="utf-8") as f:
        f.write(f"Unique pages: {len(unique_pages)}\n")
        f.write(f"Longest page: {longest_page[0]}\n")
        f.write(f"Longest page word count: {longest_page[1]}\n\n")
        f.write("Top 50 words (stopwords removed):\n")
        for w, c in word_counts.most_common(50):
            f.write(f"{w}\t{c}\n")
        f.write(f"\nSubdomains (alphabetical) with unique page counts:\n")
        for sub in sorted(subdomain_pages.keys()):
            f.write(f"{sub}, {len(subdomain_pages[sub])}\n")

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
    """Collect analytics: unique pages, word freq, longest page, subdomains."""
    global longest_page

    clean_url, _ = urldefrag(url)

    # Extract visible text (strips boilerplate)
    text = _extract_visible_text(soup)
    tokens = _tokenize(text)

    # Skip low-content pages
    if len(tokens) < 50:
        return

    # Track unique pages
    unique_pages.add(clean_url)

    # Track subdomains (unique URLs per subdomain)
    parsed = urlparse(clean_url)
    hostname = (parsed.hostname or "").lower()
    if hostname.endswith(".uci.edu") or hostname == "uci.edu":
        subdomain_pages[hostname].add(clean_url)

    # Longest page by word count
    wc = len(tokens)
    if wc > longest_page[1]:
        longest_page = (clean_url, wc)

    # Filter tokens, then compute frequencies using PartA logic
    filtered = [t for t in tokens
                if 2 <= len(t) <= 30
                and t not in STOP_WORDS
                and t not in JUNK_WORDS]
    page_freqs = _compute_word_frequencies(filtered)

    # Cap per-page contribution to avoid single-page skew
    PER_PAGE_CAP = 10
    for w, c in page_freqs.items():
        word_counts[w] += min(c, PER_PAGE_CAP)


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
        if "replytocom=" in query_lower or "share=" in query_lower:
            return False
        if "oembed" in query_lower or "format=xml" in query_lower:
            return False
        # DokuWiki media/action traps
        if "do=media" in query_lower or "tab_files=" in query_lower \
           or "tab_details=" in query_lower or "do=revisions" in query_lower \
           or "do=backlink" in query_lower or "do=recent" in query_lower \
           or "do=index" in query_lower:
            return False
        # filter=[] pagination traps on news/listing pages
        if "filter%5b" in query_lower or "filter[" in query_lower:
            return False

        # --- path-based filters ---
        if path_lower.endswith("/feed") or "/feed/" in path_lower:
            return False
        if path_lower.endswith(".xml"):
            return False

        bad_paths = ["/wp-json/", "/wp-content/uploads/"]
        if any(bp in path_lower for bp in bad_paths):
            return False

        # GitLab trap — thousands of repos each with issues/forks/starrers
        if "gitlab" in (parsed.hostname or "").lower():
            return False

        # filter non-page file extensions
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

        # calendar traps
        if re.search(r"(calendar|date|event)", path_lower) and re.search(
            r"\d{4}[-/]\d{2}", parsed.path
        ):
            return False

        # nested seminar-series trap
        if path_lower.count("seminar-series") > 1:
            return False

        # excessively long URLs
        if len(url) > 200:
            return False

        # repeated directory segments
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) > 10:
            return False
        if len(parts) != len(set(parts)):
            return False

        # Apache directory listing sort params (uses ; separator, and
        # parsed.query doesn't include the leading ?)
        if re.search(r"(^|[&;])(C|O)=", parsed.query):
            return False

        # login / admin pages
        if re.search(r"(login|logout|wp-admin|wp-login|action=)", url.lower()):
            return False

        return True

    except TypeError:
        print("TypeError for ", parsed)
        raise
