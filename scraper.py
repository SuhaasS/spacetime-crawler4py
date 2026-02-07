import re
import json
import atexit
from collections import Counter
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

# --- report data ---
unique_pages = set()
word_counts = Counter()
longest_page = ("", 0)
subdomains = Counter()


def _save_report():
    data = {
        "unique_pages_count": len(unique_pages),
        "longest_page": {"url": longest_page[0], "word_count": longest_page[1]},
        "top_50_words": word_counts.most_common(50),
        "subdomains": sorted(subdomains.items()),
    }
    with open("report.json", "w") as f:
        json.dump(data, f, indent=2)

atexit.register(_save_report)


def _record_stats(url, soup):
    global longest_page

    # unique pages (defragmented url)
    clean_url, _ = urldefrag(url)
    unique_pages.add(clean_url)

    # extract visible text
    text = soup.get_text(separator=" ", strip=True)
    words = re.findall(r"[a-z']+", text.lower())
    words = [w for w in words if len(w) > 1 and w not in STOP_WORDS]

    # longest page
    if len(words) > longest_page[1]:
        longest_page = (clean_url, len(words))

    # word frequencies
    word_counts.update(words)

    # subdomains
    parsed = urlparse(clean_url)
    hostname = parsed.hostname
    if hostname and hostname.endswith(".uci.edu"):
        subdomains[hostname] += 1



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

    soup = BeautifulSoup(content, "html.parser")
    base_url = resp.raw_response.url if resp.raw_response.url else url

    # record stats for report
    _record_stats(base_url, soup)

    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:")):
            continue
        absolute = urljoin(base_url, href)
        # strip fragments
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
            + r"|img|sql|apk|ppsx|odc|war|db|lif)$",
            path_lower,
        ):
            return False

        # avoid calendar traps
        if re.search(r"(calendar|date|event)", path_lower) and re.search(
            r"\d{4}[-/]\d{2}", parsed.path
        ):
            return False

        # avoid excessively long urls (likely traps)
        if len(url) > 200:
            return False

        # avoid paths with repeated directories (trap detection)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) > 10:
            return False
        if len(parts) != len(set(parts)):
            return False

        # avoid common trap patterns
        if re.search(r"(login|logout|wp-admin|wp-login|action=)", url.lower()):
            return False

        return True

    except TypeError:
        print("TypeError for ", parsed)
        raise
