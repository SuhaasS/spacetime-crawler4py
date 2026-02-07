import re
from urllib.parse import urlparse, urljoin, urldefrag

from bs4 import BeautifulSoup


ALLOWED_DOMAINS = [
    ".ics.uci.edu",
    ".cs.uci.edu",
    ".informatics.uci.edu",
    ".stat.uci.edu",
]


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
