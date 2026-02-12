# Web Crawler Architecture Overview

## Project Summary
This is a multi-threaded, polite web crawler designed to crawl UCI domain websites. It implements politeness policies, crawler trap detection, and comprehensive analytics collection.

---

## Architecture Components

### 1. **launch.py** - Entry Point
**Role:** Application bootstrap and configuration

**What it does:**
- Parses command-line arguments (`--restart`, `--config_file`)
- Loads configuration from `config.ini`
- Registers with the cache server
- Initializes and starts the crawler

**Key Functions:**
- `main(config_file, restart)`: Entry point that orchestrates startup

---

### 2. **crawler/__init__.py** - Crawler Coordinator
**Role:** Multi-threading orchestration

**What it does:**
- Creates the shared frontier (URL queue)
- Spawns multiple worker threads (default: 4)
- Manages worker lifecycle (start, join)

**Key Classes:**
- `Crawler`: Main coordinator class
  - `start()`: Spawn workers and block until completion
  - `start_async()`: Spawn workers without blocking
  - `join()`: Wait for all workers to finish

---

### 3. **crawler/frontier.py** - URL Queue Manager
**Role:** Politeness enforcement and URL state management

**What it does:**
- Organizes URLs by domain for efficient politeness
- Enforces 500ms delay between requests to same domain
- Persists state to disk (`frontier.shelve`) for crash recovery
- Prevents duplicate URL crawls
- Thread-safe operations for concurrent workers

**Key Data Structures:**
- `domain_queues`: `defaultdict(deque)` - Separate queue per domain
- `last_accessed`: `dict` - Timestamp of last access per domain
- `active_downloads`: `int` - Count of in-flight downloads
- `save`: `shelve` - Persistent storage (urlhash -> (url, completed))

**Key Methods:**
- `get_tbd_url()`: Returns next URL respecting politeness delay
- `add_url(url)`: Adds new URL to appropriate domain queue
- `mark_url_complete(url)`: Marks URL as done, decrements active count
- `has_pending_urls()`: Checks if crawling should continue

**Politeness Policy:**
- Minimum 500ms between requests to same domain
- Rotates through domains to distribute load
- If all domains are in cooldown, workers sleep 100ms and retry

---

### 4. **crawler/worker.py** - Worker Threads
**Role:** Execute the crawl loop

**What it does:**
- Requests URLs from frontier (blocks if politeness delay active)
- Downloads pages using `utils.download` module
- Extracts links using scraper module
- Adds discovered URLs back to frontier
- Marks URLs as complete
- Stops when frontier is exhausted

**Key Methods:**
- `run()`: Main crawl loop (runs until frontier empty)

**Crawl Loop:**
```
while True:
    1. Get URL from frontier (respects politeness)
    2. Download page
    3. Extract links (scraper.scraper())
    4. Add discovered links to frontier
    5. Mark URL complete
```

**Library Restrictions:**
- Enforces that `scraper.py` doesn't use `requests` or `urllib.request`
- Uses `utils.download` instead (cache server integration)

---

### 5. **scraper.py** - Link Extraction & Analytics
**Role:** URL validation, link extraction, and statistics collection

**What it does:**
- Validates URLs (domain, file type, trap detection)
- Extracts links from HTML pages
- Collects analytics: word frequencies, unique pages, longest page, subdomains
- Thread-safe statistics updates
- Generates final report on exit

**Main Entry Points:**
- `scraper(url, resp)`: Main entry - extracts and validates links
- `extract_next_links(url, resp)`: Parses HTML and extracts hrefs
- `is_valid(url)`: Comprehensive URL validation

**Analytics Collected:**
- Unique pages crawled
- Top 50 words (stopwords removed, per-page cap of 10)
- Longest page by word count
- Pages per subdomain

**URL Validation Checks:**
1. **Protocol:** Only HTTP/HTTPS
2. **Domain:** Must be in ALLOWED_DOMAINS (`.ics.uci.edu`, `.cs.uci.edu`, etc.)
3. **Query String Traps:**
   - WordPress params: `replytocom=`, `share=`, `filter[]=`
   - DokuWiki params: `do=media`, `tab_files=`, etc.
   - Apache sort params: `C=`, `O=`
4. **Path-Based Filters:**
   - RSS feeds: `/feed`, `.xml`
   - WordPress API: `/wp-json/`, `/wp-content/uploads/`
   - GitLab (entire domain blocked - massive trap)
5. **File Extensions:** Blocks 60+ non-HTML file types
6. **Crawler Traps:**
   - Calendar pages with dates: `/calendar/2023-01/`
   - Recursive segments: `/seminar-series/seminar-series/`
   - URL length > 200 characters
   - Path depth > 10 levels
   - Repeated path segments (loop detection)
   - Login/logout/admin pages

**Text Processing:**
- `_tokenize(text)`: Extracts alphanumeric tokens
- `_compute_word_frequencies(tokens)`: Counts word occurrences
- `_extract_visible_text(soup)`: Removes boilerplate (nav, footer, etc.)
- `_record_stats(url, soup)`: Updates global analytics (thread-safe)

**Output Files:**
- `crawl_log.txt`: Real-time crawl activity log
- `report.json`: Machine-readable analytics
- `final_report_stats.txt`: Human-readable analytics
- Exit handlers: `atexit` and `SIGTERM` ensure report saves on shutdown

---

## Thread Safety

### Synchronization Mechanisms:
1. **Frontier:** `RLock` protects all data structures
2. **Scraper:**
   - `stats_lock`: Protects analytics data structures
   - `log_lock`: Protects log file writes

### Why Thread Safety Matters:
- Multiple workers access shared frontier concurrently
- Multiple workers update global statistics simultaneously
- Without locks: race conditions, data corruption, duplicate URLs

---

## Data Flow

```
launch.py
    ↓
Crawler.__init__()
    ↓
Frontier.__init__() → Load save file or seed URLs
    ↓
Crawler.start() → Spawn 4 Worker threads
    ↓
Worker.run() loop:
    ├─→ Frontier.get_tbd_url() → Returns URL respecting politeness
    ├─→ download(url) → Fetch page via cache server
    ├─→ scraper.scraper(url, resp) → Extract & validate links
    │       ├─→ extract_next_links() → Parse HTML, find hrefs
    │       ├─→ _record_stats() → Update analytics
    │       └─→ is_valid() → Filter each link
    ├─→ Frontier.add_url() → Queue discovered URLs
    └─→ Frontier.mark_url_complete() → Update state
```

---

## Key Design Decisions

### 1. **Per-Domain Queuing**
- Organizes URLs by domain for efficient politeness
- Allows rotation between domains without complex scheduling

### 2. **Politeness in Frontier, Not Workers**
- Centralizes delay logic in one place
- Workers just request URLs - frontier handles timing

### 3. **Active Download Tracking**
- `active_downloads` counter prevents premature shutdown
- Workers may be downloading pages that will discover new URLs

### 4. **Persistent State (shelve)**
- Crash recovery - can resume crawls
- Duplicate detection - tracks seen URLs

### 5. **Per-Page Word Cap (10)**
- Prevents single page from dominating word frequencies
- More representative of overall corpus

### 6. **Comprehensive Trap Detection**
- Learned from real UCI websites
- Comments explain why each rule exists

---

## Configuration (config.ini)

```ini
[IDENTIFICATION]
USERAGENT = IR UW26 35485800, 79822855, 30679988, 32438497

[CONNECTION]
HOST = styx.ics.uci.edu
PORT = 9000

[CRAWLER]
SEEDURL = https://www.ics.uci.edu,...
POLITENESS = 0.5  # 500ms delay

[LOCAL PROPERTIES]
SAVE = frontier.shelve
THREADCOUNT = 4
```

---

## Performance Characteristics

### Time Complexity:
- URL validation: O(1) for most checks, O(n) for path segment loops
- Text tokenization: O(n) where n = character count
- Word frequency: O(m) where m = token count
- Frontier operations: O(1) amortized (dict/deque)

### Space Complexity:
- Frontier: O(u) where u = unique URLs discovered
- Analytics: O(p) where p = unique pages + unique words

### Scalability:
- Thread count configurable (default: 4)
- Linear speedup with threads (up to politeness limits)
- Bottleneck: Politeness delay (500ms per domain)

---

## Testing Approach

### Validation:
- Assert checks for forbidden libraries
- URL validation unit testable (pure function)
- Mock factories in Crawler for dependency injection

### Debugging:
- Comprehensive logging (Worker, Frontier, etc.)
- Real-time crawl log with timestamps
- Persistent state for post-mortem analysis

---

## Summary of Improvements Made

### Code Quality:
✅ Added comprehensive docstrings to all modules, classes, and functions
✅ Organized code with section headers for better navigation
✅ Improved variable names for clarity
✅ Consolidated related logic
✅ Removed redundant comments

### Documentation:
✅ Module-level docstrings explain role in architecture
✅ Function docstrings explain args, returns, and behavior
✅ Inline comments explain WHY, not WHAT
✅ Trap detection comments explain real-world reasoning

### Readability:
✅ Grouped related filters with clear section headers
✅ Improved code formatting and structure
✅ Better constant organization
✅ Clearer thread-safety documentation

### Functionality:
✅ **NO CHANGES** - All behavior preserved exactly as original
✅ Thread safety mechanisms unchanged
✅ Politeness policy unchanged
✅ Validation rules unchanged
✅ Analytics collection unchanged

---

## For Your Professor

**Key Points to Emphasize:**

1. **Multi-threading with Politeness**: 4 workers share a thread-safe frontier that enforces per-domain delays

2. **Intelligent Trap Detection**: 15+ categories of traps identified and blocked through testing

3. **Scalable Architecture**: Clean separation of concerns (frontier, workers, scraper)

4. **Crash Recovery**: Persistent state allows resuming crawls

5. **Comprehensive Analytics**: Real-time logging + final report with word frequencies and subdomain stats

6. **Thread Safety**: Proper use of locks prevents race conditions

7. **Clean Code**: Well-documented, organized, and maintainable
