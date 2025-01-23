import os
import re
import sys
import subprocess
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse
from collections import deque
import asyncio
import aiohttp

from aiohttp import ClientSession
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright

app = FastAPI()

OUTPUT_DIR = f"{os.path.expanduser('~')}/Desktop/product-discoverer/output_files"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def ensure_playwright_installed():
    """
    Ensure that Playwright is installed and the necessary browsers are set up.
    This function will:
      1. Attempt to import 'playwright'.
      2. If the import fails, install 'playwright' via pip.
      3. Then run 'playwright install' to download the default browser engines.

    You could customize the 'playwright install' command to install only
    specific browsers like 'playwright install chromium' if desired.
    """
    try:
        import playwright  # noqa  (Just a test import)
        print("Playwright is already installed.")
    except ImportError:
        print("Playwright not found. Installing via pip...")
        # Install 'playwright' with pip
        subprocess.run([sys.executable, "-m", "pip", "install", "playwright"], check=True)
        
        print("Installing default browsers for Playwright...")
        # Run 'playwright install' which installs chromium, firefox, and webkit by default
        subprocess.run(["playwright", "install"], check=True)

async def loader(domain: str, interval: int = 5):
    """
    Periodically print a loading message for a given domain.
    
    Args:
        domain (str): The domain being crawled.
        interval (int): Number of seconds to wait between prints.
    """
    try:
        while True:
            print(f"[LOADER] Crawling {domain} ...")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        # This exception is raised when we cancel the task
        print(f"[LOADER] Stopped loader for {domain}")
        raise
    

################################################################################
# Step 1: Headless Browser Fetch (with optional infinite scroll)
################################################################################

async def fetch_html_headless(url: str, scroll_wait: float = 1.5, max_scrolls: int = 5):
    """
    Uses Playwright in headless mode to open a page, optionally perform
    'infinite scroll', and then return the final rendered HTML.

    Args:
        url (str): The URL to load.
        scroll_wait (float): Seconds to wait between scroll steps.
        max_scrolls (int): How many times to scroll to bottom (limit for safety).

    Returns:
        str | None: The final HTML after scrolling, or None if error.
    """
    # Playwright must be installed: 'pip install playwright' 
    # and you must run 'playwright install' once to install browser engines.
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url)

            # Simple approach to "infinite scroll" a fixed number of times.
            # If the site needs more scrolling to load everything,
            # increase max_scrolls or detect when there's no new content.
            for _ in range(max_scrolls):
                # Scroll to bottom
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(scroll_wait)
            
            # Get final rendered HTML
            html = await page.content()
            await browser.close()
            return html
    except Exception as e:
        print(f"[Headless Error] Could not fetch {url}: {e}")
        return None


################################################################################
# Step 2: BFS-based extraction, but each page load uses the headless browser
################################################################################

async def extract_product_urls_headless(
    domain: str,
    output_file: str,
    max_concurrency: int = 5,
    chunk_size: int = 500,
    max_scrolls: int = 5
) -> int:
    """
    BFS crawl for all links on the given domain, using a headless browser
    for each page to handle JavaScript/infinite scrolling.

    This version uses two sets:
      - discovered:  to avoid adding duplicates to the queue/output file
      - visited:     to avoid re-fetching the same URL

    Args:
        domain (str): The starting domain or URL.
        output_file (str): Path where discovered URLs will be written.
        max_concurrency (int): Max parallel fetches.
        chunk_size (int): Write URLs to file in chunks of this size.
        max_scrolls (int): How many times to scroll each page.

    Returns:
        int: Total count of unique URLs discovered.
    """

    # Regex to exclude unwanted paths
    exclude_pattern = re.compile(
        r"chat|contact|reward|profile|club|write-to-us|return|payment|help|service|"
        r"user-agreement|policies|aboutus|history|blog|account|wishlist|viewcart|login|logout",
        re.IGNORECASE,
    )
    # Skip images
    exclude_extensions = (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp", ".ico", ".pdf", ".docx")

    # Prepare BFS structures
    queue = deque([domain])
    discovered = set([domain])  # Immediately mark domain as discovered
    visited = set()
    chunk_list = []
    total_count = 1  # domain is counted as first discovered

    # Domain check
    parsed_domain = urlparse(domain)
    domain_netloc = parsed_domain.netloc.replace("www.", "")

    # Concurrency limit
    semaphore = asyncio.Semaphore(max_concurrency)

    def flush_chunk():
        nonlocal chunk_list
        if chunk_list:
            with open(output_file, "a", encoding="utf-8") as f:
                f.write("\n".join(chunk_list) + "\n")
            chunk_list.clear()

    # Write initial domain to file so we see it as discovered
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(domain + "\n")

    while queue:
        current_url = queue.popleft()

        # If we've already fetched and processed this URL, skip
        if current_url in visited:
            continue

        # Acquire concurrency slot
        async with semaphore:
            html = await fetch_html_headless(current_url, max_scrolls=max_scrolls)

        # Mark it as visited after we attempt to fetch
        visited.add(current_url)

        if not html:
            # If no HTML, skip deeper parsing
            continue

        # Parse HTML
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"].strip()

            # 1) Skip javascript: links
            if href.lower().startswith("javascript:"):
                continue

            # 2) Build absolute URL
            full_url = urljoin(current_url, href)
            if full_url.endswith(":"):
                full_url = full_url[:-1]

            parsed_url = urlparse(full_url)

            # 3) Skip non-http/https
            if parsed_url.scheme not in ("http", "https"):
                continue

            # 4) Check domain (using substring check)
            if domain_netloc not in parsed_url.netloc.replace("www.", ""):
                continue

            # 5) Skip images
            if parsed_url.path.lower().endswith(exclude_extensions):
                continue

            # 6) Skip # anchors
            if "#" in parsed_url.geturl():
                continue

            # 7) Skip excluded paths
            if exclude_pattern.search(parsed_url.path):
                continue

            # 8) Normalize query
            query_dict = dict(parse_qs(parsed_url.query))
            normalized_query = urlencode(query_dict, doseq=True)
            normalized_url = parsed_url._replace(query=normalized_query).geturl().rstrip("/")

            # 9) Only if not discovered, add to queue + file chunk
            if normalized_url not in discovered:
                discovered.add(normalized_url)  # avoid duplicates in output
                queue.append(normalized_url)
                chunk_list.append(normalized_url)
                total_count += 1

                if len(chunk_list) >= chunk_size:
                    flush_chunk()

    # Flush leftover chunk
    flush_chunk()

    return total_count


################################################################################
# Step 3: Tying it together in a single domain crawl function
################################################################################

async def crawl_domain_headless(domain: str):
    """
    Crawl a domain using a headless browser to handle JS/infinite scroll.
    """
    
    ensure_playwright_installed()  # Dynamically install playwright if needed
    loader_task = asyncio.create_task(loader(domain))

    try:
        # Output file name
        domain_part = domain.replace("https://", "").replace("http://", "")
        try:
            filename_part = domain_part.split(".")[1]
        except IndexError:
            filename_part = domain_part

        output_file = os.path.join(
            OUTPUT_DIR, f"{filename_part}.txt"
        )

        # Clear out any existing file
        open(output_file, "w").close()

        # Run BFS with headless fetch
        total_found = await extract_product_urls_headless(
            domain=domain,
            output_file=output_file
        )

        print(f"Completed headless crawl of {domain}. Found {total_found} URLs.")
        print(f"Results in {output_file}")
    finally:
        # Stop the loader once crawling is finished (or if an error happens)
        loader_task.cancel()
        # Wait for the loader to actually finish
        try:
            await loader_task
        except asyncio.CancelledError:
            pass

# Helper function to validate domains asynchronously
async def validate_domains(domains: list[str]):
    """
    Validate a list of domains to ensure they are reachable.

    Args:
        domains (list[str]): A list of domain names to validate.

    Returns:
        list[str]: The updated list of valid domains with "https://" prefixed.

    Raises:
        HTTPException: If any domain is invalid or unreachable.
    """
    
    def ensure_protocol_and_www(url):
        # Parse the URL
        parsed_url = urlparse(url)
        
        # Add scheme (http/https) if missing
        if not parsed_url.scheme:
            url = "https://" + url
            parsed_url = urlparse(url)  # Re-parse after adding the scheme

        # Add www. if missing
        netloc = parsed_url.netloc
        if not netloc.startswith("www."):
            netloc = "www." + netloc

        # Reconstruct the URL with the modified netloc
        updated_url = urlunparse(
            (parsed_url.scheme, netloc, parsed_url.path, parsed_url.params, parsed_url.query, parsed_url.fragment)
        )
        return updated_url

    # Add https:// and www. to domains if missing
    updated_domains = [ensure_protocol_and_www(domain) for domain in domains]

    invalid_urls = []
    
    async def check_url(url: str, session: ClientSession):
        try:
            async with session.head(url, timeout=60) as response:
                if response.status >= 400:
                    invalid_urls.append(url)
        except Exception:
            invalid_urls.append(url)

    async with aiohttp.ClientSession() as session:
        tasks = [check_url(domain, session) for domain in updated_domains]
        await asyncio.gather(*tasks)

    if invalid_urls:
        raise HTTPException(
            status_code=400,
            detail=f"The following URLs are invalid or unreachable: {invalid_urls}"
        )

    return updated_domains

class CrawlRequest(BaseModel):
    """
    Pydantic model for crawl request payload.

    Attributes:
        domains (list[str]): list of domain names to crawl.
    """
    
    domains: list[str] = Field(..., min_items=10)

# API endpoint to start crawling
@app.post("/crawl/")
async def start_crawling(request: CrawlRequest, background_tasks: BackgroundTasks):
    """
    Start the crawling process for a list of domains.

    Args:
        domains (list[str]): list of domains to crawl.
        background_tasks (BackgroundTasks): FastAPI background tasks manager.

    Returns:
        dict: A message indicating that crawling has started.
    """

    updated_domains = await validate_domains(request.domains)

    for domain in updated_domains:
        background_tasks.add_task(crawl_domain_headless, domain)

    return {"message": "Crawling started for provided domains."}

@app.get("/download/{domain}")
async def download_results(domain: str):
    """
    Downloads the results file for a given domain.

    Args:
        domain: The domain to download the results for.
    """
    # File name
    domain_part = domain.replace("https://", "").replace("http://", "")
    try:
        filename_part = domain_part.split(".")[1]
    except IndexError:
        filename_part = domain_part

    file_name = os.path.join(OUTPUT_DIR, f"{filename_part}.txt")
    if os.path.exists(file_name):
        return FileResponse(file_name, filename=file_name)
    else:
        raise HTTPException(status_code=404, detail="Results file not found.")

if __name__ == "__main__":    
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    