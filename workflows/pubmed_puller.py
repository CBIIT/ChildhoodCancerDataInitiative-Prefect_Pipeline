"""
PubMed DOI Extraction Pipeline (Prefect)
=========================================

Searches PubMed via NCBI E-utilities for publications matching a combination of:
    - a dbGaP phs accession (e.g. "phs000424" or "phs000424.v9")
    - author name(s)
    - keyword(s) (searched in Title/Abstract)
    - an optional publication date range

...and extracts DOIs plus basic metadata (PMID, title, journal, year, authors)
into CSV files, then uploads the whole output directory.

Two modes:
    1. Single query: pass phs_accession / authors / keywords / date_from / date_to
        directly as flow parameters.
    2. Batch: pass `query_file`, a local path to a CSV/TSV where each row is one
        query (see `load_queries_from_file` for the expected columns). Individual
        criteria parameters are ignored when `query_file` is given.

Either way, the flow always writes a timestamped output directory containing
one CSV per query plus a manifest.csv, and uploads that directory as a whole --
never a single loose file -- so downstream handling doesn't need to special-case
the single-query path.

IMPORTANT NOTES
---------------
1. dbGaP phs accessions are NOT a structured PubMed search field. This script
    matches them as a free-text term, which only finds papers where an author
    or indexer explicitly wrote the accession number somewhere PubMed indexes
    (title, abstract, or other indexed text). It will miss papers that cite the
    dataset without stating the accession. For more authoritative accession ->
    publication links, cross-check against dbGaP's own study page (which lists
    associated publications) or use NCBI's ELink from the `gap` database to
    `pubmed` if you have the dbGaP internal UID.
2. NCBI strongly recommends an `email` param on every request (they use it to
    contact you before blocking access if something goes wrong), and rate-limits
    to 3 requests/second without an API key, or 10/second with one. Get a free
    key at https://www.ncbi.nlm.nih.gov/account/settings/ and pass it in to go faster.
3. Requires: `pip install prefect requests`

RATE LIMITING
-------------
A shared, thread-safe RateLimiter enforces a minimum interval between outgoing
requests, regardless of how many tasks/threads are making them. This keeps you
compliant even if Prefect task runners ever execute batches concurrently, which
independent per-call sleeps would not guarantee. HTTP 429 responses are also
detected and backed off explicitly rather than just retried blindly.

USAGE
-----
Edit the parameters in the `if __name__ == "__main__"` block, or import
`pubmed_doi_flow` and call it directly / deploy it with Prefect.
"""

import csv
import json
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import requests
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash
from src.utils import get_time, folder_ul, file_dl

NCBI_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


# --------------------------------------------------------------------------- #
# Rate limiting
# --------------------------------------------------------------------------- #
class RateLimiter:
    """
    Thread-safe rate limiter enforcing a minimum interval between calls.

    Unlike sleeping a fixed amount after each request, this tracks the
    wall-clock time of the last call and only sleeps as long as needed to
    reach `min_interval` since then -- and a lock ensures that's true even
    if multiple threads/tasks are calling it at once, which matters if you
    ever parallelize batches with Prefect's `.submit()`.
    """

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            remaining = self.min_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
            self._last_call = time.monotonic()


# NCBI limits: 3 req/s without a key, 10 req/s with one. A small safety
# margin is built in (2.5/s and 9/s) so timing jitter doesn't tip you over.
_NO_KEY_LIMITER = RateLimiter(min_interval=1 / 2.5)
_WITH_KEY_LIMITER = RateLimiter(min_interval=1 / 9)


def _get_limiter(api_key: Optional[str]) -> RateLimiter:
    return _WITH_KEY_LIMITER if api_key else _NO_KEY_LIMITER


def _ncbi_get(url: str, params: dict, api_key: Optional[str], timeout: int) -> requests.Response:
    """GET against an NCBI endpoint, respecting the shared rate limiter and
    backing off explicitly on HTTP 429 (Too Many Requests) before Prefect's
    task-level retry kicks in."""
    limiter = _get_limiter(api_key)
    logger = get_run_logger()

    limiter.wait()
    resp = requests.get(url, params=params, timeout=timeout)

    if resp.status_code == 429:
        retry_after = float(resp.headers.get("Retry-After", 1.0))
        logger.warning(f"NCBI returned 429 (rate limited); backing off {retry_after}s")
        time.sleep(retry_after)
        limiter.wait()
        resp = requests.get(url, params=params, timeout=timeout)

    resp.raise_for_status()
    return resp


# --------------------------------------------------------------------------- #
# Query construction
# --------------------------------------------------------------------------- #
def build_query(
    phs_accession: Optional[str] = None,
    authors: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> str:
    """Build a PubMed (esearch) boolean query string from the given criteria."""
    parts = []

    if phs_accession:
        base_acc = phs_accession.split(".")[0]  # phs000424.v9.p2 -> phs000424
        if base_acc == phs_accession:
            parts.append(f'"{phs_accession}"[Text Word]')
        else:
            parts.append(f'("{phs_accession}"[Text Word] OR "{base_acc}"[Text Word])')

    if authors:
        # Deliberately unquoted: PubMed auto-truncates unquoted [Author] terms
        # to match varying initials turns truncation
        # OFF and requires an exact match with no initials at all, which will
        # almost never hit a real citation.
        author_terms = " OR ".join(f"{a}[Author]" for a in authors)
        parts.append(f"({author_terms})")

    if keywords:
        kw_terms = " OR ".join(f'"{k}"[Title/Abstract]' for k in keywords)
        parts.append(f"({kw_terms})")

    if not parts:
        raise ValueError(
            "At least one search criterion (phs_accession, authors, or keywords) "
            "must be provided."
        )

    query = " AND ".join(parts)

    if date_from or date_to:
        date_from = date_from or "1900/01/01"
        date_to = date_to or datetime.today().strftime("%Y/%m/%d")
        query += f' AND ("{date_from}"[Date - Publication] : "{date_to}"[Date - Publication])'

    return query


# --------------------------------------------------------------------------- #
# Batch query file loading
# --------------------------------------------------------------------------- #
def _split_multi(value: Optional[str]) -> Optional[List[str]]:
    """Split a semicolon-delimited cell (e.g. 'Jones;Smith J') into a list,
    trimming whitespace and dropping empty entries. Returns None if empty."""
    if not value:
        return None
    parts = [p.strip() for p in value.split(";") if p.strip()]
    return parts or None


@task
def load_queries_from_file(query_file: str) -> List[dict]:
    """
    Read a CSV/TSV file where each row defines one PubMed query.

    Delimiter is chosen from the file extension (.tsv -> tab, else comma).

    Expected columns (case-insensitive, all optional per row, but at least
    one of phs_accession/authors/keywords should be non-empty for a row to
    produce results):
        - label         optional; used to name that row's output file.
                        Defaults to "query_<row number>".
        - phs_accession a single dbGaP accession, e.g. phs000424
        - authors       one or more names, semicolon-separated,
                        e.g. "Jones;Smith J"
        - keywords      one or more keywords, semicolon-separated,
                        e.g. "childhood;cancer"
        - date_from     "YYYY/MM/DD"
        - date_to       "YYYY/MM/DD"
    """
    logger = get_run_logger()
    path = Path(query_file)
    delimiter = "\t" if path.suffix.lower() == ".tsv" else ","

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        reader.fieldnames = [(fn or "").strip().lower() for fn in (reader.fieldnames or [])]
        rows = list(reader)

    queries = []
    for i, row in enumerate(rows):
        queries.append(
            {
                "label": (row.get("label") or f"query_{i + 1}").strip(),
                "phs_accession": (row.get("phs_accession") or "").strip() or None,
                "authors": _split_multi(row.get("authors")),
                "keywords": _split_multi(row.get("keywords")),
                "date_from": (row.get("date_from") or "").strip() or None,
                "date_to": (row.get("date_to") or "").strip() or None,
            }
        )

    logger.info(f"Loaded {len(queries)} quer{'y' if len(queries) == 1 else 'ies'} from {path}")
    return queries


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
@task(retries=3, retry_delay_seconds=5)
def esearch(query: str, email: str, api_key: Optional[str] = None, page_size: int = 200) -> List[str]:
    """Run esearch, paginating through all results, and return a list of PMIDs."""
    logger = get_run_logger()
    pmids: List[str] = []
    retstart = 0

    while True:
        params = {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retstart": retstart,
            "retmax": page_size,
            "email": email,
        }
        if api_key:
            params["api_key"] = api_key

        resp = _ncbi_get(f"{NCBI_BASE}/esearch.fcgi", params, api_key, timeout=30)
        result = resp.json()["esearchresult"]

        if "ERROR" in result:
            raise RuntimeError(f"PubMed esearch error: {result['ERROR']}")

        ids = result.get("idlist", [])
        pmids.extend(ids)
        total = int(result.get("count", 0))
        logger.info(f"esearch: fetched {len(pmids)}/{total} PMIDs")

        retstart += page_size

        if retstart >= total or not ids:
            break

    return pmids


@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=6))
def efetch_details(
    pmids: List[str], email: str, api_key: Optional[str] = None, batch_size: int = 200
) -> List[dict]:
    """Fetch full records for a list of PMIDs and extract DOI + metadata."""
    logger = get_run_logger()
    records: List[dict] = []

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i : i + batch_size]
        params = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "xml",
            "email": email,
        }
        if api_key:
            params["api_key"] = api_key

        resp = _ncbi_get(f"{NCBI_BASE}/efetch.fcgi", params, api_key, timeout=60)
        root = ET.fromstring(resp.content)

        for article in root.findall(".//PubmedArticle"):
            pmid = article.findtext(".//PMID")
            title = (article.findtext(".//ArticleTitle") or "").strip()

            doi = None
            for id_elem in article.findall(".//ArticleId"):
                if id_elem.get("IdType") == "doi":
                    doi = id_elem.text
                    break

            journal = article.findtext(".//Journal/Title") or ""
            year = (
                article.findtext(".//JournalIssue/PubDate/Year")
                or article.findtext(".//JournalIssue/PubDate/MedlineDate")
                or ""
            )

            author_names = []
            for author in article.findall(".//AuthorList/Author"):
                last = author.findtext("LastName")
                fore = author.findtext("ForeName")
                if last:
                    author_names.append(f"{fore} {last}".strip() if fore else last)

            records.append(
                {
                    "pmid": pmid,
                    "doi": doi,
                    "title": title,
                    "journal": journal,
                    "year": year,
                    "authors": "; ".join(author_names),
                }
            )

        logger.info(f"efetch: processed {min(i + batch_size, len(pmids))}/{len(pmids)} records")

    return records


@task
def save_results(records: List[dict], output_path: str) -> None:
    logger = get_run_logger()
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["pmid", "doi", "title", "journal", "year", "authors"]
    if path.suffix.lower() == ".json":
        path.write_text(json.dumps(records, indent=2))
    else:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

    logger.info(f"Saved {len(records)} record(s) to {path.resolve()}")


@task
def write_manifest(summary_rows: List[dict], output_dir: Path) -> Path:
    manifest_path = output_dir / "manifest.csv"
    fieldnames = ["label", "query", "n_pmids", "n_missing_doi", "output_file", "error"]
    with open(manifest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)
    return manifest_path


# --------------------------------------------------------------------------- #
# Per-query processing (plain helper, not a task -- it orchestrates tasks)
# --------------------------------------------------------------------------- #
def _sanitize_filename(label: str) -> str:
    return "".join(c if (c.isalnum() or c in ("-", "_")) else "_" for c in label) or "query"


def _process_query(query_spec: dict, email: str, api_key: Optional[str], output_dir: Path) -> dict:
    logger = get_run_logger()
    label = query_spec.get("label") or "query"

    try:
        query = build_query(
            query_spec.get("phs_accession"),
            query_spec.get("authors"),
            query_spec.get("keywords"),
            query_spec.get("date_from"),
            query_spec.get("date_to"),
        )
    except ValueError as e:
        logger.warning(f"[{label}] skipped: {e}")
        return {
            "label": label,
            "query": None,
            "n_pmids": 0,
            "n_missing_doi": 0,
            "output_file": None,
            "error": str(e),
        }

    logger.info(f"[{label}] query: {query}")
    output_file = output_dir / f"{_sanitize_filename(label)}.csv"

    pmids = esearch(query, email, api_key)
    logger.info(f"[{label}] found {len(pmids)} matching PMIDs")

    if not pmids:
        save_results([], str(output_file))
        return {
            "label": label,
            "query": query,
            "n_pmids": 0,
            "n_missing_doi": 0,
            "output_file": output_file.name,
            "error": None,
        }

    records = efetch_details(pmids, email, api_key)
    missing_doi = sum(1 for r in records if not r["doi"])
    if missing_doi:
        logger.warning(f"[{label}] {missing_doi} of {len(records)} records have no DOI in PubMed")

    save_results(records, str(output_file))

    return {
        "label": label,
        "query": query,
        "n_pmids": len(pmids),
        "n_missing_doi": missing_doi,
        "output_file": output_file.name,
        "error": None,
    }


# --------------------------------------------------------------------------- #
# Flow
# --------------------------------------------------------------------------- #
@flow(name="pubmed-doi-extraction")
def pubmed_doi_flow(
    bucket: str,
    runner: str,
    query_file: Optional[str] = None,  # local path to a CSV/TSV of queries; see load_queries_from_file
    phs_accession: Optional[str] = None,
    authors: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
    date_from: Optional[str] = None,  # "YYYY/MM/DD"
    date_to: Optional[str] = None,  # "YYYY/MM/DD"
    email: Optional[str] = None,
    api_key: Optional[str] = None,  # NCBI API key for increased rate limits, kept optional,
    # as we don't want to feed this into Prefect at this time,
    # we could set it up as a variable in Prefect later if need be.
) -> List[dict]:
    """
    Search PubMed for one or more queries and save DOIs + metadata.

    Two modes:
        - Batch: pass `query_file` (a local CSV/TSV path). Each row becomes one
            query. Individual phs_accession/authors/keywords/date_from/date_to
            arguments are ignored (a warning is logged if both are given).
        - Single: leave `query_file` as None and pass phs_accession/authors/
            keywords/date_from/date_to directly.

    Either way, results are written into one timestamped output directory
    (one CSV per query + a manifest.csv), and that whole directory is
    uploaded -- never a single loose file -- via `dir_ul`.

    Returns a list of per-query summary dicts (label, query string, hit
    count, missing-DOI count, output filename, error). The actual PMID/DOI
    records live in the per-query CSV files, not in this return value.
    """
    logger = get_run_logger()

    time_str = get_time()
    output_dir = Path(f"pubmed_results_{time_str}")
    output_dir.mkdir(parents=True, exist_ok=True)

    if query_file:
        if phs_accession or authors or keywords or date_from or date_to:
            logger.warning(
                "query_file was provided; ignoring individual phs_accession/authors/"
                "keywords/date_from/date_to arguments."
            )
        file_dl(bucket, query_file)
        query_file = Path(query_file).name
        query_specs = load_queries_from_file(query_file)
    else:
        query_specs = [
            {
                "label": "query_1",
                "phs_accession": phs_accession,
                "authors": authors,
                "keywords": keywords,
                "date_from": date_from,
                "date_to": date_to,
            }
        ]

    summary_rows = [
        _process_query(spec, email, api_key, output_dir) for spec in query_specs
    ]

    manifest_path = write_manifest(summary_rows, output_dir)
    logger.info(f"Wrote manifest for {len(summary_rows)} quer{'y' if len(summary_rows) == 1 else 'ies'} to {manifest_path}")

    # Upload the whole output directory -- always a directory, even for a
    # single query -- so downstream handling never has to special-case this.
    folder_ul(
            local_folder=str(output_dir),
            bucket=bucket,
            destination=runner,
            sub_folder="",
        )
    
    return summary_rows


if __name__ == "__main__":
    # Example 1: single query (same as before)
    pubmed_doi_flow(
        bucket="my-bucket",
        runner="my-runner",
        phs_accession="phs000424",
        keywords=["eQTL"],
        email="you@example.com",
        api_key=None,
    )

    # Example 2: batch mode from a CSV/TSV of queries
    # pubmed_doi_flow(
    #     bucket="my-bucket",
    #     runner="my-runner",
    #     query_file="/path/to/queries.csv",
    #     email="you@example.com",
    #     api_key=None,
    # )