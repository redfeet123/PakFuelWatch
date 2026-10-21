"""Pakistan Bureau of Statistics (PBS) data ingestion and parsing module.

Fetches and processes:
1. SPI (Sensitive Price Indicator, weekly) -> spi_weekly Silver table.
2. CPI (Consumer Price Index, monthly) -> cpi_monthly Silver table.

Uses Patchright browser automation, openpyxl for Excel, and pdfplumber for PDF.
Resilient parsing: logs parse failures with file name and page/sheet, quarantines failing files.
"""

import io
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import openpyxl
import pandas as pd
import pdfplumber
import pyarrow as pa

from ingestion.config import get_settings
from ingestion.core.browser import (
    download_file,
    get_selector,
    launch_browser_context,
    polite_goto,
    save_page,
)
from ingestion.core.delta_io import write_delta_merge
from ingestion.core.schemas import CPI_MONTHLY_SCHEMA, SPI_WEEKLY_SCHEMA
from ingestion.core.storage import StorageClient, get_storage_client
from ingestion.silver.food_prices import normalize_unit
from ingestion.silver.utils import (
    TransformationResult,
    get_latest_bronze_file,
    get_silver_table_uri,
    write_quarantine,
    write_rejects,
)

logger = logging.getLogger("pakfuelwatch.sources.pbs")


# =====================================================================
# Ingestion & Discovery (Bronze)
# =====================================================================


def discover_pbs_release_links(
    page: Any,
    section: str = "spi",
) -> List[Dict[str, str]]:
    """Scan a PBS page for downloadable release links (PDF or Excel).

    Args:
        page: Patchright Page instance.
        section: Section identifier ('spi' or 'cpi').

    Returns:
        List of dicts with 'title', 'url', and 'file_type'.
    """
    selector = get_selector("pbs", section, "release_links")
    links: List[Dict[str, str]] = []

    elements = page.locator(selector).all()
    for el in elements:
        href = el.get_attribute("href")
        if not href:
            continue

        abs_url = urljoin(page.url, href)
        text = (el.inner_text() or "").strip()
        lower_href = href.lower()

        if lower_href.endswith(".pdf"):
            ftype = "pdf"
        elif lower_href.endswith(".xlsx"):
            ftype = "xlsx"
        elif lower_href.endswith(".xls"):
            ftype = "xls"
        else:
            continue

        links.append(
            {
                "title": text,
                "url": abs_url,
                "file_type": ftype,
            }
        )

    logger.info("Discovered %d release links for PBS section '%s'", len(links), section)
    return links


def fetch_pbs_spi_latest(
    browser_context: Optional[Any] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Find and download the latest weekly SPI release from PBS portal into Bronze.

    Args:
        browser_context: Optional Patchright BrowserContext. If None, launches one.
        storage_client: StorageClient instance.

    Returns:
        Tuple of (s3_uri, was_written).
    """
    settings = get_settings()
    storage = storage_client or get_storage_client()
    target_url = get_selector("pbs", "spi", "page_url", default=f"{settings.pbs_base_url}/cpi")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _execute(ctx: Any) -> Tuple[str, bool]:
        page = ctx.new_page()
        try:
            polite_goto(page, target_url)
            # Capture HTML snapshot and screenshot
            save_page("pbs_spi", page, storage_client=storage)

            links = discover_pbs_release_links(page, section="spi")
            if not links:
                logger.warning("No SPI release links found on page: %s", target_url)
                return "", False

            latest_link = links[0]
            download_url = latest_link["url"]
            filename = f"pbs_spi_latest_{today_str}.{latest_link['file_type']}"
            return download_file(
                page=page,
                selector_or_url=download_url,
                source="pbs_spi",
                storage_client=storage,
                filename=filename,
            )
        finally:
            page.close()

    if browser_context:
        return _execute(browser_context)

    with launch_browser_context() as ctx:
        return _execute(ctx)


def fetch_pbs_cpi_latest(
    browser_context: Optional[Any] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Find and download the latest monthly CPI release from PBS portal into Bronze.

    Args:
        browser_context: Optional Patchright BrowserContext.
        storage_client: StorageClient instance.

    Returns:
        Tuple of (s3_uri, was_written).
    """
    settings = get_settings()
    storage = storage_client or get_storage_client()
    target_url = get_selector("pbs", "cpi", "page_url", default=f"{settings.pbs_base_url}/cpi")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _execute(ctx: Any) -> Tuple[str, bool]:
        page = ctx.new_page()
        try:
            polite_goto(page, target_url)
            save_page("pbs_cpi", page, storage_client=storage)

            links = discover_pbs_release_links(page, section="cpi")
            if not links:
                logger.warning("No CPI release links found on page: %s", target_url)
                return "", False

            latest_link = links[0]
            download_url = latest_link["url"]
            filename = f"pbs_cpi_latest_{today_str}.{latest_link['file_type']}"
            return download_file(
                page=page,
                selector_or_url=download_url,
                source="pbs_cpi",
                storage_client=storage,
                filename=filename,
            )
        finally:
            page.close()

    if browser_context:
        return _execute(browser_context)

    with launch_browser_context() as ctx:
        return _execute(ctx)


def fetch_pbs_backfill(
    months: int = 12,
    sections: Optional[List[str]] = None,
    browser_context: Optional[Any] = None,
    storage_client: Optional[StorageClient] = None,
) -> List[Tuple[str, bool]]:
    """Walk back historical release archives for at least the specified months.

    Args:
        months: Number of months to walk back (minimum 12).
        sections: Sections to backfill (defaults to ['spi', 'cpi']).
        browser_context: Optional BrowserContext.
        storage_client: StorageClient instance.

    Returns:
        List of (s3_uri, was_written) tuples for downloaded historical releases.
    """
    target_sections = sections or ["spi", "cpi"]
    storage = storage_client or get_storage_client()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _execute(ctx: Any) -> List[Tuple[str, bool]]:
        res: List[Tuple[str, bool]] = []
        for sec in target_sections:
            page = ctx.new_page()
            try:
                page_url = get_selector("pbs", sec, "page_url")
                polite_goto(page, page_url)
                save_page(f"pbs_{sec}_backfill", page, storage_client=storage)

                links = discover_pbs_release_links(page, section=sec)
                # Take up to the requested months of releases (or 4 * months for weekly SPI)
                limit = (months * 4) if sec == "spi" else months
                selected_links = links[:limit]

                for idx, item in enumerate(selected_links, start=1):
                    fn = f"pbs_{sec}_backfill_{idx}_{today_str}.{item['file_type']}"
                    try:
                        download_res = download_file(
                            page=page,
                            selector_or_url=item["url"],
                            source=f"pbs_{sec}",
                            storage_client=storage,
                            filename=fn,
                        )
                        res.append(download_res)
                    except Exception as exc:
                        logger.warning("Failed backfill download for %s: %s", item["url"], exc)
            finally:
                page.close()
        return res

    if browser_context:
        return _execute(browser_context)

    with launch_browser_context() as ctx:
        return _execute(ctx)


# =====================================================================
# Parsing & Silver Transformations
# =====================================================================


def parse_spi_excel(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse a PBS SPI Excel file into an uncleaned DataFrame.

    Logs parse failures with filename and sheet name if a sheet fails.

    Args:
        file_bytes: Excel binary content.
        filename: Name of the file being parsed.

    Returns:
        DataFrame containing parsed observation rows.
    """
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    records: List[Dict[str, Any]] = []

    for sheet_name in wb.sheetnames:
        sheet = wb[sheet_name]
        try:
            rows = list(sheet.iter_rows(values_only=True))
            if not rows:
                continue

            # Identify header row: search for 'item' or 'commodity' or 'description'
            header_idx = -1
            item_col = unit_col = price_col = date_col = -1

            for r_idx, row in enumerate(rows[:20]):
                row_strs = [str(c).strip().lower() if c is not None else "" for c in row]
                for c_idx, cell in enumerate(row_strs):
                    if any(k in cell for k in ("item", "commodity", "description")):
                        item_col = c_idx
                    elif any(k in cell for k in ("unit", "quantity")):
                        unit_col = c_idx
                    elif any(k in cell for k in ("price", "avg", "current week")):
                        price_col = c_idx
                    elif any(k in cell for k in ("date", "week ending", "week end")):
                        date_col = c_idx

                if item_col != -1 and price_col != -1:
                    header_idx = r_idx
                    break

            if header_idx == -1:
                logger.debug(
                    "No recognized SPI table header in sheet '%s' of %s",
                    sheet_name,
                    filename,
                )
                continue

            for row in rows[header_idx + 1 :]:
                if not row or len(row) <= max(item_col, price_col):
                    continue

                raw_item = row[item_col]
                raw_price = row[price_col]
                raw_unit = row[unit_col] if unit_col != -1 and len(row) > unit_col else None
                raw_date = row[date_col] if date_col != -1 and len(row) > date_col else None

                if raw_item is None or raw_price is None:
                    continue

                records.append(
                    {
                        "raw_week_end": raw_date,
                        "raw_item": raw_item,
                        "raw_unit": raw_unit,
                        "raw_price": raw_price,
                        "sheet_name": sheet_name,
                    }
                )
        except Exception as exc:
            logger.error(
                "Failed to parse PBS SPI Excel file '%s' (sheet: '%s'): %s",
                filename,
                sheet_name,
                exc,
            )
            raise

    return pd.DataFrame(records)


def parse_spi_pdf(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse a PBS SPI PDF file into an uncleaned DataFrame using pdfplumber.

    Logs parse failures with filename and page number if an error occurs.

    Args:
        file_bytes: PDF binary content.
        filename: Name of the file being parsed.

    Returns:
        DataFrame containing parsed table rows.
    """
    records: List[Dict[str, Any]] = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            try:
                tables = page.extract_tables()
                if not tables:
                    continue

                for table in tables:
                    if not table or len(table) < 2:
                        continue

                    # Header search
                    item_col = unit_col = price_col = date_col = -1
                    header_row_idx = -1

                    for r_idx, row in enumerate(table[:5]):
                        row_strs = [
                            str(c).strip().lower().replace("\n", " ") if c is not None else ""
                            for c in row
                        ]
                        for c_idx, cell in enumerate(row_strs):
                            if any(k in cell for k in ("item", "commodity", "description")):
                                item_col = c_idx
                            elif any(k in cell for k in ("unit", "quantity")):
                                unit_col = c_idx
                            elif any(k in cell for k in ("price", "avg", "current")):
                                price_col = c_idx
                            elif any(k in cell for k in ("date", "week")):
                                date_col = c_idx

                        if item_col != -1 and price_col != -1:
                            header_row_idx = r_idx
                            break

                    if header_row_idx == -1:
                        continue

                    for row in table[header_row_idx + 1 :]:
                        if not row or len(row) <= max(item_col, price_col):
                            continue

                        raw_item = row[item_col]
                        raw_price = row[price_col]
                        raw_unit = row[unit_col] if unit_col != -1 and len(row) > unit_col else None
                        raw_date = row[date_col] if date_col != -1 and len(row) > date_col else None

                        if not raw_item or not raw_price:
                            continue

                        records.append(
                            {
                                "raw_week_end": raw_date,
                                "raw_item": str(raw_item).replace("\n", " ").strip(),
                                "raw_unit": (
                                    str(raw_unit).replace("\n", " ").strip() if raw_unit else None
                                ),
                                "raw_price": str(raw_price).replace("\n", " ").strip(),
                                "page_num": page_idx,
                            }
                        )
            except Exception as exc:
                logger.error(
                    "Failed to parse PBS SPI PDF file '%s' on page %d: %s",
                    filename,
                    page_idx,
                    exc,
                )
                raise

    return pd.DataFrame(records)


def parse_cpi_excel(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse a PBS CPI Excel release file into an uncleaned DataFrame."""
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    records: List[Dict[str, Any]] = []

    for sheet_name in wb.sheetnames:
        sheet = wb[sheet_name]
        try:
            rows = list(sheet.iter_rows(values_only=True))
            if not rows:
                continue

            cat_col = idx_col = yoy_col = month_col = -1
            header_idx = -1

            for r_idx, row in enumerate(rows[:20]):
                row_strs = [str(c).strip().lower() if c is not None else "" for c in row]
                for c_idx, cell in enumerate(row_strs):
                    if any(k in cell for k in ("group", "category", "commodity", "item")):
                        cat_col = c_idx
                    elif any(k in cell for k in ("index", "general")):
                        idx_col = c_idx
                    elif any(k in cell for k in ("yoy", "inflation", "change %", "annual")):
                        yoy_col = c_idx
                    elif any(k in cell for k in ("month", "period", "date")):
                        month_col = c_idx

                if cat_col != -1 and idx_col != -1:
                    header_idx = r_idx
                    break

            if header_idx == -1:
                continue

            for row in rows[header_idx + 1 :]:
                if not row or len(row) <= max(cat_col, idx_col):
                    continue

                raw_cat = row[cat_col]
                raw_idx = row[idx_col]
                raw_yoy = row[yoy_col] if yoy_col != -1 and len(row) > yoy_col else None
                raw_m = row[month_col] if month_col != -1 and len(row) > month_col else None

                if raw_cat is None or raw_idx is None:
                    continue

                records.append(
                    {
                        "raw_month": raw_m,
                        "category": str(raw_cat).strip(),
                        "raw_index_value": raw_idx,
                        "raw_yoy_pct": raw_yoy,
                        "sheet_name": sheet_name,
                    }
                )
        except Exception as exc:
            logger.error(
                "Failed to parse PBS CPI Excel file '%s' (sheet '%s'): %s",
                filename,
                sheet_name,
                exc,
            )
            raise

    return pd.DataFrame(records)


def parse_cpi_pdf(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse a PBS CPI PDF release file into an uncleaned DataFrame using pdfplumber."""
    records: List[Dict[str, Any]] = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            try:
                tables = page.extract_tables()
                if not tables:
                    continue

                for table in tables:
                    if not table or len(table) < 2:
                        continue

                    cat_col = idx_col = yoy_col = month_col = -1
                    header_row_idx = -1

                    for r_idx, row in enumerate(table[:5]):
                        row_strs = [
                            str(c).strip().lower().replace("\n", " ") if c is not None else ""
                            for c in row
                        ]
                        for c_idx, cell in enumerate(row_strs):
                            if any(k in cell for k in ("group", "category", "item")):
                                cat_col = c_idx
                            elif any(k in cell for k in ("index", "general")):
                                idx_col = c_idx
                            elif any(k in cell for k in ("yoy", "inflation", "change")):
                                yoy_col = c_idx
                            elif any(k in cell for k in ("month", "period", "date")):
                                month_col = c_idx

                        if cat_col != -1 and idx_col != -1:
                            header_row_idx = r_idx
                            break

                    if header_row_idx == -1:
                        continue

                    for row in table[header_row_idx + 1 :]:
                        if not row or len(row) <= max(cat_col, idx_col):
                            continue

                        raw_cat = row[cat_col]
                        raw_idx = row[idx_col]
                        raw_yoy = row[yoy_col] if yoy_col != -1 and len(row) > yoy_col else None
                        raw_m = row[month_col] if month_col != -1 and len(row) > month_col else None

                        if not raw_cat or not raw_idx:
                            continue

                        records.append(
                            {
                                "raw_month": raw_m,
                                "category": str(raw_cat).replace("\n", " ").strip(),
                                "raw_index_value": str(raw_idx).replace("\n", " ").strip(),
                                "raw_yoy_pct": (
                                    str(raw_yoy).replace("\n", " ").strip() if raw_yoy else None
                                ),
                                "page_num": page_idx,
                            }
                        )
            except Exception as exc:
                logger.error(
                    "Failed to parse PBS CPI PDF file '%s' on page %d: %s",
                    filename,
                    page_idx,
                    exc,
                )
                raise

    return pd.DataFrame(records)


def transform_pbs_spi(
    file_bytes: Optional[bytes] = None,
    filename: Optional[str] = None,
    silver_base_uri: Optional[str] = None,
    rejects_base_uri: Optional[str] = None,
    quarantine_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Transform PBS SPI release into Silver spi_weekly table.

    Quarantines failing files without crashing if parse failure occurs.

    Args:
        file_bytes: Optional raw file bytes.
        filename: Optional file name.
        silver_base_uri: Target Silver base URI.
        rejects_base_uri: Rejects base URI.
        quarantine_base_uri: Quarantine base URI.
        storage_client: StorageClient instance.

    Returns:
        TransformationResult summary.
    """
    table_name = "spi_weekly"
    storage = storage_client or get_storage_client()

    # 1. Acquire raw data
    fname = filename or "pbs_spi_latest.xlsx"
    if file_bytes is None:
        s3_uri, file_bytes, meta = get_latest_bronze_file("pbs_spi", storage_client=storage)
        if file_bytes is None:
            logger.warning("[%s] No bronze SPI files found to transform.", table_name)
            return TransformationResult(
                table_name=table_name,
                read_count=0,
                rejected_count=0,
                written_count=0,
                table_uri=get_silver_table_uri(table_name, silver_base_uri),
            )
        fname = s3_uri.split("/")[-1] if s3_uri else fname

    # 2. Parse file with quarantine protection
    raw_df = pd.DataFrame()
    try:
        lower_name = fname.lower()
        if lower_name.endswith(".pdf"):
            raw_df = parse_spi_pdf(file_bytes, fname)
        else:
            raw_df = parse_spi_excel(file_bytes, fname)
    except Exception as exc:
        logger.error("Parse failure on file '%s': %s. Quarantining file...", fname, exc)
        write_quarantine(
            content=file_bytes,
            filename=fname,
            source="pbs_spi",
            error_message=str(exc),
            base_quarantine_uri=quarantine_base_uri,
            storage_client=storage,
        )
        return TransformationResult(
            table_name=table_name,
            read_count=0,
            rejected_count=0,
            written_count=0,
            table_uri=get_silver_table_uri(table_name, silver_base_uri),
        )

    read_count = len(raw_df)
    if read_count == 0:
        logger.info("[%s] 0 rows parsed from %s.", table_name, fname)
        return TransformationResult(
            table_name=table_name,
            read_count=0,
            rejected_count=0,
            written_count=0,
            table_uri=get_silver_table_uri(table_name, silver_base_uri),
        )

    # 3. Clean and validate rows
    valid_rows: List[Dict[str, Any]] = []
    reject_rows: List[Dict[str, Any]] = []
    fallback_date = datetime.now(timezone.utc).date()

    for _, row in raw_df.iterrows():
        raw_d = row.get("raw_week_end")
        raw_item = row.get("raw_item")
        raw_p = row.get("raw_price")
        raw_u = row.get("raw_unit")

        # Validate item
        if pd.isna(raw_item) or not str(raw_item).strip():
            reject_rows.append({**row.to_dict(), "reason": "missing_item"})
            continue
        clean_item = " ".join(str(raw_item).strip().split()).title()

        # Validate unit
        clean_unit = normalize_unit(raw_u) if raw_u else "Piece"
        if not clean_unit:
            reject_rows.append({**row.to_dict(), "reason": "unit_mismatch"})
            continue

        # Validate price
        if pd.isna(raw_p) or str(raw_p).strip() in (".", "", "None", "null", "nan"):
            reject_rows.append({**row.to_dict(), "reason": "missing_price"})
            continue

        try:
            # Strip commas or currency symbols
            p_str = re.sub(r"[^\d.]", "", str(raw_p))
            price_val = float(p_str)
        except (ValueError, TypeError):
            reject_rows.append({**row.to_dict(), "reason": "non_numeric_price"})
            continue

        if price_val <= 0:
            reject_rows.append({**row.to_dict(), "reason": "non_positive_price"})
            continue

        # Validate week_end date
        parsed_date = None
        if raw_d and not pd.isna(raw_d):
            d_str = str(raw_d).strip()
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    parsed_date = datetime.strptime(d_str, fmt).date()
                    break
                except ValueError:
                    continue

        parsed_date = parsed_date or fallback_date

        valid_rows.append(
            {
                "week_end": parsed_date,
                "item": clean_item,
                "price_pkr": price_val,
                "unit": clean_unit,
            }
        )

    # 4. Deduplicate valid rows on natural key (week_end, item, unit)
    clean_df = pd.DataFrame(valid_rows)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(subset=["week_end", "item", "unit"], keep="last")

    rejects_df = pd.DataFrame(reject_rows)
    rejected_count = len(rejects_df)
    written_count = len(clean_df)

    rejects_uri = write_rejects(
        rejects_df=rejects_df,
        table_name=table_name,
        base_rejects_uri=rejects_base_uri,
    )

    table_uri = get_silver_table_uri(table_name, silver_base_uri)
    if written_count > 0:
        pa_table = pa.Table.from_pandas(clean_df, schema=SPI_WEEKLY_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["week_end", "item", "unit"],
        )

    logger.info(
        "[%s] Transformation complete: %d read, %d rejected, %d written to %s",
        table_name,
        read_count,
        rejected_count,
        written_count,
        table_uri,
    )

    return TransformationResult(
        table_name=table_name,
        read_count=read_count,
        rejected_count=rejected_count,
        written_count=written_count,
        table_uri=table_uri,
        rejects_uri=rejects_uri,
    )


def transform_pbs_cpi(
    file_bytes: Optional[bytes] = None,
    filename: Optional[str] = None,
    silver_base_uri: Optional[str] = None,
    rejects_base_uri: Optional[str] = None,
    quarantine_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Transform PBS CPI release into Silver cpi_monthly table.

    Quarantines failing files without crashing if parse failure occurs.

    Args:
        file_bytes: Optional raw file bytes.
        filename: Optional file name.
        silver_base_uri: Target Silver base URI.
        rejects_base_uri: Rejects base URI.
        quarantine_base_uri: Quarantine base URI.
        storage_client: StorageClient instance.

    Returns:
        TransformationResult summary.
    """
    table_name = "cpi_monthly"
    storage = storage_client or get_storage_client()

    fname = filename or "pbs_cpi_latest.xlsx"
    if file_bytes is None:
        s3_uri, file_bytes, meta = get_latest_bronze_file("pbs_cpi", storage_client=storage)
        if file_bytes is None:
            logger.warning("[%s] No bronze CPI files found to transform.", table_name)
            return TransformationResult(
                table_name=table_name,
                read_count=0,
                rejected_count=0,
                written_count=0,
                table_uri=get_silver_table_uri(table_name, silver_base_uri),
            )
        fname = s3_uri.split("/")[-1] if s3_uri else fname

    raw_df = pd.DataFrame()
    try:
        lower_name = fname.lower()
        if lower_name.endswith(".pdf"):
            raw_df = parse_cpi_pdf(file_bytes, fname)
        else:
            raw_df = parse_cpi_excel(file_bytes, fname)
    except Exception as exc:
        logger.error("Parse failure on file '%s': %s. Quarantining file...", fname, exc)
        write_quarantine(
            content=file_bytes,
            filename=fname,
            source="pbs_cpi",
            error_message=str(exc),
            base_quarantine_uri=quarantine_base_uri,
            storage_client=storage,
        )
        return TransformationResult(
            table_name=table_name,
            read_count=0,
            rejected_count=0,
            written_count=0,
            table_uri=get_silver_table_uri(table_name, silver_base_uri),
        )

    read_count = len(raw_df)
    if read_count == 0:
        logger.info("[%s] 0 rows parsed from %s.", table_name, fname)
        return TransformationResult(
            table_name=table_name,
            read_count=0,
            rejected_count=0,
            written_count=0,
            table_uri=get_silver_table_uri(table_name, silver_base_uri),
        )

    valid_rows: List[Dict[str, Any]] = []
    reject_rows: List[Dict[str, Any]] = []
    default_month = datetime.now(timezone.utc).strftime("%Y-%m")

    for _, row in raw_df.iterrows():
        raw_m = row.get("raw_month")
        cat = row.get("category", "General")
        raw_idx = row.get("raw_index_value")
        raw_yoy = row.get("raw_yoy_pct")

        # Validate category
        if pd.isna(cat) or not str(cat).strip():
            reject_rows.append({**row.to_dict(), "reason": "missing_category"})
            continue
        clean_cat = " ".join(str(cat).strip().split()).title()

        # Validate index value
        if pd.isna(raw_idx) or str(raw_idx).strip() in (".", "", "None", "null", "nan"):
            reject_rows.append({**row.to_dict(), "reason": "missing_index_value"})
            continue

        try:
            cleaned_idx = re.sub(r"[^\d.]", "", str(raw_idx))
            index_val = float(cleaned_idx)
        except (ValueError, TypeError):
            reject_rows.append({**row.to_dict(), "reason": "non_numeric_index_value"})
            continue

        if index_val <= 0:
            reject_rows.append({**row.to_dict(), "reason": "non_positive_index_value"})
            continue

        # Validate YoY %
        yoy_val: Optional[float] = None
        if raw_yoy and not pd.isna(raw_yoy):
            try:
                cleaned_yoy = re.sub(r"[^\d.-]", "", str(raw_yoy))
                yoy_val = float(cleaned_yoy)
            except (ValueError, TypeError):
                reject_rows.append({**row.to_dict(), "reason": "non_numeric_yoy_pct"})
                continue

        # Month normalization
        clean_month = str(raw_m).strip() if raw_m and not pd.isna(raw_m) else default_month
        if len(clean_month) > 7:
            clean_month = clean_month[:7]

        valid_rows.append(
            {
                "month": clean_month,
                "category": clean_cat,
                "index_value": index_val,
                "yoy_pct": yoy_val,
            }
        )

    clean_df = pd.DataFrame(valid_rows)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(subset=["month", "category"], keep="last")

    rejects_df = pd.DataFrame(reject_rows)
    rejected_count = len(rejects_df)
    written_count = len(clean_df)

    rejects_uri = write_rejects(
        rejects_df=rejects_df,
        table_name=table_name,
        base_rejects_uri=rejects_base_uri,
    )

    table_uri = get_silver_table_uri(table_name, silver_base_uri)
    if written_count > 0:
        pa_table = pa.Table.from_pandas(clean_df, schema=CPI_MONTHLY_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["month", "category"],
        )

    logger.info(
        "[%s] Transformation complete: %d read, %d rejected, %d written to %s",
        table_name,
        read_count,
        rejected_count,
        written_count,
        table_uri,
    )

    return TransformationResult(
        table_name=table_name,
        read_count=read_count,
        rejected_count=rejected_count,
        written_count=written_count,
        table_uri=table_uri,
        rejects_uri=rejects_uri,
    )
