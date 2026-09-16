"""
Data quality rules and validation engine for ELT pipeline.
Enforces business rules, normalizes data, logs audit trails, and flags quarantine errors.
"""
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as date_parser

from config.settings import (
    MIN_VALID_YEAR,
    MAX_VALID_YEAR,
    YEMENI_PHONE_VALID_PREFIXES,
)

# Arabic and Persian numeral mapping to standard ASCII digits
ARABIC_EASTERN_DIGITS = "٠١٢٣٤٥٦٧٨٩"
PERSIAN_DIGITS = "۰۱۲۳۴۵۶۷۸۹"
DIGIT_TRANSLATION_TABLE = str.maketrans(
    {
        **{ord(char): str(idx) for idx, char in enumerate(ARABIC_EASTERN_DIGITS)},
        **{ord(char): str(idx) for idx, char in enumerate(PERSIAN_DIGITS)},
    }
)


def normalize_arabic_numerals(text: Any) -> str:
    """Converts Eastern Arabic and Persian numerals to Western ASCII digits."""
    if text is None:
        return ""
    s = str(text)
    return s.translate(DIGIT_TRANSLATION_TABLE)


def clean_phone_number(raw_phone: Any) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """
    Validates and normalizes Yemeni mobile phone numbers.
    Rules:
    - Normalize Arabic/Eastern numerals.
    - Remove spaces, dashes, parentheses, dots, and leading '+'.
    - Remove country codes (+967, 00967, 967) and trunk '0'.
    - Strictly 9 digits starting with (77, 78, 73, 71, 70).
    Returns:
        (cleaned_phone, error_code, correction_dict)
    """
    if raw_phone is None:
        return None, "INVALID_PHONE_NUMBER", None

    orig_str = str(raw_phone).strip()
    if not orig_str:
        return None, "INVALID_PHONE_NUMBER", None

    # 1. Normalize Eastern/Arabic digits
    normalized = normalize_arabic_numerals(orig_str)

    # 2. Strip noise (spaces, dashes, parens, dots, leading plus)
    cleaned = re.sub(r"[\s\-\(\)\.]", "", normalized)
    if cleaned.startswith("+"):
        cleaned = cleaned[1:]

    # 3. Strip international prefixes: 00967 or 967
    if cleaned.startswith("00967"):
        cleaned = cleaned[5:]
    elif cleaned.startswith("967"):
        cleaned = cleaned[3:]

    # 4. Strip national trunk prefix '0' if preceding a 9-digit mobile number
    if cleaned.startswith("0") and len(cleaned) == 10:
        cleaned = cleaned[1:]

    # 5. Check if strictly 9 digits and starts with a valid Yemeni mobile prefix
    if not (re.fullmatch(r"\d{9}", cleaned) and cleaned.startswith(YEMENI_PHONE_VALID_PREFIXES)):
        return None, "INVALID_PHONE_NUMBER", None

    # Check if a correction occurred
    correction = None
    if cleaned != orig_str:
        correction = {
            "field": "phone",
            "original_value": orig_str,
            "corrected_value": cleaned,
            "rule_code": "PHONE_NORMALIZED",
        }

    return cleaned, None, correction


def validate_customer_id(raw_customer_id: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Validates customer_id. Must not be null, empty, or whitespace.
    Returns:
        (customer_id, error_code)
    """
    if raw_customer_id is None:
        return None, "MISSING_CUSTOMER_ID"

    cid = str(raw_customer_id).strip()
    if not cid:
        return None, "MISSING_CUSTOMER_ID"

    return cid, None


def validate_items(raw_items: Any) -> Tuple[Optional[List[Any]], Optional[str]]:
    """
    Validates the items field. Must be valid JSON containing an array of at least 1 item.
    Returns:
        (parsed_items, error_code)
    """
    if raw_items is None:
        return None, "EMPTY_ITEMS"

    parsed = None
    if isinstance(raw_items, list):
        parsed = raw_items
    elif isinstance(raw_items, str):
        trimmed = raw_items.strip()
        if not trimmed:
            return None, "EMPTY_ITEMS"
        try:
            parsed = json.loads(trimmed)
        except Exception:
            return None, "CORRUPTED_ITEMS_JSON"
    else:
        return None, "CORRUPTED_ITEMS_JSON"

    if not isinstance(parsed, list):
        return None, "CORRUPTED_ITEMS_JSON"

    if len(parsed) < 1:
        return None, "EMPTY_ITEMS"

    return parsed, None


def validate_and_parse_order_time(raw_time: Any) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """
    Parses time/order_date into ISO 8601 string (YYYY-MM-DDTHH:MM:SSZ).
    Enforces boundary: 2000 <= year <= 2030.
    Returns:
        (iso_str, error_code, correction_dict)
    """
    if raw_time is None:
        return None, "INVALID_IMPOSSIBLE_DATE", None

    orig_str = str(raw_time).strip()
    if not orig_str:
        return None, "INVALID_IMPOSSIBLE_DATE", None

    # Normalize Arabic digits first in case dates contain Arabic numbers
    normalized_time_str = normalize_arabic_numerals(orig_str)

    try:
        dt = date_parser.parse(normalized_time_str)
    except Exception:
        return None, "INVALID_IMPOSSIBLE_DATE", None

    if dt.year < MIN_VALID_YEAR or dt.year > MAX_VALID_YEAR:
        return None, "INVALID_IMPOSSIBLE_DATE", None

    # Convert to UTC ISO 8601 representation
    if dt.tzinfo is not None:
        dt_utc = dt.astimezone(timezone.utc)
    else:
        dt_utc = dt.replace(tzinfo=timezone.utc)

    iso_str = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    correction = None
    if orig_str != iso_str:
        correction = {
            "field": "order_date",
            "original_value": orig_str,
            "corrected_value": iso_str,
            "rule_code": "DATE_STANDARDIZED",
        }

    return iso_str, None, correction


def clean_total_amount(raw_amount: Any) -> Tuple[Optional[float], Optional[str], Optional[Dict[str, Any]]]:
    """
    Cleans total amount / price:
    - Normalizes Arabic digits.
    - Removes currency symbols/strings (YER, ريال, YR, commas, spaces).
    - Converts to float.
    - Checks for negative values (AMBIGUOUS_NEGATIVE_VALUE).
    Returns:
        (cleaned_amount, error_code, correction_dict)
    """
    if raw_amount is None:
        return None, "INVALID_PRICE_FORMAT", None

    orig_str = str(raw_amount).strip()
    if not orig_str:
        return None, "INVALID_PRICE_FORMAT", None

    # 1. Normalize Eastern/Arabic digits
    cleaned = normalize_arabic_numerals(orig_str)

    # 2. Remove currency keywords and symbols
    currency_patterns = [r"YER", r"ريال", r"YR", r"﷼", r"YEM"]
    for pattern in currency_patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

    # 3. Remove commas and spaces
    cleaned = cleaned.replace(",", "").strip()

    # 4. Check negative
    is_negative = False
    if cleaned.startswith("-") or cleaned.endswith("-"):
        is_negative = True

    try:
        val = float(cleaned)
    except Exception:
        return None, "INVALID_PRICE_FORMAT", None

    if val < 0 or is_negative:
        return None, "AMBIGUOUS_NEGATIVE_VALUE", None

    val = round(val, 2)

    correction = None
    # If the original string had currency or Arabic digits or formatting differences
    if orig_str != str(val):
        correction = {
            "field": "total_amount",
            "original_value": orig_str,
            "corrected_value": val,
            "rule_code": "PRICE_NORMALIZED",
        }

    return val, None, correction


def validate_record(raw_record: Dict[str, Any], run_id: str) -> Tuple[bool, Dict[str, Any], int, Optional[str], Optional[str]]:
    """
    Applies all data quality rules to a raw record.
    Returns:
        (is_valid, processed_document, corrections_count, error_code, error_details)
    """
    corrections: List[Dict[str, Any]] = []

    # 1. Order ID validation
    order_id = raw_record.get("order_id")
    if order_id is None or not str(order_id).strip():
        quarantine_doc = {
            "run_id": run_id,
            "order_id": str(order_id) if order_id is not None else None,
            "error_code": "MISSING_ORDER_ID",
            "error_details": "Record lacks a valid order_id business key",
            "raw_record": raw_record,
        }
        return False, quarantine_doc, 0, "MISSING_ORDER_ID", quarantine_doc["error_details"]

    clean_order_id = str(order_id).strip()

    # 2. Customer ID validation
    raw_customer_id = raw_record.get("customer_id")
    customer_id, cust_err = validate_customer_id(raw_customer_id)
    if cust_err:
        quarantine_doc = {
            "run_id": run_id,
            "order_id": clean_order_id,
            "error_code": cust_err,
            "error_details": f"Invalid customer_id value: {raw_customer_id!r}",
            "raw_record": raw_record,
        }
        return False, quarantine_doc, 0, cust_err, quarantine_doc["error_details"]

    # 3. Phone Number validation
    raw_phone = raw_record.get("phone") or raw_record.get("phone_number")
    cleaned_phone, phone_err, phone_corr = clean_phone_number(raw_phone)
    if phone_err:
        quarantine_doc = {
            "run_id": run_id,
            "order_id": clean_order_id,
            "error_code": phone_err,
            "error_details": f"Phone number fails Yemeni format validation: {raw_phone!r}",
            "raw_record": raw_record,
        }
        return False, quarantine_doc, 0, phone_err, quarantine_doc["error_details"]
    if phone_corr:
        corrections.append(phone_corr)

    # 4. Items validation
    raw_items = raw_record.get("items")
    parsed_items, items_err = validate_items(raw_items)
    if items_err:
        quarantine_doc = {
            "run_id": run_id,
            "order_id": clean_order_id,
            "error_code": items_err,
            "error_details": f"Items validation failed with {items_err}: {raw_items!r}",
            "raw_record": raw_record,
        }
        return False, quarantine_doc, 0, items_err, quarantine_doc["error_details"]

    # 5. Date / Time validation
    raw_time = raw_record.get("order_date") or raw_record.get("time") or raw_record.get("order_time")
    iso_time, time_err, time_corr = validate_and_parse_order_time(raw_time)
    if time_err:
        quarantine_doc = {
            "run_id": run_id,
            "order_id": clean_order_id,
            "error_code": time_err,
            "error_details": f"Date parsing or year boundary check (2000-2030) failed: {raw_time!r}",
            "raw_record": raw_record,
        }
        return False, quarantine_doc, 0, time_err, quarantine_doc["error_details"]
    if time_corr:
        corrections.append(time_corr)

    # 6. Price / Total Amount validation
    raw_amount = (
        raw_record.get("total_amount")
        if raw_record.get("total_amount") is not None
        else (raw_record.get("price") if raw_record.get("price") is not None else raw_record.get("amount"))
    )
    cleaned_amount, amount_err, amount_corr = clean_total_amount(raw_amount)
    if amount_err:
        quarantine_doc = {
            "run_id": run_id,
            "order_id": clean_order_id,
            "error_code": amount_err,
            "error_details": f"Amount validation failed ({amount_err}): {raw_amount!r}",
            "raw_record": raw_record,
        }
        return False, quarantine_doc, 0, amount_err, quarantine_doc["error_details"]
    if amount_corr:
        corrections.append(amount_corr)

    # Validated Record Document
    validated_doc = {
        "order_id": clean_order_id,
        "customer_id": customer_id,
        "phone": cleaned_phone,
        "items": parsed_items,
        "order_date": iso_time,
        "total_amount": cleaned_amount,
        "corrections": corrections,
        "run_id": run_id,
    }

    return True, validated_doc, len(corrections), None, None
