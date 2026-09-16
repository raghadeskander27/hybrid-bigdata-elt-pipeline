"""
Unit tests for data quality and cleaning rules.
Covers Yemeni phone normalization, customer ID validation, items parsing,
date boundary checking, price sanitation, and audit trail generation.
"""
import pytest
from src.quality_rules import (
    clean_phone_number,
    validate_customer_id,
    validate_items,
    validate_and_parse_order_time,
    clean_total_amount,
    validate_record,
    normalize_arabic_numerals,
)


class TestPhoneNormalization:
    """Test suite for Yemeni phone normalization and validation."""

    def test_clean_yemeni_phone_variations(self):
        # Format with +967, spaces, dashes
        phone, err, corr = clean_phone_number("+967 771 234 567")
        assert err is None
        assert phone == "771234567"
        assert corr["rule_code"] == "PHONE_NORMALIZED"
        assert corr["corrected_value"] == "771234567"

        # Format with national trunk 0
        phone, err, corr = clean_phone_number("0731234567")
        assert err is None
        assert phone == "731234567"
        assert corr["rule_code"] == "PHONE_NORMALIZED"

        # Format with 00967
        phone, err, corr = clean_phone_number("00967781234567")
        assert err is None
        assert phone == "781234567"

        # Format with plain 967
        phone, err, corr = clean_phone_number("967 71 123 4567")
        assert err is None
        assert phone == "711234567"

        # Format with Y-Telecom prefix 70
        phone, err, corr = clean_phone_number("701234567")
        assert err is None
        assert phone == "701234567"
        assert corr is None  # Already in standard format, no correction needed

    def test_arabic_and_persian_numerals_phone(self):
        # Eastern Arabic digits: ٠١٢٣٤٥٦٧٨٩
        phone, err, corr = clean_phone_number("+٩٦٧ ٧٧١ ٢٣٤ ٥٦٧")
        assert err is None
        assert phone == "771234567"
        assert corr["rule_code"] == "PHONE_NORMALIZED"

        # Persian digits: ۰۱۲۳۴۵۶۷۸۹
        phone, err, corr = clean_phone_number("۰۷۳۱۲۳۴۵۶۷")
        assert err is None
        assert phone == "731234567"

    def test_invalid_phone_numbers(self):
        # Invalid prefix (79 is not a valid Yemeni mobile prefix)
        phone, err, corr = clean_phone_number("+967 791 234 567")
        assert err == "INVALID_PHONE_NUMBER"
        assert phone is None

        # Too short
        phone, err, corr = clean_phone_number("7712345")
        assert err == "INVALID_PHONE_NUMBER"

        # Too long
        phone, err, corr = clean_phone_number("77123456789")
        assert err == "INVALID_PHONE_NUMBER"

        # None or empty
        phone, err, corr = clean_phone_number("")
        assert err == "INVALID_PHONE_NUMBER"

        phone, err, corr = clean_phone_number(None)
        assert err == "INVALID_PHONE_NUMBER"


class TestCustomerIDValidation:
    """Test suite for customer_id presence and validation."""

    def test_valid_customer_id(self):
        cid, err = validate_customer_id("CUST-1001")
        assert err is None
        assert cid == "CUST-1001"

    def test_missing_or_empty_customer_id(self):
        # Null / None
        cid, err = validate_customer_id(None)
        assert err == "MISSING_CUSTOMER_ID"
        assert cid is None

        # Empty string
        cid, err = validate_customer_id("")
        assert err == "MISSING_CUSTOMER_ID"

        # Pure whitespace
        cid, err = validate_customer_id("    \t  \n")
        assert err == "MISSING_CUSTOMER_ID"


class TestItemsValidation:
    """Test suite for items JSON array parsing."""

    def test_valid_items_list_and_json_string(self):
        # Python list
        items_list = [{"item_id": "ITM-1", "qty": 2}]
        parsed, err = validate_items(items_list)
        assert err is None
        assert len(parsed) == 1

        # Valid JSON string
        json_str = '[{"item_id": "ITM-2", "name": "Item B"}]'
        parsed, err = validate_items(json_str)
        assert err is None
        assert parsed[0]["item_id"] == "ITM-2"

    def test_empty_items(self):
        parsed, err = validate_items("[]")
        assert err == "EMPTY_ITEMS"

        parsed, err = validate_items([])
        assert err == "EMPTY_ITEMS"

        parsed, err = validate_items("")
        assert err == "EMPTY_ITEMS"

        parsed, err = validate_items(None)
        assert err == "EMPTY_ITEMS"

    def test_corrupted_items_json(self):
        # Malformed JSON syntax
        parsed, err = validate_items("[{invalid_json")
        assert err == "CORRUPTED_ITEMS_JSON"

        # Not a JSON list (e.g. JSON object / dict)
        parsed, err = validate_items('{"single_item": true}')
        assert err == "CORRUPTED_ITEMS_JSON"


class TestOrderTimeValidation:
    """Test suite for date parsing and 2000-2030 boundary checking."""

    def test_valid_dates(self):
        iso_str, err, corr = validate_and_parse_order_time("2024-03-15 14:30:00")
        assert err is None
        assert iso_str == "2024-03-15T14:30:00Z"
        assert corr["rule_code"] == "DATE_STANDARDIZED"

        # Already in ISO format
        iso_str2, err2, corr2 = validate_and_parse_order_time("2022-01-01T00:00:00Z")
        assert err2 is None
        assert iso_str2 == "2022-01-01T00:00:00Z"
        assert corr2 is None

    def test_impossible_dates_out_of_bounds(self):
        # Past boundary (< 2000)
        iso_str, err, _ = validate_and_parse_order_time("1890-05-12T10:00:00Z")
        assert err == "INVALID_IMPOSSIBLE_DATE"
        assert iso_str is None

        # Future boundary (> 2030)
        iso_str, err, _ = validate_and_parse_order_time("2099-12-31T23:59:59Z")
        assert err == "INVALID_IMPOSSIBLE_DATE"

        # Unparsable gibberish
        iso_str, err, _ = validate_and_parse_order_time("not-a-valid-date")
        assert err == "INVALID_IMPOSSIBLE_DATE"


class TestTotalAmountCleaning:
    """Test suite for price cleaning and negative value checking."""

    def test_clean_price_formats(self):
        # Comma thousands separator and YER currency label
        val, err, corr = clean_total_amount("15,000 YER")
        assert err is None
        assert val == 15000.0
        assert corr["rule_code"] == "PRICE_NORMALIZED"

        # Arabic numerals with Arabic currency ريال
        val, err, corr = clean_total_amount("٨,٥٠٠ ريال")
        assert err is None
        assert val == 8500.0
        assert corr["rule_code"] == "PRICE_NORMALIZED"

    def test_negative_price_ambiguity(self):
        val, err, _ = clean_total_amount("-500.00")
        assert err == "AMBIGUOUS_NEGATIVE_VALUE"
        assert val is None

        val, err, _ = clean_total_amount("-100 YER")
        assert err == "AMBIGUOUS_NEGATIVE_VALUE"


class TestAuditTrailAndRecordValidation:
    """Test suite for full record validation and audit trail logging."""

    def test_corrected_record_audit_trail(self):
        record = {
            "order_id": "ORD-999",
            "customer_id": "CUST-999",
            "phone": "+967 771-234-567",
            "items": '[{"item_id": "ITM-1", "name": "Test", "qty": 1}]',
            "order_date": "2024-06-01 10:00:00",
            "total_amount": "٥,٠٠٠ YER",
        }
        is_valid, doc, corr_count, err, _ = validate_record(record, run_id="test_run_1")
        assert is_valid is True
        assert err is None
        assert corr_count == 3  # phone, date, and price corrected
        assert len(doc["corrections"]) == 3

        rule_codes = {c["rule_code"] for c in doc["corrections"]}
        assert "PHONE_NORMALIZED" in rule_codes
        assert "DATE_STANDARDIZED" in rule_codes
        assert "PRICE_NORMALIZED" in rule_codes

    def test_clean_record_zero_corrections(self):
        record = {
            "order_id": "ORD-888",
            "customer_id": "CUST-888",
            "phone": "771234567",
            "items": [{"item_id": "ITM-1", "name": "Test", "qty": 1}],
            "order_date": "2024-06-01T10:00:00Z",
            "total_amount": 50.0,
        }
        is_valid, doc, corr_count, err, _ = validate_record(record, run_id="test_run_2")
        assert is_valid is True
        assert corr_count == 0
        assert len(doc["corrections"]) == 0
