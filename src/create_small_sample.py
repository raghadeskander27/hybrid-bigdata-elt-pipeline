"""
Sample data generator and extractor module.
Supports:
1. Creating synthetic dirty edge-case test dataset (test_dirty_orders.csv)
2. Extracting bounded row samples from large external CSV files (e.g. orders_huge_mixed_quality.csv)
"""
import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Optional, Union

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DEFAULT_DIRTY_DATASET_PATH, DATA_DIR

SAMPLE_RECORDS = [
    # 1. Perfectly clean valid record
    {
        "order_id": "ORD-1001",
        "customer_id": "CUST-001",
        "phone": "771234567",
        "items": '[{"item_id": "ITM-1", "name": "Laptop Stand", "qty": 1, "price": 45.0}]',
        "order_date": "2024-03-15T10:30:00Z",
        "total_amount": "45.00",
    },
    # 2. Yemeni phone with country code (+967), spaces, and dashes -> Normalization required
    {
        "order_id": "ORD-1002",
        "customer_id": "CUST-002",
        "phone": "+967 771-234-568",
        "items": '[{"item_id": "ITM-2", "name": "Wireless Mouse", "qty": 2, "price": 15.0}]',
        "order_date": "2023-11-20 14:15:00",
        "total_amount": "30.00",
    },
    # 3. Yemeni phone with 00967 and Eastern Arabic numerals in phone and total_amount + currency label
    {
        "order_id": "ORD-1003",
        "customer_id": "CUST-003",
        "phone": "00967 ٧٣١٢٣٤٥٦٩",
        "items": '[{"item_id": "ITM-3", "name": "Mechanical Keyboard", "qty": 1, "price": 8500}]',
        "order_date": "2024-01-10T09:00:00",
        "total_amount": "٨,٥٠٠ YER",
    },
    # 4. Yemeni phone starting with national trunk 0 (0781234560) -> Normalization to 781234560
    {
        "order_id": "ORD-1004",
        "customer_id": "CUST-004",
        "phone": "0781234560",
        "items": '[{"item_id": "ITM-4", "name": "USB Cable", "qty": 3, "price": 5.0}]',
        "order_date": "2024-05-01T12:00:00Z",
        "total_amount": "15.00 ريال",
    },
    # 5. Yemeni phone with 70 network and Persian numerals
    {
        "order_id": "ORD-1005",
        "customer_id": "CUST-005",
        "phone": "۹۶۷۷۰۱۲۳۴۵۶۱",
        "items": '[{"item_id": "ITM-5", "name": "Monitor", "qty": 1, "price": 120.0}]',
        "order_date": "2024-06-12T08:30:00Z",
        "total_amount": "120.00",
    },
    # 6. Yemeni phone with 71 network
    {
        "order_id": "ORD-1006",
        "customer_id": "CUST-006",
        "phone": "+967-71-2345672",
        "items": '[{"item_id": "ITM-6", "name": "Webcam", "qty": 1, "price": 40.0}]',
        "order_date": "2024-07-04T16:20:00Z",
        "total_amount": "40.00 YER",
    },
    # 7. QUARANTINE: Invalid Yemeni phone prefix (79...) -> INVALID_PHONE_NUMBER
    {
        "order_id": "ORD-2001",
        "customer_id": "CUST-007",
        "phone": "+967 791 234 567",
        "items": '[{"item_id": "ITM-7", "name": "Speaker", "qty": 1, "price": 25.0}]',
        "order_date": "2024-02-14T11:00:00Z",
        "total_amount": "25.00",
    },
    # 8. QUARANTINE: Phone number too short -> INVALID_PHONE_NUMBER
    {
        "order_id": "ORD-2002",
        "customer_id": "CUST-008",
        "phone": "77123",
        "items": '[{"item_id": "ITM-8", "name": "Headphones", "qty": 1, "price": 35.0}]',
        "order_date": "2024-02-15T11:00:00Z",
        "total_amount": "35.00",
    },
    # 9. QUARANTINE: Missing / Empty customer_id -> MISSING_CUSTOMER_ID
    {
        "order_id": "ORD-2003",
        "customer_id": "",
        "phone": "771234567",
        "items": '[{"item_id": "ITM-9", "name": "Desk Lamp", "qty": 1, "price": 18.0}]',
        "order_date": "2024-02-16T11:00:00Z",
        "total_amount": "18.00",
    },
    # 10. QUARANTINE: Whitespace only customer_id -> MISSING_CUSTOMER_ID
    {
        "order_id": "ORD-2004",
        "customer_id": "   ",
        "phone": "771234567",
        "items": '[{"item_id": "ITM-10", "name": "Notebook", "qty": 5, "price": 2.0}]',
        "order_date": "2024-02-17T11:00:00Z",
        "total_amount": "10.00",
    },
    # 11. QUARANTINE: Empty items array -> EMPTY_ITEMS
    {
        "order_id": "ORD-2005",
        "customer_id": "CUST-011",
        "phone": "771234567",
        "items": "[]",
        "order_date": "2024-02-18T11:00:00Z",
        "total_amount": "50.00",
    },
    # 12. QUARANTINE: Corrupted / Malformed items JSON -> CORRUPTED_ITEMS_JSON
    {
        "order_id": "ORD-2006",
        "customer_id": "CUST-012",
        "phone": "771234567",
        "items": '{"not_a_list": true}',
        "order_date": "2024-02-19T11:00:00Z",
        "total_amount": "50.00",
    },
    # 13. QUARANTINE: Malformed JSON syntax in items -> CORRUPTED_ITEMS_JSON
    {
        "order_id": "ORD-2007",
        "customer_id": "CUST-013",
        "phone": "771234567",
        "items": "[{item_id: unquoted_val",
        "order_date": "2024-02-20T11:00:00Z",
        "total_amount": "50.00",
    },
    # 14. QUARANTINE: Impossible date in past (year 1890) -> INVALID_IMPOSSIBLE_DATE
    {
        "order_id": "ORD-2008",
        "customer_id": "CUST-014",
        "phone": "771234567",
        "items": '[{"item_id": "ITM-14", "name": "Antique Clock", "qty": 1, "price": 200.0}]',
        "order_date": "1890-05-12T10:00:00Z",
        "total_amount": "200.00",
    },
    # 15. QUARANTINE: Impossible date in future (year 2099) -> INVALID_IMPOSSIBLE_DATE
    {
        "order_id": "ORD-2009",
        "customer_id": "CUST-015",
        "phone": "771234567",
        "items": '[{"item_id": "ITM-15", "name": "Futuristic Drone", "qty": 1, "price": 500.0}]',
        "order_date": "2099-12-31T23:59:59Z",
        "total_amount": "500.00",
    },
    # 16. QUARANTINE: Unparsable date string -> INVALID_IMPOSSIBLE_DATE
    {
        "order_id": "ORD-2010",
        "customer_id": "CUST-016",
        "phone": "771234567",
        "items": '[{"item_id": "ITM-16", "name": "Coffee Mug", "qty": 2, "price": 7.0}]',
        "order_date": "not-a-valid-date-string",
        "total_amount": "14.00",
    },
    # 17. QUARANTINE: Negative total amount -> AMBIGUOUS_NEGATIVE_VALUE
    {
        "order_id": "ORD-2011",
        "customer_id": "CUST-017",
        "phone": "771234567",
        "items": '[{"item_id": "ITM-17", "name": "Refund Voucher", "qty": 1, "price": -50.0}]',
        "order_date": "2024-03-01T15:00:00Z",
        "total_amount": "-50.00 YER",
    },
]


def create_dirty_sample_csv(
    target_path: Optional[Union[str, Path]] = None,
    output_path: Optional[Union[str, Path]] = None,
) -> Path:
    """Generates the dirty test CSV file containing synthetic edge cases."""
    chosen_path = target_path or output_path or DEFAULT_DIRTY_DATASET_PATH
    output_file = Path(chosen_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["order_id", "customer_id", "phone", "items", "order_date", "total_amount"]
    with open(output_file, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in SAMPLE_RECORDS:
            writer.writerow(record)

    return output_file


def extract_sample_from_huge_file(
    input_file: Union[str, Path],
    output_file: Union[str, Path],
    num_rows: int = 100000,
) -> Optional[Path]:
    """Extracts a bounded sample of rows from a large CSV file."""
    in_path = Path(input_file)
    out_path = Path(output_file)

    if not in_path.exists():
        print(f"Error: Input file '{in_path}' not found!")
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Reading from: {in_path}")
    print(f"Extracting {num_rows:,} rows into: {out_path} ...")

    with open(in_path, mode="r", encoding="utf-8", errors="replace") as fin:
        reader = csv.reader(fin)
        try:
            header = next(reader)
        except StopIteration:
            print("Error: Input file is completely empty.")
            return None

        with open(out_path, mode="w", encoding="utf-8", newline="") as fout:
            writer = csv.writer(fout)
            writer.writerow(header)

            count = 0
            for row in reader:
                writer.writerow(row)
                count += 1
                if count >= num_rows:
                    break

    print(f"Successfully extracted {count:,} records into {out_path}.")
    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic dirty sample or extract sample from huge CSV")
    parser.add_argument("--huge-file", action="store_true", help="Extract from huge dataset instead of synthetic")
    parser.add_argument("--input", default=r"C:\big data\orders_huge_mixed_quality.csv", help="Path to huge CSV file")
    parser.add_argument("--output", default=str(DEFAULT_DIRTY_DATASET_PATH), help="Path to output sample CSV")
    parser.add_argument("--rows", type=int, default=100000, help="Number of rows to sample")

    args = parser.parse_args()

    if args.huge_file and os.path.exists(args.input):
        extract_sample_from_huge_file(args.input, args.output, args.rows)
    else:
        generated = create_dirty_sample_csv(output_path=args.output)
        print(f"Successfully generated dirty sample dataset at: {generated} ({len(SAMPLE_RECORDS)} records)")