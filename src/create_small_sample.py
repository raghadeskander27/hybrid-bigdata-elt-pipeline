import csv
import argparse
import os

def extract_sample_from_huge_file(input_file, output_file, num_rows=100000):
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found!")
        return

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    print(f"Reading from: {input_file}")
    print(f"Extracting {num_rows:,} rows into: {output_file} ...")

    with open(input_file, mode="r", encoding="utf-8", errors="replace") as fin:
        reader = csv.reader(fin)
        try:
            header = next(reader)
        except StopIteration:
            print("Error: Input file is completely empty.")
            return

        with open(output_file, mode="w", encoding="utf-8", newline="") as fout:
            writer = csv.writer(fout)
            writer.writerow(header)
            
            count = 0
            for row in reader:
                writer.writerow(row)
                count += 1
                if count >= num_rows:
                    break
                    
    print(f"Successfully extracted {count:,} records into {output_file}.")

def create_dirty_sample_csv(output_path="data/small_sample.csv"):
    huge_default_path = r"C:\big data\orders_huge_mixed_quality.csv"
    if os.path.exists(huge_default_path):
        extract_sample_from_huge_file(huge_default_path, output_path, 100000)
    else:
        print(f"Notice: Huge dataset not found at {huge_default_path}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract a small sample from huge CSV")
    parser.add_argument("--input", default=r"C:\big data\orders_huge_mixed_quality.csv", help="Path to huge CSV file")
    parser.add_argument("--output", default="data/small_sample.csv", help="Path to output sample CSV")
    parser.add_argument("--rows", type=int, default=100000, help="Number of rows to sample")
    
    args = parser.parse_args()
    extract_sample_from_huge_file(args.input, args.output, args.rows)