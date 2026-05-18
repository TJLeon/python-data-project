import sys
from pathlib import Path

from src.ingestor import ingest_all_mhtml
from src.processor import process_all_html
from src.loader import load_all_jsons
from src.profiler import run_data_profile

SOURCE_DIR = Path("data/0_source")
BRONZE_DIR = Path("data/1_bronze")
SILVER_DIR = Path("data/2_silver")
GOLD_DIR = Path("data/3_gold")
DB_NAME = "jobs.db"

def file_idempotency(input_dir, output_dir):
	if not input_dir.exists(): # Ai: Check if source folder exists before trying to read it
		print(f"Error: {input_dir} does not exist")
		return False

	if not input_dir.is_dir(): # Ai: 1. Verify it actually exists and is a directory
		print(f"Error: {input_dir} is not a directory")
		return False

	if not any(input_dir.iterdir()): # Ai: 2. Check if it's empty
		print(f"Error: {input_dir} is empty")
		return False

	output_dir.mkdir(parents=True, exist_ok=True) # Ai: Create output folder if it doesn't exist
	return True

def run_profiler():
	db_path = GOLD_DIR/DB_NAME
	run_data_profile(db_path)

def run_gold():
	input_dir = SILVER_DIR
	output_dir = GOLD_DIR
	if not file_idempotency(input_dir, output_dir):
		return
	load_all_jsons(input_dir, output_dir)

def run_silver():
	input_dir = BRONZE_DIR
	output_dir = SILVER_DIR
	if not file_idempotency(input_dir, output_dir):
		return
	process_all_html(input_dir, output_dir)

def run_bronze():
	input_dir = SOURCE_DIR
	output_dir = BRONZE_DIR
	if not file_idempotency(input_dir, output_dir):
		return
	ingest_all_mhtml(input_dir, output_dir)

def main():
	if len(sys.argv) != 2:
		print("Usage: python main.py <command>")
		return

	match sys.argv[1]:
		case "ingest":
			run_bronze()
		case "process":
			run_silver()
		case "load":
			run_gold()
		case _:
			print(f"Invalid Argument: {sys.argv[1]}")

if __name__ == "__main__":
	main()
