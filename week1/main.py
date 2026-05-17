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

def run_profiler():
	db_path = GOLD_DIR/DB_NAME
	run_data_profile(db_path)

def run_gold():
	input_dir = SILVER_DIR
	output_dir = GOLD_DIR
	load_all_jsons(input_dir, output_dir)

def run_silver():
	input_dir = BRONZE_DIR
	output_dir = SILVER_DIR
	process_all_html(input_dir, output_dir)

def run_bronze():
	input_dir = SOURCE_DIR
	output_dir = BRONZE_DIR
	if not input_dir.exists(): # Ai: Check if source folder exists before trying to read it
		print(f"Error: {input_dir} does not exist")
		return
	output_dir.mkdir(parents=True, exist_ok=True) # Ai: Create output folder if it doesn't exist
	ingest_all_mhtml(input_dir, output_dir)

def main():
	if len(sys.argv) != 2:
		print("Usage: python main.py <command>")
		return

	match sys.argv[1]:
		case "ingest":
			run_bronze()
		case _:
			print(f"Invalid Argument: {sys.argv[1]}")

if __name__ == "__main__":
	main()
