#def main():
#	print("Hello from week1!")


#if __name__ == "__main__":
#	main()

# above is original main func hello world from uv init
# uv is like the concept of app store

import sys
from pathlib import Path # Figure out why use Path? # cus cross platform i guess
from src.ingestor import ingest_all_mhtml
from src.processor import process_all_html
from src.loader import load_all_jsons
#from src.run_data_profile import run_data_profile
# i guess syntax is "from (system or local location) import (add code things at file location)"
from src.profiler import run_data_profile # i hope this is correct

# initialize variable to object in python i guess
SOURCE_DIR = Path("data/0_source")
BRONZE_DIR = Path("data/1_bronze")
SILVER_DIR = Path("data/2_silver")
GOLD_DIR = Path("data/3_gold")
DB_NAME = "jobs.db"

# python way of defining functions i guess
def run_profiler():
	db_path = GOLD_DIR/DB_NAME
	run_data_profile(db_path)

# where function scope and line end ???
# but i guess it looks very simple and clean

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
	# ORCHESTRATION TO BE IMPLEMENTED HERE
	if len(sys.argv) != 2:
		print("Usage: python main.py <command>")
		return

	match sys.argv[1]:
		case "ingest":
			run_bronze() # hope this is correct lmao
		case _:
			print(f"Invalid Argument: {sys.argv[1]}") # f inside print is to process curly braces {}
