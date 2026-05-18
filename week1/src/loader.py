import json
import sqlite3
from pathlib import Path

def load_all_jsons(input_dir: Path, output_dir: Path):
	print("🥇 Gold: load JSON data into SQL db")

	# Ai: Define database path
	db_path = output_dir / "jobs.db"

	# Ai: 2. Data Modeling & Storage Initialization
	#     Connect to the SQLite database (creates the file if it doesn't exist)
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()

	# Ai: Create the jobs table with source_id as the Primary Key
	#     Using 'CREATE TABLE IF NOT EXISTS' ensures idempotency on subsequent runs
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS jobs (
			source_id TEXT PRIMARY KEY,
			job_title TEXT,
			company TEXT,
			description TEXT,
			tech_stack TEXT
		)
	""")
	conn.commit()

	total = 0
	inserted = 0
	skipped = 0

	# Ai: 3. Load Silver JSON data
	for json_file in input_dir.glob("*.json"):
		total += 1
		try:
			# Ai: Read JSON with utf-8 encoding
			with open(json_file, 'r', encoding='utf-8') as f:
				data = json.load(f)

			# Ai: 4. Data Storage & Idempotency
			#     Using INSERT OR IGNORE specifically checks if the PRIMARY KEY (source_id)
			#     already exists in the table. If it does, it skips inserting to prevent duplicates.
			cursor.execute(
				"""
					INSERT OR IGNORE INTO jobs (source_id, job_title, company, description, tech_stack)
					VALUES (?, ?, ?, ?, ?)
				""",
				(data.get("source_id"), data.get("job_title"), data.get("company"), data.get("description"), data.get("tech_stack")),
			)

			# Ai: The cursor.rowcount tells us exactly how many rows were actually inserted by the previous query.
			#     If it's 1, the INSERT succeeded. If it's 0, it means IGNORE triggered because it was a duplicate.
			if cursor.rowcount > 0:
				print(f"✅ Inserted: {json_file.name}")
				inserted += 1
			else:
				print(f"⏭️ Skipped (duplicate): {json_file.name}")
				skipped += 1

		except Exception as e:
			# Ai: Using Skipped here as per the required summary output format,
			#     though tracking failures might be preferred in a real-world scenario
			print(f"⚠️ Failed to process {json_file.name}: {e}")
			skipped += 1

	# Ai: Commit all the insertions to the database at once for performance
	conn.commit()
	conn.close()

	# Print final tracking summary
	print("\n📊 Gold Summary:")
	print(f"Total: {total} | Inserted: {inserted} | Skipped: {skipped}")
