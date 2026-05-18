import json
from pathlib import Path
from bs4 import BeautifulSoup
from pydantic import BaseModel

# Ai: 1. Define the Pydantic Data Contract
class JobListing(BaseModel):
	source_id: str
	job_title: str
	company: str
	description: str

def process_all_html(input_dir: Path, output_dir: Path):
	print("🥈 Silver: processing HTML to JSON...")

	processed = 0
	skipped = 0

	for html_file in input_dir.glob("*.html"):
		try:
			# Ai: 2. Read HTML with utf-8 encoding
			with open(html_file, 'r', encoding='utf-8') as f:
				soup = BeautifulSoup(f, 'html.parser')

			# Ai: 3. Extract source_id from og:url
			#     Example tag: <meta property="og:url" content="https://domain/job/12345678" />
			og_url_tag = soup.find('meta', property='og:url')
			if not og_url_tag or not og_url_tag.get('content'):
				print(f"⚠️ Missing source_id in: {html_file.name}")
				skipped += 1
				continue

			og_url = og_url_tag['content'].rstrip('/')
			var_source_id = og_url.split('/')[-1] # Gets "12345678"

			# Ai: 4. Extract target fields using data-* attributes
			#     NOTE: JobStreet/Seek commonly uses `data-automation` for identification.
			#     If your HTML uses slightly different ones (like "data-cy"), change the keys below.
			title_tag = soup.find(attrs={"data-automation": "job-detail-title"})
			company_tag = soup.find(attrs={"data-automation": "advertiser-name"})
			desc_tag = soup.find(attrs={"data-automation": "jobAdDetails"})

			# Ai: Clean text preventing "fused words" using separator=" "
			var_job_title = title_tag.get_text(separator=" ", strip=True) if title_tag else ""
			var_company = company_tag.get_text(separator=" ", strip=True) if company_tag else ""
			var_description = desc_tag.get_text(separator=" ", strip=True) if desc_tag else ""

			# NEW: Explicitly check for missing fields and skip if empty
			if not var_job_title:
				print(f"⚠️ Missing job_title in: {html_file.name}")
				skipped += 1
				continue
			if not var_description:
				print(f"⚠️ Missing description in: {html_file.name}")
				skipped += 1
				continue
			if not var_company:
				print(f"⚠️ Missing company in: {html_file.name}")
				skipped += 1
				continue

			# Ai: 5. Validate and Structure with Pydantic
			job = JobListing(
				source_id=var_source_id,
				job_title=var_job_title,
				company=var_company,
				description=var_description
			)

			# Ai: 6. Save as UTF-8 encoded JSON
			output_file = output_dir / f"{html_file.stem}.json"

			with open(output_file, 'w', encoding='utf-8') as out_f:
				# Ai: Use json.dump() to write the JSON data using pydantic serialization
				json.dump(job.model_dump(), out_f, ensure_ascii=False, indent=2)

			print(f"✅ Processed: {html_file.name}")
			processed += 1

		except Exception as e:
			print(f"⚠️ Error processing {html_file.name}: {e}")
			skipped += 1

	# Ai: Print tracking summary
	print("\n📊 Silver Summary:")
	print(f"Total: {processed + skipped} | Processed: {processed} | skipped: {skipped}")
