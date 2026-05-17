import email
from email import policy
from pathlib import Path

def ingest_all_mhtml(input_dir: Path, output_dir: Path):
	print("🥉 Bronze: extract HTML content")

	extracted = 0
	failed = 0

	# Ai: Iterate through all .mhtml files in the source directory
	for mhtml_file in input_dir.glob("*.mhtml"):
		html_content = None

		# Ai: MHTML is essentially an email multi-part archive.
		#     Read it using the built-in email module
		with open(mhtml_file, 'r', encoding='utf-8', errors='ignore') as f:
			msg = email.message_from_file(f, policy=policy.default)

			# Ai: Look for the 'text/html' part in the multi-part structure
			for part in msg.walk():
				if part.get_content_type() == 'text/html':
					# Ai: Extract and decode the quoted-printable payload
					#     decode=True automatically handles the quoted-printable decoding
					html_content = part.get_payload(decode=True)
					break

		if html_content:
			# Ai: Output: Save to bronze directory
			output_file = output_dir / f"{mhtml_file.stem}.html"
			with open(output_file, 'wb') as f:
				f.write(html_content)

			# Ai: Tracking: Success
			print(f"✅ Extracted: {mhtml_file.name}")
			extracted += 1
		else:
			# Ai: Tracking: Failure
			print(f"⚠️ No HTML content found in: {mhtml_file.name}")
			failed += 1

	# Ai: Print final summary expected format
	print("\n📊 Bronze Summary:")
	print(f"Total: {extracted + failed} | Extracted: {extracted} | Failed: {failed}")
