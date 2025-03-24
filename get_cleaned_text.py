import os
import json
import re
import unicodedata
from unidecode import unidecode
import csv
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import time
import argparse

# Remove the --input argument
def parse_args():
    parser = argparse.ArgumentParser(description='Process Wikipedia dump and clean text')
    parser.add_argument('--output_dir', required=True, help='Path to Wikipedia dump output directory')
    parser.add_argument('--stopwords', required=True, help='Path to stopwords file')
    parser.add_argument('--csv', required=True, help='Path to output CSV file')
    parser.add_argument('--min_words', type=int, default=200, help='Minimum number of words to keep a document')
    parser.add_argument('--min_word_length', type=int, default=2, help='Minimum word length to keep')
    parser.add_argument('--batch_size', type=int, default=5000, help='Batch size for processing')
    return parser.parse_args()

# Function to clean the text
def clean_text(text, stopwords, min_word_length, min_words):
    
    # Normalize line breaks
    text = re.sub(r'[\r\n\u2028\u2029]+', ' ', text)
    
    # Convert Unicode characters to ASCII
    text = unidecode(text)
    text = unicodedata.normalize('NFKD', text)
    
    # Remove non-ASCII
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    
    # Convert to lowercase
    text = text.lower()
    
    # Keep only letters and spaces
    text = re.sub(r'[^a-z\s]', ' ', text)
    
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Filter out stopwords and words shorter than min_word_length
    words = text.split()
    filtered_words = [word for word in words if word not in stopwords and len(word) > min_word_length]
    
    # Join filtered words back into text
    cleaned_text = " ".join(filtered_words)
    
    # Check if the cleaned text has more than min_words words
    if len(filtered_words) > min_words:
        return cleaned_text
    else:
        return ""  # Return empty string if text has fewer than min_words words

# Initialize CSV file - create directories if they don't exist
def initialize_csv(path_csv):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(os.path.abspath(path_csv)), exist_ok=True)
    
    # Create or overwrite the file
    open(path_csv, 'w', encoding='utf-8').close()
    print(f"Initialized output file at: {path_csv}")

# Append batch entries to CSV
def append_to_csv(texts, path_csv, lock=None):
    if lock:
        lock.acquire()
    
    try:
        with open(path_csv, 'a', encoding='utf-8') as csvfile:
            # Write one text per line (no CSV formatting needed)
            for text in texts:
                csvfile.write(text + '\n')
    finally:
        if lock:
            lock.release()

# Process a single JSON file and return cleaned entries
def process_json_file(args):
    file_path, batch_size, path_csv, stopwords, min_word_length, min_words = args
    texts = []
    
    with open(file_path, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                entry = json.loads(line)
                if 'text' in entry:
                    cleaned_text = clean_text(entry['text'], stopwords, min_word_length, min_words)
                    if cleaned_text:  # Skip if text is empty after cleaning
                        texts.append(cleaned_text)
                        
                        # Write in batches to reduce file operations
                        if len(texts) >= batch_size:
                            append_to_csv(texts, path_csv)
                            texts = []
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in {file_path}: {e}")
    
    # Write any remaining entries
    if texts:
        append_to_csv(texts, path_csv)
    
    return len(texts)

# Process all files with progress bar
def process_all_files(path_dump_output, path_csv, batch_size, stopwords, min_word_length, min_words, file_pattern="wiki_"):
    all_files = []
    
    # Process folders in sorted order
    for folder in sorted(os.listdir(path_dump_output)):
        folder_path = os.path.join(path_dump_output, folder)
        
        if os.path.isdir(folder_path):
            print(f"Processing folder: {folder}")
            
            # Process files in sorted order within each folder
            for filename in sorted(os.listdir(folder_path)):
                file_path = os.path.join(folder_path, filename)
                
                # Skip if not a file
                if not os.path.isfile(file_path):
                    continue
                    
                # Only include files that match the pattern
                if file_pattern in filename:
                    all_files.append(file_path)
    
    print(f"Found {len(all_files)} matching files to process")
    
    # Process files in parallel
    num_workers = max(1, cpu_count() - 1)  # Use all but one CPU core
    
    start_time = time.time()
    
    with Pool(num_workers) as pool:
        args = [(file, batch_size, path_csv, stopwords, min_word_length, min_words) for file in all_files]
        total_entries = 0
        
        # Process with progress bar
        for result in tqdm(pool.imap_unordered(process_json_file, args), total=len(args), desc="Processing files"):
            total_entries += result
    
    elapsed_time = time.time() - start_time
    print(f"Processed {total_entries} entries in {elapsed_time:.2f} seconds")
    print(f"Average speed: {total_entries / elapsed_time:.2f} entries/second")

# Main execution
def main():
    args = parse_args()
    
    if not os.path.exists(args.output_dir):
        raise FileNotFoundError(f"Output directory not found: {args.output_dir}")
    
    if not os.path.exists(args.stopwords):
        raise FileNotFoundError(f"Stopwords file not found: {args.stopwords}")
    
    # Load stopwords
    with open(args.stopwords, "r", encoding="utf-8") as f:
        stopwords = set(f.read().splitlines())
    
    # Initialize CSV file (create directories if needed)
    initialize_csv(args.csv)
    
    # Process all files
    process_all_files(
        args.output_dir, 
        args.csv, 
        args.batch_size, 
        stopwords, 
        args.min_word_length, 
        args.min_words
    )
    
    print(f"Text-only file created at: {args.csv}")

if __name__ == "__main__":
    main()