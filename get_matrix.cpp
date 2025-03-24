#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <utility>
#include <cctype>

// Custom hash function for word pairs using integers for efficiency
struct PairHash {
    std::size_t operator()(const std::pair<int, int>& p) const {
        // Combine the hashes of the two integers
        return (static_cast<size_t>(p.first) << 32) | static_cast<size_t>(p.second);
    }
};

void processChunk(
    const std::vector<std::string>& chunk,
    std::unordered_map<std::pair<int, int>, float, PairHash>& matrix,
    const std::unordered_map<std::string, int>& word_to_index,
    int window_size,
    const std::vector<double>& weights
) {
    for (const auto& line : chunk) {
        std::istringstream iss(line);
        std::string word;
        std::vector<int> indices;
        
        // Extract all valid words from the line
        while (iss >> word) {
            // Clean the word - convert to lowercase and remove non-alphanumeric chars
            std::string clean_word;
            for (char c : word) {
                if (std::isalnum(c)) {
                    clean_word += std::tolower(c);
                }
            }
            
            // Skip very short words or words not in our vocabulary
            if (clean_word.length() <= 2 || word_to_index.find(clean_word) == word_to_index.end()) {
                continue;
            }
            
            indices.push_back(word_to_index.at(clean_word));
        }
        
        // Process co-occurrences with sliding window
        for (size_t i = 0; i < indices.size(); ++i) {
            int idx1 = indices[i];
            size_t window_end = std::min(i + window_size + 1, indices.size());
            
            for (size_t j = i + 1; j < window_end; ++j) {
                int idx2 = indices[j];
                double weight = weights[j - i - 1];
                
                // Ensure idx1 <= idx2 for consistent storage (upper triangle)
                if (idx1 > idx2) {
                    std::swap(idx1, idx2);
                }
                
                // Update co-occurrence
                matrix[{idx1, idx2}] += static_cast<float>(weight);
            }
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <vocabulary_file> <text_file> <output_file> [window_size=10] [top_words=50000]" << std::endl;
        return 1;
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    
    std::string vocab_file = argv[1];
    std::string text_file = argv[2];
    std::string output_file = argv[3];
    
    // Parse optional arguments with defaults
    int window_size = 10;
    if (argc > 4) {
        window_size = std::stoi(argv[4]);
    }
    
    size_t top_words = 50000;
    if (argc > 5) {
        top_words = std::stoi(argv[5]);
    }
    
    // Pre-compute distance weights
    std::vector<double> weights(window_size);
    for (int i = 0; i < window_size; ++i) {
        weights[i] = 1.0 / (i + 1);
    }
    
    std::cout << "Processing with window size: " << window_size << ", top words: " << top_words << std::endl;
    
    // ========== Phase 1: Load vocabulary and create word-to-index mapping ==========
    std::cout << "Loading vocabulary..." << std::endl;
    std::unordered_map<std::string, int> word_to_index;
    std::vector<std::string> index_to_word;
    
    std::ifstream vocab_in(vocab_file);
    if (!vocab_in) {
        std::cerr << "Failed to open vocabulary file: " << vocab_file << std::endl;
        return 1;
    }
    
    // Skip the header line (assume CSV format)
    std::string line;
    std::getline(vocab_in, line);
    
    // Store word and count pairs
    std::vector<std::pair<std::string, int>> word_counts;
    
    while (std::getline(vocab_in, line)) {
        std::istringstream iss(line);
        std::string word, id_str, word_count_str, doc_count_str;
        
        // Parse CSV format with columns: word, id, word_count, doc_count
        if (std::getline(iss, word, ',') && 
            std::getline(iss, id_str, ',') && 
            std::getline(iss, word_count_str, ',') && 
            std::getline(iss, doc_count_str)) {
            
            // Normalize word to lowercase
            std::string clean_word;
            for (char c : word) {
                if (std::isalnum(c)) {
                    clean_word += std::tolower(c);
                }
            }
            
            // Convert word_count to integer
            int word_count = 0;
            try {
                word_count = std::stoi(word_count_str);
            } catch (const std::exception& e) {
                // Skip if word_count is not a valid number
                continue;
            }
            
            if (!clean_word.empty() && clean_word.length() > 2) {
                word_counts.emplace_back(clean_word, word_count);
            }
        }
    }
    vocab_in.close();
    
    // Sort by word count in descending order (using the word_count column)
    std::sort(word_counts.begin(), word_counts.end(),
              [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
                  return a.second > b.second;
              });
    
    // Take only the top N words
    size_t vocab_size = std::min(word_counts.size(), top_words);
    
    // Create index mappings
    for (size_t i = 0; i < vocab_size; ++i) {
        word_to_index[word_counts[i].first] = i;
        index_to_word.push_back(word_counts[i].first);
    }
    
    std::cout << "Using top " << vocab_size << " words from vocabulary" << std::endl;
    
    // Free memory
    word_counts.clear();
    word_counts.shrink_to_fit();
    
    // Main co-occurrence matrix
    std::unordered_map<std::pair<int, int>, float, PairHash> matrix;
    matrix.reserve(1000000);  // Pre-allocate for better performance
    
    // ========== Phase 2: Process text file in chunks ==========
    std::cout << "Processing text file..." << std::endl;
    
    std::ifstream text_in(text_file);
    if (!text_in) {
        std::cerr << "Failed to open text file: " << text_file << std::endl;
        return 1;
    }
    
    // Process in memory-efficient chunks
    const size_t CHUNK_SIZE = 10000;  // Lines per chunk
    
    std::vector<std::string> chunk;
    chunk.reserve(CHUNK_SIZE);
    
    size_t lines_processed = 0;
    size_t chunks_processed = 0;
    
    auto last_report_time = std::chrono::high_resolution_clock::now();
    
    while (std::getline(text_in, line)) {
        chunk.push_back(line);
        lines_processed++;
        
        if (chunk.size() >= CHUNK_SIZE) {
            // Process the chunk
            processChunk(chunk, matrix, word_to_index, window_size, weights);
            chunks_processed++;
            chunk.clear();
            chunk.reserve(CHUNK_SIZE);
            
            // Report progress every 30 seconds
            auto now = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_report_time).count();
            if (duration >= 30) {
                std::cout << "Processed " << lines_processed << " lines, " 
                          << chunks_processed << " chunks, " 
                          << matrix.size() << " pairs in memory" << std::endl;
                last_report_time = now;
            }
        }
    }
    
    // Process final chunk if not empty
    if (!chunk.empty()) {
        processChunk(chunk, matrix, word_to_index, window_size, weights);
        chunks_processed++;
    }
    
    text_in.close();
    
    // Write final matrix to output
    std::cout << "Writing final matrix with " << matrix.size() << " pairs to output file..." << std::endl;
    
    std::ofstream out(output_file);
    if (!out) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return 1;
    }
    
    // Write header
    out << "word1,word2,weight" << std::endl;
    
    // Write only the final matrix
    for (const auto& entry : matrix) {
        out << index_to_word[entry.first.first] << ","
            << index_to_word[entry.first.second] << ","
            << entry.second << std::endl;
    }
    
    out.close();
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
    
    std::cout << "Finished processing in " << duration << " seconds" << std::endl;
    std::cout << "Processed " << lines_processed << " lines in " << chunks_processed << " chunks" << std::endl;
    std::cout << "Output saved to: " << output_file << std::endl;
    
    return 0;
}