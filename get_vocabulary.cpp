#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <chrono>

// Structure to store word statistics
struct WordStats {
    int wordCount = 0;       // Total occurrences
    int documentCount = 0;   // Number of documents containing the word
};

// Split text into words
std::vector<std::string> splitTextIntoWords(const std::string& text) {
    std::vector<std::string> words;
    std::istringstream iss(text);
    std::string word;
    words.reserve(text.size() / 6); // Rough estimate of average word length
    
    while (iss >> word) {
        // Optional: clean the word (remove punctuation, convert to lowercase)
        std::transform(word.begin(), word.end(), word.begin(), 
                      [](unsigned char c){ return std::tolower(c); });
        
        // Optional: remove punctuation
        word.erase(std::remove_if(word.begin(), word.end(), 
                  [](unsigned char c){ return std::ispunct(c); }), word.end());
        
        if (!word.empty()) {
            words.push_back(word);
        }
    }
    return words;
}

// Process a batch of documents
void processBatch(std::ifstream& inputFile, 
                 std::unordered_map<std::string, WordStats>& vocabulary,
                 int batchSize, int& totalProcessed, int& lineNumber) {
    
    std::string line;
    int batchCount = 0;
    int batchStart = lineNumber;
    
    while (batchCount < batchSize && std::getline(inputFile, line)) {
        
        std::string text = line;
        
        // Split text into words and update counts
        auto words = splitTextIntoWords(text);
        std::unordered_set<std::string> uniqueWords;
        
        for (const auto& word : words) {
            vocabulary[word].wordCount++;
            uniqueWords.insert(word);
        }
        
        // Update document count for each unique word in this document
        for (const auto& word : uniqueWords) {
            vocabulary[word].documentCount++;
        }
        
        batchCount++;
        totalProcessed++;
        lineNumber++;
    }
    
    std::cout << "Processed rows: " << std::setw(8) << batchStart 
              << " - " << std::setw(8) << (lineNumber - 1)
              << " (" << batchCount << " rows)"
              << " | Total processed: " << totalProcessed 
              << " | Vocabulary size: " << vocabulary.size() << std::endl;
}

// Write vocabulary to CSV
void writeVocabularyToCsv(const std::unordered_map<std::string, WordStats>& vocabulary,
                         const std::string& outputPath) {
    std::ofstream outputFile(outputPath);
    if (!outputFile.is_open()) {
        std::cerr << "Error: Could not open output file " << outputPath << std::endl;
        return;
    }
    
    // Write header
    outputFile << "word,index,word_count,document_count\n";
    
    // Write vocabulary entries
    int index = 0;
    for (const auto& entry : vocabulary) {
        outputFile << "\"" << entry.first << "\"," 
                  << index++ << "," 
                  << entry.second.wordCount << "," 
                  << entry.second.documentCount << "\n";
    }
    
    outputFile.close();
    std::cout << "Vocabulary written to " << outputPath << std::endl;
}

int main(int argc, char* argv[]) {
    auto startTime = std::chrono::high_resolution_clock::now();
    
    // Modified default path to match the correct file name
    std::string inputPath = "cleaned_wiki_text.csv";
    std::string outputPath = "vocabulary.csv";
    int batchSize = 100000;
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--input" && i + 1 < argc) {
            inputPath = argv[++i];
        } else if (arg == "--output" && i + 1 < argc) {
            outputPath = argv[++i];
        } else if (arg == "--batch-size" && i + 1 < argc) {
            batchSize = std::stoi(argv[++i]);
        }
    }
    
    // Print input parameters for debugging
    std::cout << "Parameters:" << std::endl;
    std::cout << "  Input file: " << inputPath << std::endl;
    std::cout << "  Output file: " << outputPath << std::endl;
    std::cout << "  Batch size: " << batchSize << std::endl;
    
    // Debug print before opening the file
    std::cout << "Attempting to open file: " << inputPath << std::endl;

    // Open input file
    std::ifstream inputFile(inputPath);
    if (!inputFile.is_open()) {
        std::cerr << "Error: Could not open input file " << inputPath << std::endl;
        return 1;
    }

    // Count lines for progress tracking (optional)
    std::ifstream countFile(inputPath);
    int totalLines = std::count(std::istreambuf_iterator<char>(countFile),
                                std::istreambuf_iterator<char>(), '\n');
    countFile.close();  
    
    std::cout << "Total lines to process: " << totalLines << std::endl;
    
    // Process file in batches
    std::unordered_map<std::string, WordStats> vocabulary;
    vocabulary.reserve(1000000); // Reserve space for efficiency
    
    int totalProcessed = 0;
    int lineNumber = 1;
    
    while (inputFile.good() && !inputFile.eof()) {
        processBatch(inputFile, vocabulary, batchSize, totalProcessed, lineNumber);
    }
    
    inputFile.close();
    
    // Write vocabulary to CSV
    writeVocabularyToCsv(vocabulary, outputPath);
    
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime).count();
    
    std::cout << "Processing completed in " << duration << " seconds" << std::endl;
    std::cout << "Final vocabulary size: " << vocabulary.size() << " words" << std::endl;
    
    return 0;
}