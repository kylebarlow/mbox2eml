//////////////////////////////////////////////////////
///// main entry point for mbox2eml.cc
/////////////////////////////////////////////////////
// Copyright (c) Bishoy H.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// Description:
// This tool, mbox2eml, is designed to extract individual email messages from
// chunked mbox files and save them as separate gzip-compressed .eml.gz files in Maildir-compatible format. 
// It processes multiple mbox files named chunk_0.mbox, chunk_1.mbox, etc. in numerical order.
// It utilizes multithreading to speed up the processing of large mbox files by distributing 
// the workload across multiple CPU cores, but it requires enough memory to load each chunk file. 
// The tool takes two command-line arguments: the path to the input directory containing chunk files
// and the output directory where a Maildir structure (cur/new/tmp) will be created and the compressed 
// .eml.gz files will be saved in the cur subdirectory with continuous numbering across chunks.

// Compile with  g++ -O3 -std=c++23 -pthread -lstdc++fs -lz -o mbox2eml mbox2eml.cc 


#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <filesystem>
#include <algorithm>
#include <regex>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <chrono>
#include <random>
#include <unistd.h>
#include <zlib.h>

namespace fs = std::filesystem;

// Structure to hold email data
struct Email {
  std::string content;
  std::time_t timestamp;
};

// Function to parse RFC 2822 date format to timestamp
std::time_t parseEmailDate(const std::string& date_str) {
  // Common email date formats to try
  std::vector<std::string> formats = {
    "%a, %d %b %Y %H:%M:%S %z",     // RFC 2822: "Mon, 01 Jan 2024 12:00:00 +0000"
    "%d %b %Y %H:%M:%S %z",        // "01 Jan 2024 12:00:00 +0000"
    "%a, %d %b %Y %H:%M:%S",       // Without timezone
    "%d %b %Y %H:%M:%S"
  };
  
  std::tm tm = {};
  for (const auto& format : formats) {
    std::istringstream ss(date_str);
    ss >> std::get_time(&tm, format.c_str());
    if (!ss.fail()) {
      return std::mktime(&tm);
    }
  }
  
  // If parsing fails, return current time as fallback
  auto now = std::chrono::system_clock::now();
  return std::chrono::system_clock::to_time_t(now);
}

// Function to extract Date header from email content
std::time_t extractEmailTimestamp(const std::string& content) {
  std::istringstream stream(content);
  std::string line;
  
  // Skip the "From " line and look for Date header
  while (std::getline(stream, line)) {
    if (line.empty()) {
      // End of headers, no Date found
      break;
    }
    
    // Check for Date header (case-insensitive)
    if (line.length() > 5 && 
        (line.substr(0, 5) == "Date:" || line.substr(0, 5) == "date:")) {
      std::string date_part = line.substr(5);
      // Remove leading/trailing whitespace
      date_part.erase(0, date_part.find_first_not_of(" \t"));
      date_part.erase(date_part.find_last_not_of(" \t\r\n") + 1);
      return parseEmailDate(date_part);
    }
  }
  
  // Fallback to current time if no Date header found
  auto now = std::chrono::system_clock::now();
  return std::chrono::system_clock::to_time_t(now);
}

// Function to extract individual emails from the mbox file
std::vector<Email> extractEmails(const std::string& mbox_file) {
  std::vector<Email> emails;
  std::ifstream file(mbox_file);
  std::string line;
  Email current_email;

  while (std::getline(file, line)) {
    if (line.starts_with("From ")) { // use c++20 feature
      // Start of a new email
      if (!current_email.content.empty()) {
        current_email.timestamp = extractEmailTimestamp(current_email.content);
        emails.push_back(current_email);
      }
      current_email.content = line + "\n";
    } else {
      current_email.content += line + "\n";
    }
  }

  // Add the last email
  if (!current_email.content.empty()) {
    current_email.timestamp = extractEmailTimestamp(current_email.content);
    emails.push_back(current_email);
  }

  return emails;
}

// Function to create Maildir structure
void createMaildirStructure(const std::string& output_dir) {
  try {
    // Create main output directory if it doesn't exist
    fs::create_directories(output_dir);
    
    // Create Maildir subdirectories
    fs::create_directory(output_dir + "/cur");
    fs::create_directory(output_dir + "/new");
    fs::create_directory(output_dir + "/tmp");
    
    std::cout << "Created Maildir structure in " << output_dir << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Error creating Maildir structure: " << e.what() << std::endl;
    throw;
  }
}

// Function to find and sort chunk files
std::vector<std::string> findChunkFiles(const std::string& input_dir) {
  std::vector<std::pair<int, std::string>> chunk_files;
  std::regex chunk_pattern(R"(chunk_(\d+)\.mbox)");
  
  try {
    for (const auto& entry : fs::directory_iterator(input_dir)) {
      if (entry.is_regular_file()) {
        std::string filename = entry.path().filename().string();
        std::smatch match;
        
        if (std::regex_match(filename, match, chunk_pattern)) {
          int chunk_number = std::stoi(match[1].str());
          chunk_files.emplace_back(chunk_number, entry.path().string());
        }
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "Error reading directory: " << e.what() << std::endl;
  }
  
  // Sort by chunk number
  std::sort(chunk_files.begin(), chunk_files.end());
  
  // Extract just the file paths
  std::vector<std::string> sorted_files;
  for (const auto& pair : chunk_files) {
    sorted_files.push_back(pair.second);
  }
  
  return sorted_files;
}

// Function to compress data using gzip
std::string compressGzip(const std::string& data) {
  z_stream zs;
  memset(&zs, 0, sizeof(zs));
  
  // Use faster compression level for better throughput in multi-threaded scenario
  if (deflateInit2(&zs, Z_BEST_SPEED, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
    throw std::runtime_error("deflateInit2 failed");
  }
  
  zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(data.data()));
  zs.avail_in = data.size();
  
  int ret;
  char outbuffer[32768];
  std::string compressed;
  
  do {
    zs.next_out = reinterpret_cast<Bytef*>(outbuffer);
    zs.avail_out = sizeof(outbuffer);
    
    ret = deflate(&zs, Z_FINISH);
    
    if (compressed.size() < zs.total_out) {
      compressed.append(outbuffer, zs.total_out - compressed.size());
    }
  } while (ret == Z_OK);
  
  deflateEnd(&zs);
  
  if (ret != Z_STREAM_END) {
    throw std::runtime_error("Error during compression");
  }
  
  return compressed;
}

// Function to generate Maildir-compatible filename using email timestamp
std::string generateMaildirFilename(const Email& email, int email_count) {
  // Use the email's actual timestamp instead of current time
  std::ostringstream unique_id;
  unique_id << email.timestamp << ".M" << email_count << "P" << getpid() << "_mbox2eml";
  
  // Add flags section - :2,S (Seen flag for processed emails)
  std::string filename = unique_id.str() + ":2,S";
  
  return filename;
}

// Function to save an email to a compressed eml.gz file in Maildir cur directory
void saveEmail(const Email& email, const std::string& output_dir, int email_count) {
  std::string maildir_filename = generateMaildirFilename(email, email_count);
  std::string filename = output_dir + "/cur/" + maildir_filename;
  
  try {
    std::string compressed_content = compressGzip(email.content);
    
    std::ofstream outfile(filename, std::ios::binary);
    if (!outfile) {
      throw std::runtime_error("Failed to create output file: " + filename);
    }
    
    outfile.write(compressed_content.data(), compressed_content.size());
    if (!outfile) {
      throw std::runtime_error("Failed to write compressed data to: " + filename);
    }
  } catch (const std::exception& e) {
    std::cerr << "Error saving email " << email_count << ": " << e.what() << std::endl;
  }
}

// Worker thread function to process emails
void workerThread(const std::vector<Email>& emails, const std::string& output_dir, 
                  int start_index, int end_index, int& global_counter, std::mutex& counter_mutex) {
  for (int i = start_index; i < end_index; ++i) {
    // Get email number under minimal lock
    int email_number;
    {
      std::lock_guard<std::mutex> lock(counter_mutex);
      email_number = global_counter++;
    }
    
    // Do all the heavy work (compression, I/O) outside the lock
    saveEmail(emails[i], output_dir, email_number);
    
    // Optional: uncomment for progress tracking (but adds lock contention)
    // {
    //   std::lock_guard<std::mutex> lock(counter_mutex);
    //   std::cout << "Saved email_" << email_number << std::endl;
    // }
  }
}

int main(int argc, char* argv[]) {
  // Check for correct number of arguments
  if (argc != 3) {
    std::cerr << "mbox2eml: Extract individual email messages from chunked mbox files and save them as separate compressed .eml.gz files in Maildir format." << std::endl;
    std::cerr << "Error: Incorrect number of arguments." << std::endl;
    std::cerr << "Usage: " << argv[0] << " <input_directory> <output_directory>" << std::endl;
    std::cerr << "Input directory should contain files named: chunk_0.mbox, chunk_1.mbox, etc." << std::endl;
    return 1;
  }

  std::string input_dir = argv[1];
  std::string output_dir = argv[2];

  // Create Maildir structure in output directory
  try {
    createMaildirStructure(output_dir);
  } catch (const std::exception& e) {
    std::cerr << "Error creating Maildir structure: " << e.what() << std::endl;
    return 1;
  }

  // Find all chunk files in the input directory
  std::vector<std::string> chunk_files = findChunkFiles(input_dir);
  if (chunk_files.empty()) {
    std::cerr << "No chunk files found in " << input_dir << std::endl;
    std::cerr << "Looking for files named: chunk_0.mbox, chunk_1.mbox, etc." << std::endl;
    return 1;
  }

  std::cout << "Found " << chunk_files.size() << " chunk files to process." << std::endl;

  // Determine the number of threads to use (e.g., based on CPU cores)
  int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    num_threads = 2; // Default to 2 threads if hardware concurrency is unknown
  }

  std::mutex counter_mutex;
  int global_email_counter = 0;
  int total_emails_processed = 0;

  // Process each chunk file sequentially to maintain order
  for (const std::string& chunk_file : chunk_files) {
    std::cout << "Processing " << fs::path(chunk_file).filename().string() << "..." << std::endl;
    
    // Extract emails from current chunk
    std::vector<Email> emails = extractEmails(chunk_file);
    std::cout << "Extracted " << emails.size() << " emails from current chunk." << std::endl;
    
    if (emails.empty()) {
      std::cout << "No emails found in " << fs::path(chunk_file).filename().string() << ", skipping." << std::endl;
      continue;
    }

    // Calculate the number of emails per thread
    int emails_per_thread = emails.size() / num_threads;
    int remaining_emails = emails.size() % num_threads;

    // Create and launch worker threads for current chunk
    std::vector<std::thread> threads;
    int start_index = 0;

    for (int i = 0; i < num_threads; ++i) {
      int end_index = start_index + emails_per_thread;
      if (i < remaining_emails) {
        end_index++;
      }
      threads.emplace_back(workerThread, std::ref(emails), output_dir, start_index, end_index, 
                           std::ref(global_email_counter), std::ref(counter_mutex));
      start_index = end_index;
    }

    // Wait for all threads to finish processing current chunk
    for (auto& thread : threads) {
      thread.join();
    }
    
    total_emails_processed += emails.size();
    std::cout << "Completed processing " << fs::path(chunk_file).filename().string() 
              << " (" << emails.size() << " emails)" << std::endl;
  }

  std::cout << "Finished processing all " << chunk_files.size() << " chunks." << std::endl;
  std::cout << "Total emails processed: " << total_emails_processed << std::endl;
  return 0;
}
