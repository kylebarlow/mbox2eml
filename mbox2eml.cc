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
// chunked mbox files (including Gmail Takeout format) and save them as separate .eml files 
// in Maildir-compatible format with attachment extraction. It processes multiple mbox files 
// named chunk_0.mbox, chunk_1.mbox, etc. in numerical order. Attachments are automatically 
// detected, extracted, compressed, and saved separately while email content is stripped of 
// attachments for faster indexing. It utilizes multithreading to speed up processing by 
// distributing the workload across multiple CPU cores. The tool takes two command-line 
// arguments: the path to the input directory containing chunk files and the output directory 
// where a Maildir structure (cur/new/tmp/attachments) will be created.

// Compile with  g++ -O3 -std=c++23 -pthread -lstdc++fs -o mbox2eml mbox2eml.cc 


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

// Structure to hold attachment data
struct Attachment {
  std::string filename;
  std::string content;
  std::string content_type;
};

// Structure to hold email data
struct Email {
  std::string content;  // Email content with attachments stripped
  std::time_t timestamp;
  std::vector<Attachment> attachments;
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

// Function to decode base64 content properly
std::string decodeBase64(const std::string& encoded) {
  const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string decoded;
  std::vector<int> lookup(256, -1);
  
  // Create lookup table
  for (int i = 0; i < 64; i++) {
    lookup[static_cast<unsigned char>(chars[i])] = i;
  }
  
  // Clean input - remove whitespace and line breaks
  std::string clean_encoded;
  for (char c : encoded) {
    if (lookup[static_cast<unsigned char>(c)] >= 0 || c == '=') {
      clean_encoded += c;
    }
  }
  
  for (size_t i = 0; i < clean_encoded.length(); i += 4) {
    if (i + 1 >= clean_encoded.length()) break;
    
    int a = lookup[static_cast<unsigned char>(clean_encoded[i])];
    int b = lookup[static_cast<unsigned char>(clean_encoded[i + 1])];
    int c = (i + 2 < clean_encoded.length() && clean_encoded[i + 2] != '=') ? 
            lookup[static_cast<unsigned char>(clean_encoded[i + 2])] : 0;
    int d = (i + 3 < clean_encoded.length() && clean_encoded[i + 3] != '=') ? 
            lookup[static_cast<unsigned char>(clean_encoded[i + 3])] : 0;
    
    if (a >= 0 && b >= 0) {
      decoded += static_cast<char>((a << 2) | (b >> 4));
      
      if (i + 2 < clean_encoded.length() && clean_encoded[i + 2] != '=' && c >= 0) {
        decoded += static_cast<char>((b << 4) | (c >> 2));
        
        if (i + 3 < clean_encoded.length() && clean_encoded[i + 3] != '=' && d >= 0) {
          decoded += static_cast<char>((c << 6) | d);
        }
      }
    }
  }
  
  return decoded;
}

// Function to parse filename from Content-Disposition header
std::string parseFilename(const std::string& disposition_header) {
  // Look for filename= parameter
  size_t filename_pos = disposition_header.find("filename=");
  if (filename_pos == std::string::npos) {
    return "";
  }
  
  std::string filename = disposition_header.substr(filename_pos + 9);
  
  // Remove quotes and clean up
  if (!filename.empty() && filename.front() == '"') {
    filename = filename.substr(1);
  }
  
  // Find the end of the filename (next quote or semicolon)
  size_t end_pos = filename.find_first_of("\";");
  if (end_pos != std::string::npos) {
    filename = filename.substr(0, end_pos);
  }
  
  // Remove carriage returns
  filename.erase(std::remove(filename.begin(), filename.end(), '\r'), filename.end());
  filename.erase(std::remove(filename.begin(), filename.end(), '\n'), filename.end());
  
  return filename;
}

// Function to extract all boundaries from email content (including nested)
std::vector<std::string> extractBoundaries(const std::string& content) {
  std::vector<std::string> boundaries;
  std::istringstream stream(content);
  std::string line;
  
  // First pass: extract boundaries from main headers
  while (std::getline(stream, line)) {
    if (line.empty() || line == "\r") break; // End of headers
    
    if (line.find("Content-Type:") != std::string::npos && 
        line.find("multipart") != std::string::npos) {
      
      // Look for boundary in this line or continue reading
      std::string boundary_line = line;
      while (boundary_line.find("boundary=") == std::string::npos && std::getline(stream, line)) {
        boundary_line += " " + line;
        if (line.empty() || line == "\r") break;
      }
      
      size_t boundary_pos = boundary_line.find("boundary=");
      if (boundary_pos != std::string::npos) {
        std::string boundary = boundary_line.substr(boundary_pos + 9);
        
        // Remove quotes
        if (!boundary.empty() && boundary.front() == '"') {
          boundary = boundary.substr(1);
        }
        size_t end_quote = boundary.find('"');
        if (end_quote != std::string::npos) {
          boundary = boundary.substr(0, end_quote);
        }
        
        // Clean up
        boundary.erase(std::remove(boundary.begin(), boundary.end(), '\r'), boundary.end());
        boundary.erase(std::remove(boundary.begin(), boundary.end(), '\n'), boundary.end());
        
        if (!boundary.empty()) {
          boundaries.push_back(boundary);
        }
      }
    }
  }
  
  // Second pass: find nested boundaries in the content
  size_t search_pos = 0;
  while ((search_pos = content.find("Content-Type:", search_pos)) != std::string::npos) {
    size_t line_end = content.find('\n', search_pos);
    if (line_end == std::string::npos) break;
    
    std::string content_type_line = content.substr(search_pos, line_end - search_pos);
    if (content_type_line.find("multipart") != std::string::npos) {
      
      // Look for boundary= in this area
      size_t boundary_search_start = search_pos;
      size_t boundary_search_end = content.find("\n\n", search_pos);
      if (boundary_search_end == std::string::npos) {
        boundary_search_end = content.find("\r\n\r\n", search_pos);
      }
      if (boundary_search_end == std::string::npos) {
        boundary_search_end = search_pos + 500; // Reasonable header limit
      }
      
      std::string header_section = content.substr(boundary_search_start, 
                                                  boundary_search_end - boundary_search_start);
      
      size_t boundary_pos = header_section.find("boundary=");
      if (boundary_pos != std::string::npos) {
        std::string boundary = header_section.substr(boundary_pos + 9);
        
        // Remove quotes and clean up
        if (!boundary.empty() && boundary.front() == '"') {
          boundary = boundary.substr(1);
        }
        size_t end_quote = boundary.find('"');
        if (end_quote != std::string::npos) {
          boundary = boundary.substr(0, end_quote);
        }
        
        // Clean up whitespace and line breaks
        boundary.erase(std::remove(boundary.begin(), boundary.end(), '\r'), boundary.end());
        boundary.erase(std::remove(boundary.begin(), boundary.end(), '\n'), boundary.end());
        boundary.erase(std::remove(boundary.begin(), boundary.end(), ' '), boundary.end());
        
        // Remove semicolon if present (end of parameter)
        size_t semicolon_pos = boundary.find(';');
        if (semicolon_pos != std::string::npos) {
          boundary = boundary.substr(0, semicolon_pos);
        }
        
        if (!boundary.empty()) {
          // Check if we already have this boundary
          bool already_exists = false;
          for (const auto& existing : boundaries) {
            if (existing == boundary) {
              already_exists = true;
              break;
            }
          }
          if (!already_exists) {
            boundaries.push_back(boundary);
          }
        }
      }
    }
    
    search_pos = line_end + 1;
  }
  
  return boundaries;
}

// Function to extract attachments from MIME email content (Gmail Takeout compatible)
Email extractAttachments(const std::string& content) {
  Email email;
  
  // Extract all boundaries
  std::vector<std::string> boundaries = extractBoundaries(content);
  if (boundaries.empty()) {
    email.content = content; // Not multipart, return as-is
    return email;
  }
  
  // Debug: print found boundaries
  // std::cerr << "Found " << boundaries.size() << " boundaries:" << std::endl;
  // for (const auto& boundary : boundaries) {
  //   std::cerr << "  Boundary: '" << boundary << "'" << std::endl;
  // }
  
  // Find the first boundary position to extract headers
  size_t first_boundary_pos = std::string::npos;
  std::string first_delimiter;
  for (const auto& boundary : boundaries) {
    std::string delimiter = "--" + boundary;
    size_t pos = content.find(delimiter);
    if (pos != std::string::npos && (first_boundary_pos == std::string::npos || pos < first_boundary_pos)) {
      first_boundary_pos = pos;
      first_delimiter = delimiter;
    }
  }
  
  if (first_boundary_pos == std::string::npos) {
    email.content = content;
    return email;
  }
  
  std::string headers = content.substr(0, first_boundary_pos);
  
  // Build new email content with headers
  std::ostringstream stripped_email;
  stripped_email << headers;
  
  // Parse all parts using ALL boundaries
  std::vector<std::string> text_parts;
  std::vector<std::string> attachment_markers;
  
  // Process all boundaries
  for (const auto& boundary : boundaries) {
    std::string delimiter = "--" + boundary;
    size_t search_pos = 0;
    
    while (true) {
      size_t part_start = content.find(delimiter, search_pos);
      if (part_start == std::string::npos) break;
      
      // Skip the boundary line
      size_t content_start = content.find('\n', part_start);
      if (content_start == std::string::npos) break;
      content_start++;
      
      // Find next boundary (any boundary, not just same one)
      size_t part_end = std::string::npos;
      for (const auto& any_boundary : boundaries) {
        std::string any_delimiter = "--" + any_boundary;
        size_t pos = content.find(any_delimiter, content_start);
        if (pos != std::string::npos && (part_end == std::string::npos || pos < part_end)) {
          part_end = pos;
        }
        
        // Also check for final boundary
        std::string final_delimiter = any_delimiter + "--";
        pos = content.find(final_delimiter, content_start);
        if (pos != std::string::npos && (part_end == std::string::npos || pos < part_end)) {
          part_end = pos;
        }
      }
      
      if (part_end == std::string::npos) break;
      
      std::string part = content.substr(content_start, part_end - content_start);
      
      // Skip if this part is too small (likely boundary fragment)
      if (part.length() < 10) {
        search_pos = part_end;
        continue;
      }
      
      // Parse part headers and body
      std::istringstream part_stream(part);
      std::string line;
      std::string content_type;
      std::string content_disposition;
      std::string content_encoding;
      std::string content_id;
      std::string filename;
      bool in_headers = true;
      std::ostringstream body_stream;
      
      while (std::getline(part_stream, line)) {
        if (in_headers && (line.empty() || line == "\r")) {
          in_headers = false;
          continue;
        }
        
        if (in_headers) {
          if (line.find("Content-Type:") != std::string::npos) {
            content_type = line;
          } else if (line.find("Content-Disposition:") != std::string::npos) {
            content_disposition = line;
            filename = parseFilename(line);
          } else if (line.find("Content-Transfer-Encoding:") != std::string::npos) {
            content_encoding = line;
          } else if (line.find("Content-ID:") != std::string::npos) {
            content_id = line;
          }
        } else {
          body_stream << line << "\n";
        }
      }
      
      std::string body = body_stream.str();
      
      // Determine if this is an attachment
      bool is_attachment = false;
      
      // Check Content-Disposition for attachment
      if (content_disposition.find("attachment") != std::string::npos) {
        is_attachment = true;
      }
      // Check Content-ID (inline images/attachments)
      else if (!content_id.empty()) {
        is_attachment = true;
      }
      // Check for any images (very aggressive)
      else if (content_type.find("image/") != std::string::npos) {
        is_attachment = true;
      }
      // Check for any base64 content (lowered threshold)
      else if (content_encoding.find("base64") != std::string::npos && body.length() > 100) {
        is_attachment = true;
      }
      // Check for non-text content types
      else if (!content_type.empty() && 
               content_type.find("text/plain") == std::string::npos &&
               content_type.find("text/html") == std::string::npos &&
               content_type.find("multipart") == std::string::npos) {
        is_attachment = true;
      }
      // Check for other binary content types
      else if (content_type.find("application/") != std::string::npos ||
               content_type.find("video/") != std::string::npos ||
               content_type.find("audio/") != std::string::npos) {
        is_attachment = true;
      }
      // Very aggressive: any part with a filename
      else if (!filename.empty()) {
        is_attachment = true;
      }
      
      if (is_attachment && !body.empty()) {
        // Extract attachment
        Attachment attachment;
        attachment.filename = filename.empty() ? 
          ("attachment_" + std::to_string(email.attachments.size()) + ".bin") : filename;
        attachment.content_type = content_type;
        
        // Decode based on encoding
        if (content_encoding.find("base64") != std::string::npos) {
          attachment.content = decodeBase64(body);
        } else {
          attachment.content = body;
        }
        
        email.attachments.push_back(attachment);
        
        // Generate the actual saved filename for reference
        std::ostringstream saved_filename;
        saved_filename << "email_" << std::setfill('0') << std::setw(9) << 0  // placeholder for email_count
                       << "_attachment_" << (email.attachments.size() - 1) << "_" << attachment.filename;
        
        // Determine compression based on file type (simple check here)
        std::string lower_filename = attachment.filename;
        std::transform(lower_filename.begin(), lower_filename.end(), lower_filename.begin(), ::tolower);
        bool will_compress = !(lower_filename.ends_with(".jpg") || lower_filename.ends_with(".jpeg") ||
                              lower_filename.ends_with(".png") || lower_filename.ends_with(".gif") ||
                              lower_filename.ends_with(".zip") || lower_filename.ends_with(".gz") ||
                              attachment.content_type.find("image/") != std::string::npos);
        
        std::string full_saved_name = saved_filename.str() + (will_compress ? ".gz" : "");
        
        // Add marker with both original and saved filenames
        attachment_markers.push_back("[Attachment extracted: " + attachment.filename + 
                                    " (" + std::to_string(attachment.content.length()) + " bytes) " +
                                    "-> saved as: " + full_saved_name + "]");
      } else if (!content_type.empty() && 
                 (content_type.find("text/") != std::string::npos || 
                  content_type.find("multipart") != std::string::npos)) {
        // Keep text content
        text_parts.push_back(part);
      }
      
      search_pos = part_end;
    }
  }
  
  // Rebuild email content with only text parts
  if (!text_parts.empty() || !attachment_markers.empty()) {
    stripped_email << first_delimiter << "\n";
    
    // Add text parts
    for (const auto& text_part : text_parts) {
      stripped_email << text_part;
      stripped_email << first_delimiter << "\n";
    }
    
    // Add attachment markers as text
    if (!attachment_markers.empty()) {
      stripped_email << "Content-Type: text/plain; charset=\"utf-8\"\n\n";
      stripped_email << "Attachments extracted:\n";
      for (const auto& marker : attachment_markers) {
        stripped_email << marker << "\n";
      }
      stripped_email << "\n" << first_delimiter << "\n";
    }
    
    stripped_email << first_delimiter << "--\n";
  }
  
  email.content = stripped_email.str();
  return email;
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
        Email processed_email = extractAttachments(current_email.content);
        processed_email.timestamp = current_email.timestamp;  // Preserve timestamp
        emails.push_back(processed_email);
      }
      current_email = Email{}; // Reset
      current_email.content = line + "\n";
    } else {
      current_email.content += line + "\n";
    }
  }

  // Add the last email
  if (!current_email.content.empty()) {
    current_email.timestamp = extractEmailTimestamp(current_email.content);
    Email processed_email = extractAttachments(current_email.content);
    processed_email.timestamp = current_email.timestamp;  // Preserve timestamp
    emails.push_back(processed_email);
  }

  return emails;
}

// Function to create Maildir structure with attachments directory
void createMaildirStructure(const std::string& output_dir) {
  try {
    // Create main output directory if it doesn't exist
    fs::create_directories(output_dir);
    
    // Create Maildir subdirectories
    fs::create_directory(output_dir + "/cur");
    fs::create_directory(output_dir + "/new");
    fs::create_directory(output_dir + "/tmp");
    
    // Create attachments directory
    fs::create_directory(output_dir + "/attachments");
    
    std::cout << "Created Maildir structure with attachments directory in " << output_dir << std::endl;
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
  // Use .eml extension for mu compatibility (mu doesn't reliably support .gz files)
  std::string filename = unique_id.str() + ":2,S.eml";
  
  return filename;
}

// Function to check if file format is already compressed
bool isAlreadyCompressed(const std::string& filename, const std::string& content_type) {
  // Check by file extension
  std::string lower_filename = filename;
  std::transform(lower_filename.begin(), lower_filename.end(), lower_filename.begin(), ::tolower);
  
  // Image formats (already compressed)
  if (lower_filename.ends_with(".jpg") || lower_filename.ends_with(".jpeg") ||
      lower_filename.ends_with(".png") || lower_filename.ends_with(".gif") ||
      lower_filename.ends_with(".webp") || lower_filename.ends_with(".bmp")) {
    return true;
  }
  
  // Archive formats (already compressed)
  if (lower_filename.ends_with(".zip") || lower_filename.ends_with(".rar") ||
      lower_filename.ends_with(".7z") || lower_filename.ends_with(".gz") ||
      lower_filename.ends_with(".bz2") || lower_filename.ends_with(".xz")) {
    return true;
  }
  
  // Video/Audio formats (already compressed)
  if (lower_filename.ends_with(".mp4") || lower_filename.ends_with(".avi") ||
      lower_filename.ends_with(".mkv") || lower_filename.ends_with(".mp3") ||
      lower_filename.ends_with(".flac") || lower_filename.ends_with(".ogg")) {
    return true;
  }
  
  // Check by content type
  if (content_type.find("image/jpeg") != std::string::npos ||
      content_type.find("image/png") != std::string::npos ||
      content_type.find("image/gif") != std::string::npos ||
      content_type.find("image/webp") != std::string::npos) {
    return true;
  }
  
  if (content_type.find("application/zip") != std::string::npos ||
      content_type.find("application/x-zip") != std::string::npos ||
      content_type.find("application/gzip") != std::string::npos) {
    return true;
  }
  
  return false;
}

// Function to save attachments separately
void saveAttachments(const Email& email, const std::string& output_dir, int email_count) {
  for (size_t i = 0; i < email.attachments.size(); ++i) {
    const auto& attachment = email.attachments[i];
    
    // Create attachment filename: email_NNNNNN_attachment_N_filename
    std::ostringstream att_filename;
    att_filename << "email_" << std::setfill('0') << std::setw(9) << email_count 
                 << "_attachment_" << i << "_" << attachment.filename;
    
    std::string att_path = output_dir + "/attachments/" + att_filename.str();
    
    try {
      // Check if format is already compressed
      bool already_compressed = isAlreadyCompressed(attachment.filename, attachment.content_type);
      
      if (already_compressed) {
        // Save directly without compression
        std::ofstream att_file(att_path, std::ios::binary);
        if (!att_file) {
          throw std::runtime_error("Failed to create attachment file: " + att_path);
        }
        
        att_file.write(attachment.content.data(), attachment.content.size());
        if (!att_file) {
          throw std::runtime_error("Failed to write attachment data to: " + att_path);
        }
        
      } else {
        // Compress the attachment content
        std::string compressed_content = compressGzip(attachment.content);
        
        std::ofstream att_file(att_path + ".gz", std::ios::binary);
        if (!att_file) {
          throw std::runtime_error("Failed to create attachment file: " + att_path);
        }
        
        att_file.write(compressed_content.data(), compressed_content.size());
        if (!att_file) {
          throw std::runtime_error("Failed to write attachment data to: " + att_path);
        }
      }
      
    } catch (const std::exception& e) {
      std::cerr << "Error saving attachment " << i << " for email " << email_count 
                << ": " << e.what() << std::endl;
    }
  }
}

// Function to save an email to an uncompressed .eml file in Maildir cur directory
void saveEmail(const Email& email, const std::string& output_dir, int email_count) {
  std::string maildir_filename = generateMaildirFilename(email, email_count);
  std::string filename = output_dir + "/cur/" + maildir_filename;
  
  try {
    // Save the stripped email content
  std::ofstream outfile(filename);
    if (!outfile) {
      throw std::runtime_error("Failed to create output file: " + filename);
    }
    
  outfile << email.content;
    if (!outfile) {
      throw std::runtime_error("Failed to write email data to: " + filename);
    }
    
    // Save attachments separately if any exist
    if (!email.attachments.empty()) {
      saveAttachments(email, output_dir, email_count);
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
    std::cerr << "mbox2eml: Extract individual email messages from chunked mbox files and save them as separate .eml files in Maildir format." << std::endl;
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
