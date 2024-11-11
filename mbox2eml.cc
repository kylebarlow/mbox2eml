//////////////////////////////////////////////////////
/////
/////////////////////////////////////////////////////
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <filesystem>

// Compile with  g++ -O3 -std=c++23 -pthread -lstdc++fs -o mbox2eml mbox2eml.cc 

namespace fs = std::filesystem;

// Structure to hold email data
struct Email {
  std::string content;
};

// Function to extract individual emails from the mbox file
std::vector<Email> extractEmails(const std::string& mbox_file) {
  std::vector<Email> emails;
  std::ifstream file(mbox_file);
  std::string line;
  Email current_email;

  while (std::getline(file, line)) {
    if (line.starts_with("From ")) {
      // Start of a new email
      if (!current_email.content.empty()) {
        emails.push_back(current_email);
      }
      current_email.content = line + "\n";
    } else {
      current_email.content += line + "\n";
    }
  }

  // Add the last email
  if (!current_email.content.empty()) {
    emails.push_back(current_email);
  }

  return emails;
}

// Function to save an email to an eml file
void saveEmail(const Email& email, const std::string& output_dir, int email_count) {
  std::string filename = output_dir + "/email_" + std::to_string(email_count) + ".eml";
  std::ofstream outfile(filename);
  outfile << email.content;
}

// Worker thread function to process emails
void workerThread(const std::vector<Email>& emails, const std::string& output_dir, 
                  int start_index, int end_index, std::mutex& output_mutex) {
  for (int i = start_index; i < end_index; ++i) {
    {
      std::lock_guard<std::mutex> lock(output_mutex);
      saveEmail(emails[i], output_dir, i + 1);
      std::cout << "Saved email_" << i + 1 << ".eml" << std::endl;
    }
  }
}

int main(int argc, char* argv[]) {
  // Check for correct number of arguments
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <mbox_file> <output_directory>" << std::endl;
    return 1;
  }

  std::string mbox_file = argv[1];
  std::string output_dir = argv[2];

  // Create the output directory if it doesn't exist
  fs::create_directory(output_dir);

  // Extract emails from the mbox file
  std::vector<Email> emails = extractEmails(mbox_file);
  std::cout << "Extracted " << emails.size() << " emails." << std::endl;

  // Determine the number of threads to use (e.g., based on CPU cores)
  int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    num_threads = 2; // Default to 2 threads if hardware concurrency is unknown
  }

  // Calculate the number of emails per thread
  int emails_per_thread = emails.size() / num_threads;
  int remaining_emails = emails.size() % num_threads;

  // Create and launch worker threads
  std::vector<std::thread> threads;
  std::mutex output_mutex;
  int start_index = 0;

  for (int i = 0; i < num_threads; ++i) {
    int end_index = start_index + emails_per_thread;
    if (i < remaining_emails) {
      end_index++;
    }
    threads.emplace_back(workerThread, std::ref(emails), output_dir, start_index, end_index, 
                         std::ref(output_mutex));
    start_index = end_index;
  }

  // Wait for all threads to finish
  for (auto& thread : threads) {
    thread.join();
  }

  std::cout << "Finished processing all emails." << std::endl;
  return 0;
}
