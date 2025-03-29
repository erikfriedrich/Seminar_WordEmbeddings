library(data.table)

bats_dir <- "/Users/erikfriedrich/Desktop/Studium/Seminararbeit/testing/BATS_3.0"

read_file <- function(file_path) {
  dt <- fread(file_path, header = FALSE, stringsAsFactors = FALSE)
  
  # Ensure there are 2 columns
  if (ncol(dt) < 2) {
    warning("File does not have 2 columns: ", file_path)
    return(NULL)
  }
  
  # Rename columns
  colnames(dt)[1:2] <- c("A", "B")
  
  return(dt)
}

final_dt <- data.table()
main_folders <- list.dirs(bats_dir, recursive = FALSE)

# Print total number of folders to process
cat("Processing", length(main_folders), "main folders\n")

for (main_idx in seq_along(main_folders)) {
  main <- main_folders[main_idx]
  main_name <- basename(main)
  
  # Print current folder progress
  cat("\nProcessing folder", main_idx, "of", length(main_folders), ":", main_name, "\n")
  
  sub_files <- list.files(main, full.names = TRUE, pattern = "\\.txt$")
  
  # Print number of files in this folder
  cat("Found", length(sub_files), "text files\n")
  
  for (file_idx in seq_along(sub_files)) {
    sub_file <- sub_files[file_idx]
    sub_name <- basename(sub_file)
    
    # Print current file progress (every 10th file to avoid too much output)
    cat("  Processing file", file_idx, "of", length(sub_files), ":", sub_name, "\n")
    
    # Read the file
    dt <- read_file(sub_file)
    
    # Skip if file couldn't be processed
    if (is.null(dt) || nrow(dt) < 2) {
      cat("  Skipping file", sub_name, "(insufficient data)\n")
      next
    }
    
    # Generate all pairwise combinations within the same file
    combinations <- data.table()
    for (i in 1:nrow(dt)) {
      for (j in 1:nrow(dt)) {
        if (i != j) {  # Avoid self-pairing
          combinations <- rbind(combinations, data.table(
            main = main_name,
            sub = sub_name,
            A = dt$A[i],
            B = dt$B[i],
            C = dt$A[j],
            D = dt$B[j]
          ))
        }
      }
    }
    
    # Add to final result
    final_dt <- rbind(final_dt, combinations)
  }
}

# Print final statistics
cat("\nProcessing complete!\n")
cat("Total combinations created:", nrow(final_dt), "\n")
cat("Number of folders processed:", length(main_folders), "\n")

# View a sample of final result
cat("\nSample of final data:\n")
print(head(final_dt, 10))


output_file <- "/Users/erikfriedrich/Desktop/Studium/Seminararbeit/testing/bats_combinations.csv"
fwrite(final_dt, output_file)
cat("Results saved to", output_file, "\n")
