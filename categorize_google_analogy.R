# get a better structured verion (by category) of the analogy dataset (questions_words.txt)

lines <- readLines(path_analogy)

# Identify category lines and extract categories
category_indices <- grep("^:", lines)
categories <- sub("^:", "", lines[category_indices])

# Ensure the category indices are properly matched
end_indices <- c(category_indices[-1] - 1, length(lines))

# Extract data using map2 while safely handling empty lines and NA values
data_list <- map2(category_indices, end_indices, ~ {
  section <- lines[(.x + 1):.y] %>% tolower()
  section <- section[!is.na(section) & section != ""]  # Remove NA and empty strings
  section
})

# Convert to a tidy data frame
df_analogy <- map2_dfr(categories, data_list, ~ tibble(category = .x, question = .y)) %>%
  separate(question, into = c("word1", "word2", "word3", "word4"), sep = " ", fill = "right")

# optional save
write.table(df_analogy, path_cleaned_analogy, sep = " ", row.names = FALSE, col.names = TRUE, quote = FALSE)