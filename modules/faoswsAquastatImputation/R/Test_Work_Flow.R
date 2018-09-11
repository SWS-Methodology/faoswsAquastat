
# LAOD LIBRARIES -------------------------------------------------------------------------------------------------------------------------
library("validate")
library("errorlocate")
library("dcmodify")
library('assertr')
library("tidyr")
library("stringr")
library(data.table)
library(dplyr)


# LOAD DATA -------------------------------------------------------------------------------------------------------------------------------
# Laoding data (this will be the Aquastat data pulled from SWS)
test_data <- read.csv("aqua_test.csv", stringsAsFactors = FALSE)

# Select relevant features
test_data <-test_data[, c("AreaName", "Year", "Item", "Value", "Symbol")]



# OBTAINING WIDE FORMAT DATA ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# load reference file with the indicator rules
prep_proc_rules <- as.character(read.csv("pre_proc_Aquastat_indicators_rules.txt", header = TRUE)$RULES)

# Extract all the elements and items that are used in rules
rule_elements_items <- unique(unlist(str_extract_all(prep_proc_rules, regex("(?<=\\[)[0-9]+(?=\\])"))))

# # catching the elements only (right hand side of equations)
# elements <- substr(prep_proc_rules, start = 1, stop = 6)
# elements <- unique(unlist(str_extract_all(elements, regex("(?<=\\[)[0-9]+(?=\\])"))))
# 
# # Extracting the items (left hand side)
# items <- setdiff(rule_elements_items, elements)
                 
# Replace brackets in rules to make them valid R code
proc_rules <- str_replace_all(prep_proc_rules, "\\[([0-9]+)\\]", "Value_\\1")

# convert data to data.table object
test_data <- data.table(test_data)

# select relevant items
test_data <- test_data[as.character(Item) %chin% rule_elements_items]

# Reshape data so that the elements are now columns
test_data_wide <- dcast(test_data, AreaName+ Year ~ Item, value.var = c("Value", "Symbol"), drop = FALSE)

# A last thing to be done is to create a 5-year label withinh each country.
# It will allow us to implement the priority rules rules established by Aquastat
 test_data_wide0 <- test_data_wide[, Window := as.integer(rep(, each = 5)), by = "AreaName"]



df %>% 
  group_by(V6, V5) %>%
  mutate(index = row_number(if (V5[1] == "+") V2 else desc(V2)))

result <- test_data_wide[, c("AreaName", "Year", "step1", "window")]
View(result)
?rep

# BEGINING OF PROCESSING -------------------------------------------------------------------------------------------------------------------------------
df <- data.frame(date = as.Date(c("2015-10-05","2015-10-08","2015-10-09",
                                  "2015-10-12","2015-10-14")),       
                 value = c(8,3,9,NA,5)



