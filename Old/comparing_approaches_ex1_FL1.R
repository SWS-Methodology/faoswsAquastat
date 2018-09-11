library(validate)
library(dcmodify)
library(errorlocate)
library(tidyverse)
library(data.table)

d <- readr::read_csv("~./github/Aquastat/test_old_system.csv") 
df_definitions <- read_csv("~./github/Aquastat/Aquastat_Definitions.csv")
df_annex1 <- read_csv("~./github/Aquastat/Annex1.csv")
d_imputed <- read_csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aquastat_test_imputed_guide.csv")


# Definie rules according the Aquastat guidelines
rules_guidelines <- c( "[4103] = [4101]+[4102]" 
                      ,"[4105] = [4104]-[4106]" 
                      ,"[4107] = [4104]/([4100]/100) "
                      ,"[4108] = [4109]+[4110]"
                      ,"[4150] = [4100]*[4151]/100000"
                      ,"[4157] = [4154]+[4155]-[4156]"
                      ,"[4158] = [4157]*1000000/[4104]"
                      ,"[4164] = [4160]+[4162]+[4168]"
                      ,"[4176] = [4160]+[4162]+[4168]-[4174]"
                      ,"[4182] = [4176]+[4452]"
                      ,"[4185] = [4176]+[4155]"
                      ,"[4187] = [4154]+[4452]"
                      ,"[4188] = [4185]+[4187]-[4156]"
                      ,"[4190] = [4188]*1000000/[4104]"
                      ,"[4192] = 100*([4164]+[4452])/([4164]+[4452]+[4157])"
                      ,"[4196] = [4509]+[4195]"
                      ,"[4253] = [4251]+[4252]+[4250]"
                      ,"[4254] = [4250]/[4253]*100"
                      ,"[4255] = [4251]/[4253]*100"
                      ,"[4256] = [4252]/[4253]*100"
                      ,"[4257] = [4253]*1000000/[4104]"
                      ,"[4263] = [4253]-[4264]-[4265]-[4451]"
                      ,"[4271] = 100*[4260]/[4250]"
                      ,"[4273] = 100*[4250]/[4188]"
                      ,"[4275] = 100*[4263]/[4188]"
                      ,"[4300] = [4303]+[4304]"
                      ,"[4305] = 100*[4300]/[4103] "
                      ,"[4311] = [4308]+[4309]+[4310]"
                      ,"[4313] = [4311]+[4312]+[4316]"
                      ,"[4317] = [4313]+[4314]+[4315]"
                      ,"[4319] = 100*[4313]/[4317]"
                      ,"[4323] = 100*[4320]/[4313]"
                      ,"[4324] = 100*[4321]/[4313]"
                      ,"[4325] = 100*[4322]/[4313]"
                      ,"[4327] = 100*[4326]/[4313]"
                      ,"[4328] = 100*[4318]/[4313]"
                      ,"[4330] = 100*[4313]/[4307]"
                      ,"[4331] = 100*[4313]/[4103]"
                      ,"[4445] = 100*[4400]/[4313]"
                      ,"[4446] = 100*[4303]/[4313]"
                      ,"[4448] = [4314]+[4315]"
                      ,"[4450] = 100*[4263]/[4157]"
                      ,"[4455] = 100*[4454]/[4101]"
                      ,"[4456] = [4160]+[4162]+[4168]+[4170]"
                      ,"[4457] = [4251]*1000000/[4104]"
                      ,"[4458] = [4112]/[4104]/1000"
                      ,"[4459] = [4309]+[4310]"
                      ,"[4462] = 100*[4379]/[4461]"
                      ,"[4463] = 100*[4461]/[4311]"
                      ,"[4464] = 100*[4379]/[4461]"
                      ,"[4466] = 100*[4465]/[4313]"
                      ,"[4467] = 100*[4263]/[4253]"
                      ,"[4468] = [4251]*1000000/[4106]"
                      ,"[4470] = 100*[4103]/[4100]"
                      ,"[4471] = 1000000*[4197]/[4104]"
                      ,"[4509] = [4193]+[4194]"
                      ,"[4514] = 100*[4513]/[4313]"
                      ,"[4527] = 100*[4526]/[4313]"
                      ,"[4531] = [4252]*1000000/[4104]"
                      ,"[4532] = [4250]*1000000/[4104]"
                      ,"[4533] = [4465]*1"
                      ,"[4534] = [4513]*1"
                      ,"[4535] = [4265]*1"
                      ,"[4536] = [4156]*1"
                      ,"[4538] = 100*[4108]/[4449]"
                      ,"[4540] = 100*[4539]/[4313]")

# Replace brackets in rules to make them valid R code
proc_rules_guide <- str_replace_all(rules_guidelines, "\\[([0-9]+)\\]", "Value_\\1")

# Extract all the elements that are used in rules
rule_elements_guide <- unique(unlist(str_extract_all(rules_guidelines, regex("(?<=\\[)[0-9]+(?=\\])"))))

# Indicator codes
aquastat_indicators_guide <- substring(proc_rules_guide, 7, 10)

# Calculation rules dataframe
rule = as.character(substring(proc_rules_guide, 14))
label = as.character(paste0("i", aquastat_indicators_guide))
description = as.vector(unique(filter(df_annex1, Element %in% aquastat_indicators_guide)$ElementName))
origin = as.character(rep("Aquastat Working System", length(rule)))
idf <- data.frame(rule = rule, label = label, description = description)
i<- indicator(.data=idf)


# CALCULATING WITHOUT IMPUTATION ------
raw_long <- mutate(d, aquastatElement = paste0("Value_", aquastatElement)) %>% 
  select(geographicAreaM49, timePointYears, aquastatElement, Value) %>% setDT()

# wide format
raw_wide <- dcast(raw_long, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))

# Indicators BEFORE imputation
raw_wide$ID <- paste(row.names(raw_wide), raw_wide$geographicAreaM49, raw_wide$timePointYears, sep = "_")
calculations <- confront(raw_wide , i, key = "ID")
# Object of class 'indication'
# Call:
#   confront(dat = raw_wide, x = i, key = "ID")
# 
# Confrontations: 66 could be coulculated
# Warnings      : 0
# Errors        : 3 indicators couldnt be calculated 


# merging calculation and metadata
out <- as.data.frame(calculations) %>% separate(ID, c("Row", "geographicAreaM49", "timePointYears"), sep = "_")
origin(i) <- "raw data"
measures1 <- as.data.frame(i)
out_final <- merge(out, measures1) %>% tbl_df() %>%  rename(Value_raw = value) %>% mutate(label = substring(label, 2)) %>% select(geographicAreaM49, timePointYears, Value_raw,  label, description, origin, rule)

# Check if all indicator codes are in the final output
#length(unique(out_final$label))
# [1] 63 indicator could be calculated
#setdiff(aquastat_indicators_guide, unique(out_final$label))
# [1] "4456" "4527" "4540"
# "4456"is imissing in the raw data
# "4539" a compontent of "4540 is missing in the raw data
# "4526" a component of 4527 is missing in the raw data




# CALCULATION BASED ON IMPUTED DATASET -----------------------------
# Load imputed dataset
d_imputed

# select variables
d_imputed_long <- d_imputed %>%  select(geographicAreaM49, timePointYears, aquastatElement, Value) %>% setDT()

# wide format
imputed_wide <- dcast(d_imputed_long, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))

# Indicators AFTER imputation
imputed_wide$ID <- paste(row.names(imputed_wide), imputed_wide$geographicAreaM49, imputed_wide$timePointYears, sep = "_")
calculations_imp <- confront(imputed_wide , i, key = "ID")
# Object of class 'indication'
# Call:
#   confront(dat = imputed_wide, x = i, key = "ID")
# 
# Confrontations: 66
# Warnings      : 0
# Errors        : 3


# Adding metadata
out_imp <- as.data.frame(calculations_imp) %>% separate(ID, c("Row", "geographicAreaM49", "timePointYears"), sep = "_")
origin(i) <- "imputed data"
measures1 <- as.data.frame(i)
out_imp_final <- merge(out_imp, measures1) %>% tbl_df() %>%   rename(Value_imp = value) %>% mutate(label = substring(label, 2)) %>% select(geographicAreaM49, timePointYears, Value_imp,  label, description, origin, rule)

# Check if all indicator codes are in the final output
#length(unique(out_imp_final$label))
# [1] 63 indicator could be calculated
#setdiff(aquastat_indicators_guide, unique(out_imp_final$label))
# [1] "4456" "4527" "4540"
# "4456"is imissing in the raw data
# "4539" a compontent of "4540 is missing in the raw data
# "4526" a component of 4527 is missing in the raw data



# BINDING BEFORE AND AFTER IMPUTATION OUTPUTS 
# Merging datasest
merged_df_guide <- 
  left_join(out_imp_final %>%  select(-origin),
            out_final %>%  select(-origin),
            by = c("geographicAreaM49", "timePointYears", "label", "description", "rule")) %>% 
  rename(aquastatElement = label) %>% 
  select(geographicAreaM49, timePointYears, aquastatElement, description, Value_raw, Value_imp,  rule)



#setdiff(out_final$timePointYears, out_imp_final$timePointYears)
#[1] "1964" "1969" "2017" "1968" "1973" "1978" "1966"
# The year below are not available in the imputed dataset.
# These years contain the elements 4472 (NRI) and 4549 (Envir. Flow requ.) which are not available in the calculation rules of the guidelines
# that is why the imputed dataset (out_imp_final has less rows than the original data without imputation)
# names(raw_wide)
# names(imputed_wide)
# allelements <- 
#   left_join(raw_wide %>% mutate(label = "raw"),imputed_wide %>%  mutate(label = "imputed")) %>%  arrange(geographicAreaM49) %>%  tbl_df()
# SAVE -----
write.csv(merged_df_guide, "~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/Comparing_approaches_ex1.csv", row.names = FALSE)
