library(validate)
library(dcmodify)
library(errorlocate)
library(tidyverse)
library(data.table)


# Load data and auxiliary tables
 d <- readr::read_csv("~./github/Aquastat/test_old_system.csv") # this data is supposed to come from GetData function in the SWS QA
# # df_annex1 <- read_csv("~./github/Aquastat/Annex1.csv")
d_imputed <- read_csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aquastat_test_imputed_all_TSEXP.csv")


# Definie rules according the Aquastat guidelines
rules_guidelines <- c("[4103]=[4101]+[4102]",
                      "[4105]=[4104]-[4106]",
                      "[4107]=[4104]/([4100]/100)",
                      "[4456]=[4160]+[4162]+[4168]+[4170]",
                      "[4108]=[4109]+[4110]",
                      "[4150]=[4100]*[4155] / 100000",
                      "[4157]=[4154]+[4155]-[4156]", 
                      "[4158]=[4157]*1000000/[4104]", 
                      "[4164]=[4160]+[4162]+[4168]",
                      "[4176]=[4160]+[4162]+[4168]-[4174]", 
                      "[4182]=[4176]+[4452]", 
                      "[4185]=[4176]+[4155]",
                      "[4187]=[4154]+[4452]",
                      "[4188]=[4185]+[4187]-[4156]",
                      "[4190]=[4188]*1000000/[4104]", 
                      "[4192]=100*([4164]+[4452])/([4164]+[4452]+[4157])",
                      "[4509]=[4193]+[4194]" , 
                      "[4196]=[4509]+[4195]",
                      "[4253]=[4251]+[4252]+[4250]",
                      "[4254]=[4250]/[4253]*100", 
                      "[4255]=[4251]/[4253]*100", 
                      "[4256]=[4252]/[4253]*100", 
                      "[4257]=[4253]*1000000/[4104]", 
                      "[4263]=[4253]-[4264]-[4265]-[4451]", 
                      "[4271]=100*[4260]/[4250]", 
                      "[4273]=100*[4250]/[4188]", 
                      "[4275]=100*[4263]/[4188]", 
                      "[4300]=[4303]+[4304]", 
                      "[4305]=100*[4300]/[4103]", 
                      "[4311]=[4308]+[4309]+[4310]", 
                      "[4313]=[4311]+[4312]+[4316]", 
                      "[4317]=[4313]+[4314]+[4315]", 
                      "[4319]=100*[4313]/[4317]",
                      "[4323]=100*[4320]/[4313]", 
                      "[4324]=100*[4321]/[4313]", 
                      "[4325]=100*[4322]/[4313]", 
                      "[4327]=100*[4326]/[4313]", 
                      "[4328]=100*[4318]/[4313]", 
                      "[4330]=100*[4313]/[4307]", 
                      "[4331]=100*[4313]/[4103]", 
                      "[4445]=100*[4400]/[4313]", 
                      "[4446]=100*[4303]/[4313]", 
                      "[4448]=[4314]+[4315]",
                      "[4450]=100*[4263]/[4157]",
                      "[4455]=100*[4454]/[4101]", 
                      "[4457]=[4251]*1000000/[4104]",
                      "[4458]=[4112]/[4104]/1000",
                      "[4459]=[4309]+[4310]", 
                      "[4462]=100*[4379]/[4461]", 
                      "[4463]=100*[4461]/[4311]",
                      "[4464]=100*[4379]/[4461]", 
                      "[4466]=100*[4465]/[4313]", 
                      "[4467]=100*[4263]/[4253]" , 
                      "[4468]=[4251]*1000000/[4106]",
                      "[4470]=100*[4103]/[4100]", 
                      "[4471]=1000000*[4197]/[4104]", 
                      "[4514]=100*[4513]/[4313]", 
                      "[4527]=100*[4526]/[4313]", 
                      "[4531]=[4252]*1000000/[4104]", 
                      "[4532]=[4250]*1000000/[4104]",
                      "[4538]=100*[4108]/[4449]", 
                      "[4540]=100*[4539]/[4313]", 
                      "[4550]=100*[4263]/([4188]-[4549])", 
                      "[4551]=([4552]*[4254])+([4553]*[4256])+([4554]*[4255])", 
                      "[4552]=(([4548]*[4555]/100)/[4250])/1000000000", 
                      "[4553]=([4546]/[4252])/1000000000", 
                      "[4554]=([4547]/[4251])/1000000000", 
                      "[4555]=1/(1+((1-([4556]/100))/(([4556]/100)*[4557])))",
                      "[4556]=100*[4379]/[4101]")

# Replace brackets in rules to make them valid R code
proc_rules_guide <- str_replace_all(rules_guidelines, "\\[([0-9]+)\\]", "Value_\\1")

# Extract all the elements that are used in rules
rule_elements_guide <- unique(unlist(str_extract_all(rules_guidelines, regex("(?<=\\[)[0-9]+(?=\\])"))))

# Indicator codes
aquastat_indicators_guide <- substring(proc_rules_guide, 7, 10)


# Calculation rules dataframe
rule = as.character(substring(proc_rules_guide, 12))
label = as.character(paste0("i", aquastat_indicators_guide))
idf <- data.frame(rule = rule, label = label)
i<- indicator(.data=idf)


# # CALCULATING WITHOUT IMPUTATION -------------------------------------------------------------------------------------------------------------------
# raw_long <- mutate(d, aquastatElement = paste0("Value_", aquastatElement)) %>% 
#   select(geographicAreaM49, timePointYears, aquastatElement, Value) %>% setDT()
# 
# # wide format
# raw_wide <- dcast(raw_long, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))
# 
# 
# # Indicators BEFORE imputation
# raw_wide$ID <- paste(row.names(raw_wide), raw_wide$geographicAreaM49, raw_wide$timePointYears, sep = "_")
# calculations <- confront(raw_wide , i, key = "ID")
# 
# # merging calculation and metadata
# out <- as.data.frame(calculations) %>% separate(ID, c("Row", "geographicAreaM49", "timePointYears"), sep = "_")
# origin(i) <- "raw data"
# measures1 <- as.data.frame(i)
# out_final <- merge(out, measures1) %>% tbl_df() %>%  rename(Value_raw = value) %>% mutate(label = substring(label, 2)) %>% select(geographicAreaM49, timePointYears, Value_raw,  label, description, origin, rule)
# 
# # Check if all indicator codes are in the final output
# # length(unique(out_final$label))
# # [1] 66 indicator could be calculated
# # setdiff(aquastat_indicators_guide, unique(out_final$label))
# # [1] "4456" "4527" "4540"
# # "4456"is missing in the raw data
# # "4527" a compontent of "4527 is missing in the raw data
# # "4540" a component of 4540 is missing in the raw data




# CALCULATION BASED ON IMPUTED DATASET -------------------------------------------------------------------------------------------------------------
# select variables
d_imputed_long <- d_imputed %>% mutate(aquastatElement = paste0("Value_", aquastatElement)) %>%  select(geographicAreaM49, timePointYears, aquastatElement, Value) %>% setDT()

# wide format
imputed_wide <- dcast(d_imputed_long, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))
#imputed_wide[is.na(imputed_wide)] <- 0

# Indicators AFTER imputation
imputed_wide$ID <- paste(row.names(imputed_wide), imputed_wide$geographicAreaM49, imputed_wide$timePointYears, sep = "_")
calculations_imp <- confront(imputed_wide , i, key = "ID")

# Adding metadata
out_imp <- as.data.frame(calculations_imp) %>% tidyr::separate(ID, c("Row", "geographicAreaM49", "timePointYears"), sep = "_")
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
  select(geographicAreaM49, timePointYears, aquastatElement, Value_raw, Value_imp,  rule) %>%  
  left_join(d_imputed %>% mutate(aquastatElement = as.character(aquastatElement)) %>% select(aquastatElement, definitions) %>% distinct())




# final_output <-
#     bind_rows(out_final %>% 
#                   mutate(timePointYears = as.integer(timePointYears),
#                          aquastatElement = label,
#                          Value = Value_raw) %>% 
#                   select(geographicAreaM49, timePointYears, aquastatElement,  Value, origin, rule),
#           
#               out_imp_final %>% 
#                   mutate(timePointYears = as.integer(timePointYears),
#                          aquastatElement = label,
#                          Value = Value_imp) %>% 
#                   select(geographicAreaM49, timePointYears, aquastatElement, Value, origin, rule)) %>% 
#             
# 
#     arrange(geographicAreaM49, aquastatElement) %>% 
#   
#   
#     left_join(d_imputed %>% mutate(aquastatElement = as.character(aquastatElement)) %>% select(aquastatElement, definitions) %>% distinct())
# 



# SAVE -----
write.csv(merged_df_guide, "~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/Comparing_approaches_TSEXPAN_ex2_FL.csv", row.names = FALSE)
# write.csv(final_output , "~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/Aquastat_final_output_GDS.csv", row.names = FALSE)

