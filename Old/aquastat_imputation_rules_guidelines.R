library(tidyverse)
library(data.table)
library(imputeTS)
library(listviewer)
library(tictoc)


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

# LOAD DATA ---- (it will be replaceb by the data taken from SWS in QA)
d <- readr::read_csv("~./github/Aquastat/test_old_system.csv") %>% setDT()

# Keep only elements mentioned in the rules so that we're working with a smaller set of data
d <- d[aquastatElement %in% rule_elements_guide]
d[, aquastatElement := factor(aquastatElement, levels = rule_elements_guide)]

# Indicators and variables are processed separately
dt <- d[,.(geographicAreaM49, timePointYears, aquastatElement, Value, flag_aquastat)]

# Get wide format dataset (variables as columns)
# For all elements
dt_wide <- dcast(setDT(dt), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value", "flag_aquastat"))
dt_wide_value <- dt_wide %>% select(geographicAreaM49, timePointYears, starts_with("Value_"))
# save it for later
# write.csv(dt_wide_value, "~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aqua_basedataset.csv", row.names = FALSE)

dt_wide_flag <- dt_wide %>% select(geographicAreaM49, timePointYears, starts_with("flag_"))


# Create a  list of dataframes which is the starting point for the subsettings
list_value <- split(as.data.frame(dt_wide_value), dt_wide_value$geographicAreaM49)
list_flag <- split(as.data.frame(dt_wide_flag), dt_wide_flag$geographicAreaM49)

# PREPARATION FOR IMPUTATION -----------
# Dataset: all missing value columns ----
AllMissingColumns <- function(df) as.vector(which(colSums(is.na(df)) == nrow(df))) 
dt_wide_allNAs_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns(.)) %>%  setDT())
dt_wide_allNAs_flags <-  lapply(list_flag, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns(.))  %>%  setDT())

# Dataset: zero variance with no missing values -----
ZeroVarianceCols_noNA <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & colSums(is.na(df)) == 0)) 
dt_wide_zerovar0_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.)) %>%  setDT())
dt_wide_zerovar0_flags <-  lapply(list_flag, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.))%>%  setDT())

# Dataset: zero variance and at least one NA ----
ZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar1_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())
dt_wide_zerovar1_flags <-  lapply(list_flag, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())

# Dataset: nonzero variance and at least one NA ----
NonZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) != 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar2_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT())
dt_wide_zerovar2_flags <-  lapply(list_flag, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT()) 


# IMPUTATION ACTION ---------
l1_values <- dt_wide_allNAs_values   # NO IMPUTATION
l2_values <- dt_wide_zerovar0_values  # NO IMPUTATION
l3_values <- dt_wide_zerovar1_values  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
l4_values <- dt_wide_zerovar2_values  # INTERPOLATION


# locf ----
l3_imputed_values <- lapply(l3_values, function(x) imputeTS::na.locf(x))

# linear interpolation ----
l4_imputed_values <- lapply(l4_values, function(x) imputeTS::na.interpolation(x))

# RECOMBINATION OF DATASETS -----
l_non_imputed_values <- Map(merge, l1_values, l2_values)
l_imputed_values <-  Map(merge, l3_imputed_values, l4_imputed_values) 
l_all_values <- Map(merge, l_non_imputed_values, l_imputed_values)

# GET LONG FORMAT (Four variables dataset) ----
l_long_all_values <- lapply(l_all_values, function(x) gather(x, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")))
long_all_values <- do.call("rbind", l_long_all_values) %>% tbl_df() 
row.names(long_all_values) <- NULL
long_all_values <- long_all_values %>% arrange(geographicAreaM49, aquastatElement)


# SAVE -----
write.csv(long_all_values, "~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aquastat_test_imputed_guide.csv", row.names = FALSE)
