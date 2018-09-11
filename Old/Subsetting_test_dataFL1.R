library(tidyverse)
library(data.table)
library(imputeTS)
library(listviewer)


# DEFINE RULES -----
# take it all the 4170 "[4456]=[4160]+[4162]+[4168]+[4170]",
rules <- c("[4103]=[4101] + [4102]", "[4105]=[4104] - [4106]", "[4107]=[4104]/([4100]/100)", "[4456]=[4160]+[4162]+[4168]+[4170]", "[4108]=[4109]+[4110]", "[4150]=[4100] * [4155] / 100000", "[4157]=[4154]+[4155]-[4156]", "[4158]=[4157]*1000000/[4104]", "[4164]=[4160]+[4162]+[4168]", "[4176]=[4160]+[4162]+[4168]-[4174]", "[4182]=[4176]+[4452]", "[4185]=[4176]+[4155]", "[4187]=[4154]+[4452]", "[4188]=[4185]+[4187]-[4156]", "[4190]=[4188]*1000000/[4104]", "[4192]=100*([4164]+[4452])/([4164]+[4452]+[4157])", "[4509]=[4193]+[4194]" , "[4196]=[4509]+[4195]", "[4253]=[4251]+[4252]+[4250]", "[4254]=[4250]/[4253]*100", "[4255]=[4251]/[4253]*100", "[4256]=[4252]/[4253]*100", "[4257]=[4253]*1000000/[4104]", "[4263]=[4253]-[4264]-[4265]-[4451]", "[4271]=100*[4260]/[4250]", "[4273]=100*[4250]/[4188]", "[4275]=100*[4263]/[4188]", "[4300]=[4303]+[4304]", "[4305]=100*[4300]/[4103]", "[4311]=[4308]+[4309]+[4310]", "[4313]=	[4311]+[4312]+[4316]", "[4317]=	[4313]+[4314]+[4315]", "[4319]=100*[4313]/[4317]", "[4323]=100*[4320]/[4313]", "[4324]=100*[4321]/[4313]", "[4325]=100*[4322]/[4313]", "[4327]=100*[4326]/[4313]", "[4328]=100*[4318]/[4313]", "[4330]= 100*[4313]/[4307]", "[4331]=100*[4313]/[4103]" , "[4445]=100*[4400]/[4313]", "[4446]=100*[4303]/[4313]", "[4448]=[4314]+[4315]", "[4450]=100*[4263]/[4157]", "[4455]=100*[4454]/[4101]",  "[4457]=[4251]*1000000/[4104]", "[4458]=[4112]/[4104]/1000", "[4459]=[4309]+[4310]", "[4462]=	100*[4379]/[4461]", "[4463]=100*[4461]/[4311]", "[4464]=100*[4379]/[4461]", "[4466]=100*[4465]/[4313]", "[4467]=100*[4263]/[4253]" , "[4468]=[4251]*1000000/[4106]", "[4470]=100*[4103]/[4100]", "[4471]=1000000*[4197]/[4104]", "[4514]=100*[4513]/[4313]", "[4527]=100*[4526]/[4313]", "[4531]=	[4252]*1000000/[4104]", "[4532]=[4250]*1000000/[4104]", "[4538]=100*[4108]/[4449]", "[4540]=100*[4539]/[4313]", "[4550]=100*[4263]/([4188]-[4549])", "[4551]=([4552]*[4254])+([4553]*[4256])+([4554]*[4255])", "[4552]=	(([4548]*[4555]/100)/[4250])/1000000000", "[4553]=([4546]/[4252])/1000000000", "[4554]=	([4547]/[4251])/1000000000", "[4555]=1/(1+((1-([4556]/100))/(([4556]/100)*[4557])))", "[4556]=100*[4379]/[4101]")

# Replace brackets in rules to make them valid R code
proc_rules <- str_replace_all(rules, "\\[([0-9]+)\\]", "Value_\\1")

# Extract all the elements that are used in rules
rule_elements <- unique(unlist(str_extract_all(rules, regex("(?<=\\[)[0-9]+(?=\\])"))))

# LOAD DATA ---- (it will be replaceb by the data taken from SWS in QA)
d <- readr::read_csv("~./github/Aquastat/test_old_system.csv") %>% setDT()

# Keep only elements mentioned in the rules so that we're working with a smaller set of data
d <- d[aquastatElement %in% rule_elements]
d[, aquastatElement := factor(aquastatElement, levels = rule_elements)]


# getting the indicators/calculated variables
aquastat_indicators <- substring(proc_rules, first = 7, last = 10)

# getting the variables/componenents 
aquastat_variables <- setdiff(rule_elements, aquastat_indicators) 


# Indicators and variables are processed separately
dt <- d[,.(geographicAreaM49, timePointYears, aquastatElement, Value, flag_aquastat)]
dt_indicators <- d[aquastatElement %in% aquastat_indicators, .(geographicAreaM49, timePointYears, aquastatElement, Value, flag_aquastat)]
dt_variables <- d[aquastatElement %in% aquastat_variables, .(geographicAreaM49, timePointYears, aquastatElement, Value, flag_aquastat)]




# unique(dt$aquastatElement)
# unique(d)

# Below the elements that ulrimately make up the releant indicators for the SDGs
# All of them are collected elements and not calculated
# E4263_comp <- c(4251, 4252, 4250, 4265, 4451 )
# E4188_comp <- c(4155, 4154, 4452 , 4156, 4160, 4162, 4168, 4174)
# E4379_comp <- c(4379)
# E4313_comp <- c(4308, 4309, 4310, 4312, 4316)
# E4318_comp <- c(4318)
# E4548_comp <- c(4548)
# E4546_comp <- c(4546)
# E4547_comp <- c(4547)
# E4168_comp <- c(4168)
# primary_comp <- c(E4263_comp,E4188_comp, E4379_comp, E4313_comp, E4318_comp,E4548_comp,E4546_comp,E4547_comp)
# Subset data with relevant elements
# dt_sdg <- dt %>%  filter(aquastatElement %in% primary_comp)

# Get wide format dataset (variables as columns)
# For all elements
dt_wide <- dcast(setDT(dt), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value", "flag_aquastat"))
dt_wide_value <- dt_wide %>% select(geographicAreaM49, timePointYears, starts_with("Value_"))
dt_wide_flag <- dt_wide %>% select(geographicAreaM49, timePointYears, starts_with("flag_"))


# For indicators only
dt_wide_ind <- dcast(setDT(dt_indicators), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value", "flag_aquastat"), drop = FALSE)
dt_wide_ind_value <- dt_wide_ind %>% select(geographicAreaM49, timePointYears, starts_with("Value_"))
dt_wide_ind_flag <- dt_wide_ind %>% select(geographicAreaM49, timePointYears, starts_with("flag_"))

# For variables only
dt_wide_var<- dcast(setDT(dt_variables), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value", "flag_aquastat"), drop = FALSE)
dt_wide_var_value <- dt_wide_var %>% select(geographicAreaM49, timePointYears, starts_with("Value_"))
dt_wide_var_flag <- dt_wide_var %>% select(geographicAreaM49, timePointYears, starts_with("flag_"))

# Create a  list of dataframes which is the starting point for the subsettings
list_value_sdg <- split(as.data.frame(dt_wide_value ), dt_wide_value$geographicAreaM49)
list_flag_sdg <- split(as.data.frame(dt_wide_flag ), dt_wide_flag$geographicAreaM49)

# # Expanding time-series so every country has the same time-series length (from 1961 to 2015)
list_value_sdg <- map(list_value_sdg, ~ .x  %>% group_by(geographicAreaM49) %>% complete(timePointYears = seq(1961, 2015)) %>% ungroup())
list_flag_sdg <- map(list_flag_sdg, ~ .x %>%  group_by(geographicAreaM49) %>%complete(timePointYears = seq(1961, 2015)) %>%  ungroup())

list_value_sdg, ~ .x %>% map_at(Value_)
fake_df <- data.frame(geographicAreaM49 = rep("CountryA", 55),
                      Year = as.integer(seq(1961, 2015,1)),
                      Value_1 = as.numeric(rep(NA, 55)),
                      Value_2 = c(rep(NA, 45), runif(10)),
                      Value_3 = c(rep(10, 50), rep(NA, 5)),
                      Value_4 = runif(55, 0, 100),
                      Value_5 = c(NA, rep(54, 10)))

# str(fake_df)
# fake_df[3:ncol(fake_df)]

# ALL MISSING VALUE COLUMNS ----------------------------------------------------------------------
AllMissingColumns <- function(df) as.vector(which(colSums(is.na(df[3:ncol(df)])) == nrow(df))) 
dt_wide_allNAs_values <- map(list_value_sdg, ~ .x %>% select(geographicAreaM49, timePointYears, AllMissingColumns(.)) %>%  setDT())
dt_wide_allNAs_flags <-  map(list_flag_sdg, ~ .x %>%  select(geographicAreaM49, timePointYears, AllMissingColumns(.))  %>%  setDT())

# ZERO VARIANCE AND WITHOUT NAs -------------------------------------------------------------------------------------------------------------------
ZeroVarianceCols_noNA <- function(df) as.vector(which(apply(df[3:ncol(df)], 2, var, na.rm = TRUE) == 0 & colSums(is.na(df[3:ncol(df)])) == 0)) 
dt_wide_zerovar0_values <- map(list_value_sdg, ~ .x %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.)) %>%  setDT())
dt_wide_zerovar0_flags <-  map(list_flag_sdg, ~ .x %>%  select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.))%>%  setDT())

# ZERO VARIANCE AND AT LEAST ONE NA --------------------------------------------------------------------------------------------------------------------------
ZeroVarianceCols <- function(df) as.vector(which(apply(df[3:ncol(df)], 2, var, na.rm = TRUE) == 0 & apply(df[3:ncol(df)], 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar1_values <- map(list_value_sdg, ~.x %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())
dt_wide_zerovar1_flags <-  map(list_flag_sdg, ~.x %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())

# NONZERO VARIANCE AND AT LEAST ONE NA -----------------------------------------------------------------------------------------------------------------------
NonZeroVarianceCols <- function(df) as.vector(which(apply(df[3:ncol(df)], 2, var, na.rm = TRUE) != 0 & apply(df[3:ncol(df)], 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar2_values <- map(list_value_sdg, ~.x %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT())
dt_wide_zerovar2_flags <-  map(list_flag_sdg, ~.x %>%  select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT()) 

summary(l4)
 # IMPUTATION ACTION -----------------------------------------------------------------------------------------------------------------------------------------
 l1 <- dt_wide_allNAs_values   # NO IMPUTATION
 l2 <- dt_wide_zerovar0_values  # NO IMPUTATION
 l3 <- dt_wide_zerovar1_values  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
 l4 <- dt_wide_zerovar2_values  # NO IMPUTATION
 
l3[[3]]
 # locf ----
 l3_imputed <- map(l3, ~.x %>% data.table %>%  imputeTS::na.locf(.))

 # linear interpolation ----
 l4_imputed <- lapply(l4, function(x) imputeTS::na.interpolation(x))
  
 
# Concatenating dataframes from the four lists
 l_non_imputed <- Map(merge, l1, l2)
 l_imputed <-  Map(merge, l3_imputed, l4_imputed) 
 l_all <- Map(merge, l_non_imputed, l_imputed)
 

# Select all the relevant variables
l_long_all <- lapply(l_all, function(x) gather(x, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")))
long_all <- do.call("rbind", l_long_all)
row.names(long_all) <- NULL

# filtering out the NAs
to_select <- paste0("Value_", primary_comp)
long_all_filtered0 <- dplyr::filter(long_all, aquastatElement %in% to_select & !is.na(Value))

# Wide format 
df_wide <- dcast(long_all_filtered0 , geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))



c("Value_4188 = ((Value_4160 + Value_4162 + Value_4168 - Value_4174) + Value_4155) + (Value_4154 + Value_4452) - Value_4156",
  "Value_4253 = Value_4251 +   Value_4252 + Value_4250",
  "Value_4164 = Value_4160 + Value_4162 + Value_4168") 