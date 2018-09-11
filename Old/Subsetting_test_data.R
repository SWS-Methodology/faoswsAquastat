library(tidyverse)
library(data.table)
library(imputeTS)
library(listviewer)
# library(zoo)


# DATA (it will be replaceb by the data taken from SWS in QA)
d <- readr::read_csv("~./github/Aquastat/test_old_system.csv")

# Define rules
rules <- c("[4103] = [4101] + [4102]",
           "[4105] = [4104] - [4106]", 
           "[4107]=[4104]/([4100]/100)", 
           "[4108]=[4109]+[4110]",
           "[4150] = [4100] * [4155] / 100000",
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
           "[4509]=	[4193]+[4194]" ,
           "[4196]=[4509]+[4195]", 
           "[4253]=[4251]+[4252]+[4250]", 
           "[4254]=[4250]/[4253]*100", 
           "[4255]=[4251]/[4253]*100", 
           "[4256]=[4252]/[4253]*100", 
           "[4257]=	[4253]*1000000/[4104]",
           "[4263]=[4253]-[4264]-[4265]-[4451]", 
           "[4271]=	100*[4260]/[4250]",
           "[4273]=100*[4250]/[4188]", 
           "[4275]=100*[4263]/[4188]", 
           "[4300]=[4303]+[4304]", 
           "[4305]=100*[4300]/[4103]", 
           "[4311]=[4308]+[4309]+[4310]",
           "[4313]=	[4311]+[4312]+[4316]",
           "[4317]=	[4313]+[4314]+[4315]", 
           "[4319]=100*[4313]/[4317]",
           "[4323]=	100*[4320]/[4313]", 
           "[4324]=100*[4321]/[4313]", 
           "[4325]=100*[4322]/[4313]", 
           "[4327]=100*[4326]/[4313]", 
           "[4328]=100*[4318]/[4313]", 
           "[4330]= 100*[4313]/[4307]", 
           "[4331]=100*[4313]/[4103]" , 
           "[4445]=100*[4400]/[4313]", 
           "[4446]=100*[4303]/[4313]", 
           "[4448]=[4314]+[4315]", 
           "[4450]=100*[4263]/[4157]", 
           "[4455]=100*[4454]/[4101]", 
           "[4456]=[4160]+[4162]+[4168]+[4170]", 
           "[4457]=[4251]*1000000/[4104]", 
           "[4458]=[4112]/[4104]/1000", 
           "[4459]=	[4309]+[4310]", 
           "[4462]=	100*[4379]/[4461]", 
           "[4463]=100*[4461]/[4311]", 
           "[4464]=100*[4379]/[4461]", 
           "[4466]=100*[4465]/[4313]", 
           "[4467]=100*[4263]/[4253]" , 
           "[4468]=[4251]*1000000/[4106]", 
           "[4470]=100*[4103]/[4100]", 
           "[4471]=1000000*[4197]/[4104]",
           "[4514]=100*[4513]/[4313]",
           "[4527]=100*[4526]/[4313]", 
           "[4531]=	[4252]*1000000/[4104]",
           "[4532]=[4250]*1000000/[4104]", 
           "[4538]=100*[4108]/[4449]", 
           "[4540]=100*[4539]/[4313]", 
           "[4550]=100*[4263]/([4188]-[4549])", 
           "[4551]=([4552]*[4254])+([4553]*[4256])+([4554]*[4255])", 
           "[4552]=	(([4548]*[4555]/100)/[4250])/1000000000", 
           "[4553]=([4546]/[4252])/1000000000", 
           "[4554]=	([4547]/[4251])/1000000000", 
           "[4555]=1/(1+((1-([4556]/100))/(([4556]/100)*[4557])))", 
           "[4556]=100*[4379]/[4101]")

# Replace brackets in rules to make them valid R code
proc_rules <- str_replace_all(rules, "\\[([0-9]+)\\]", "Value_\\1")

# Extract all the elements that are used in rules
rule_elements <- unique(unlist(str_extract_all(rules, regex("(?<=\\[)[0-9]+(?=\\])"))))

# sdg_elements <- c(4263, 4253, 4264, 4265, 4451, 4188, 45449, 4250, 4251, 4252, 4379, 4312, 4311, 4313, 4318, 4548, 4546, 4547)


dt <- d %>% filter(aquastatElement %in% rule_elements, timePointYears >= 1961) %>%  select(geographicAreaM49, timePointYears, aquastatElement, Value, flag_aquastat)


# Below the elements that ulrimately make up the releant indicators for the SDGs
# All of them are collected elements and not calculated
E4263_comp <- c(4251, 4252, 4250, 4265, 4451 )
E4188_comp <- c(4155, 4154, 4452 , 4156, 4160, 4162, 4168, 4174)
E4379_comp <- c(4379)
E4313_comp <- c(4308, 4309, 4310, 4312, 4316)
E4318_comp <- c(4318)
E4548_comp <- c(4548)
E4546_comp <- c(4546)
E4547_comp <- c(4547)
E4168_comp <- c(4168)
primary_comp <- c(E4263_comp,E4188_comp, E4379_comp, E4313_comp, E4318_comp,E4548_comp,E4546_comp,E4547_comp)

# Subset data with relevant elements
# dt_sdg <- dt %>%  filter(aquastatElement %in% primary_comp)

# Get wide format dataset (variables as columns)
dt_wide<- dcast(setDT(dt), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value", "flag_aquastat"))
dt_wide_value <- dt_wide %>% select(geographicAreaM49, timePointYears, starts_with("Value"))
dt_wide_flag <- dt_wide %>% select(geographicAreaM49, timePointYears, starts_with("flag"))


# Create a  list of dataframes which is the starting point for the subsettings
list_value_sdg <- split(as.data.frame(dt_wide_value ), dt_wide_value$geographicAreaM49)
list_flag_sdg <- split(as.data.frame(dt_wide_flag ), dt_wide_flag$geographicAreaM49)

# # Expanding time-series so every country has the same time-series length (from 1961 to 2015)
# list_value_sdg <- lapply(list_value_sdg, function(x) x %>%  tbl_df() %>% group_by(geographicAreaM49) %>% complete(timePointYears = seq(1961, 2015)) %>% ungroup())
# list_flag_sdg <- lapply(list_flag_sdg, function(x) x %>%  tbl_df() %>% group_by(geographicAreaM49) %>%complete(timePointYears = seq(1961, 2015)) %>%  ungroup())

# fake_df <- data.frame(A = rep("C1", 10),
#                       B = seq(2001, 2010),
#                       V1 = rep(3.444, 10),
#                       V2 = c(rep(NA, 5), rep(10, 5)),
#                       V3 = rep(5.2, 10))
# as.vector(which(colSums(is.na(fake_df)) == FALSE)) 


# ALL MISSING VALUE COLUMNS ----------------------------------------------------------------------
AllMissingColumns <- function(df) as.vector(which(colSums(is.na(df)) == nrow(df))) 
dt_wide_allNAs_values <- lapply(list_value_sdg, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns(.)) %>%  setDT())
dt_wide_allNAs_flags <-  lapply(list_flag_sdg, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns(.))  %>%  setDT())


# ZERO VARIANCE AND WITHOUT NAs -------------------------------------------------------------------------------------------------------------------
ZeroVarianceCols_noNA <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & colSums(is.na(df)) == 0)) 
dt_wide_zerovar0_values <- lapply(list_value_sdg, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.)) %>%  setDT())
dt_wide_zerovar0_flags <-  lapply(list_flag_sdg, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.))%>%  setDT())

# ZERO VARIANCE AND AT LEAST ONE NA --------------------------------------------------------------------------------------------------------------------------
ZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar1_values <- lapply(list_value_sdg, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())
dt_wide_zerovar1_flags <-  lapply(list_flag_sdg, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())

# NONZERO VARIANCE AND AT LEAST ONE NA -----------------------------------------------------------------------------------------------------------------------
NonZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) != 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar2_values <- lapply(list_value_sdg, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT())
dt_wide_zerovar2_flags <-  lapply(list_flag_sdg, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT()) 


 # IMPUTATION ACTION -----------------------------------------------------------------------------------------------------------------------------------------
 l1_values <- dt_wide_allNAs_values   # NO IMPUTATION
 l2_values <- dt_wide_zerovar0_values  # NO IMPUTATION
 l3_values <- dt_wide_zerovar1_values  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
 l4_values <- dt_wide_zerovar2_values  # INTERPOLATION
 

 # locf ----
 l3_imputed_values <- lapply(l3_values, function(x) imputeTS::na.locf(x))
# summary(l3_values) == summary(l3_imputed_values)
 

# linear interpolation ----
 l4_imputed_values <- lapply(l4_values, function(x) imputeTS::na.interpolation(x))
# summary(l4_values) == summary(l4_imputed_values)
 
# Recobination of the dataframes from the four lists
 l_non_imputed_values <- Map(merge, l1_values, l2_values)
 l_imputed_values <-  Map(merge, l3_imputed_values, l4_imputed_values) 
 l_all_values <- Map(merge, l_non_imputed_values, l_imputed_values)
 
# get long format data (foru variables geographicAreaM49, timePointYears, aquastatElement, Value)
l_long_all_values <- lapply(l_all_values, function(x) gather(x, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")))
long_all_values <- do.call("rbind", l_long_all_values)
row.names(long_all_values) <- NULL
# tbl_df(long_all_values)
# length(unique(long_all_values$aquastatElement))
# Notice that at this point the number of variables is equal to the number of variables at the begibning os the data processing (d)
# Howvwer, some of the variable could not be imputed because either the variable is not applicable for a given country or the variable
# is not available.
# From this point the analyst can:
# 1) Decide to save the long format data to use in the calculations
# 2) Filter the data so that only a specific set of variables of interest remains.


# Selecting relevant elements based on the primary components of indicators relevant to the AQUASTAT sdg indicators
to_select <- paste0("Value_", primary_comp)
long_all_filtered0 <- dplyr::filter(long_all_values, is.na(Value))
long_all_filteredNA <- dplyr::filter(long_all_values, is.na(Value))





# WORK ON FLAGS ----------------------------------------------------------------------------------------------------------------------------------------------------
l1_flags <- dt_wide_allNAs_flags   # NO IMPUTATION
l2_flags <- dt_wide_zerovar0_flags  # NO IMPUTATION
l3_flags <- dt_wide_zerovar1_flags  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
l4_flags <- dt_wide_zerovar2_flags  # INTERPOLATION



#  Assuming that all NAs in the flag dataset need to be replaced by an appropriate flag,
# we could use the modify function from dcmodify pack to set the right flags for each variable

modifier_1 <- modifier(
  
)







# Wide format
# The NAs at the country-leve are there because the whole time-series was empty at the raw data stage.
# Therefore, it is not a problem to convert them to zero before the calculations.
df_wide <- dcast(long_all_filtered0 , geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))



c("Value_4188 = ((Value_4160 + Value_4162 + Value_4168 - Value_4174) + Value_4155) + (Value_4154 + Value_4452) - Value_4156",
  "Value_4253 = Value_4251 +   Value_4252 + Value_4250",
  "Value_4164 = Value_4160 + Value_4162 + Value_4168") 