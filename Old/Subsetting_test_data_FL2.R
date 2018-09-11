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
list_value <- split(as.data.frame(dt_wide_value), dt_wide_value$geographicAreaM49)
list_flag <- split(as.data.frame(dt_wide_flag), dt_wide_flag$geographicAreaM49)

list_value_ind <- split(as.data.frame(dt_wide_ind_value ), dt_wide_ind_value$geographicAreaM49)
list_flag_ind <- split(as.data.frame(dt_wide_ind_flag ), dt_wide_ind_flag$geographicAreaM49)

list_value_var <- split(as.data.frame(dt_wide_var_value ), dt_wide_var_value$geographicAreaM49)
list_flag_var <- split(as.data.frame(dt_wide_var_flag ), dt_wide_var_flag$geographicAreaM49)

# PREPARATION FOR IMPUTATION -----------
# Dataset: all missing value columns ----
AllMissingColumns <- function(df) as.vector(which(colSums(is.na(df)) == nrow(df))) 
dt_wide_allNAs_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns(.)) %>%  setDT())
dt_wide_allNAs_flags <-  lapply(list_flag, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns(.))  %>%  setDT())

AllMissingColumns_ind <- function(df) as.vector(which(colSums(is.na(df)) == nrow(df))) 
dt_wide_allNAs_ind_values <- lapply(list_value_ind, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns_ind(.)) %>%  setDT())
dt_wide_allNAs_ind_flags <-  lapply(list_flag_ind, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns_ind(.))  %>%  setDT())

AllMissingColumns_var <- function(df) as.vector(which(colSums(is.na(df)) == nrow(df))) 
dt_wide_allNAs_var_values <- lapply(list_value_var, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns_var(.)) %>%  setDT())
dt_wide_allNAs_var_flags <-  lapply(list_flag_var, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns_var(.))  %>%  setDT())


# Dataset: zero variance with no missing values -----
ZeroVarianceCols_noNA <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & colSums(is.na(df)) == 0)) 
dt_wide_zerovar0_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.)) %>%  setDT())
dt_wide_zerovar0_flags <-  lapply(list_flag, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.))%>%  setDT())

ZeroVarianceCols_noNA_ind <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & colSums(is.na(df)) == 0)) 
dt_wide_zerovar0_ind_values <- lapply(list_value_ind, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA_ind(.)) %>%  setDT())
dt_wide_zerovar0_ind_flags <-  lapply(list_flag_ind, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA_ind(.))%>%  setDT())

ZeroVarianceCols_noNA_var <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & colSums(is.na(df)) == 0)) 
dt_wide_zerovar0_var_values <- lapply(list_value_var, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA_var(.)) %>%  setDT())
dt_wide_zerovar0_var_flags <-  lapply(list_flag_var, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA_var(.))%>%  setDT())


# Dataset: zero variance and at least one NA ----
ZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar1_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())
dt_wide_zerovar1_flags <-  lapply(list_flag, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())

ZeroVarianceCols_ind <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar1_ind_values <- lapply(list_value_ind, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_ind(.)) %>%  setDT())
dt_wide_zerovar1_ind_flags <-  lapply(list_flag_ind, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_ind(.)) %>%  setDT())

ZeroVarianceCols_var <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar1_var_values <- lapply(list_value_var, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_var(.)) %>%  setDT())
dt_wide_zerovar1_var_flags <-  lapply(list_flag_var, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_var(.)) %>%  setDT())


# Dataset: nonzero variance and at least one NA ----
NonZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) != 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar2_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT())
dt_wide_zerovar2_flags <-  lapply(list_flag, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT()) 

NonZeroVarianceCols_ind <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) != 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar2_ind_values <- lapply(list_value_ind, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols_ind(.)) %>%  setDT())
dt_wide_zerovar2_ind_flags <-  lapply(list_flag_ind, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols_ind(.)) %>%  setDT()) 

NonZeroVarianceCols_var <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) != 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar2_var_values <- lapply(list_value_var, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols_var(.)) %>%  setDT())
dt_wide_zerovar2_var_flags <-  lapply(list_flag_var, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols_var(.)) %>%  setDT()) 


# IMPUTATION ACTION ---------
 l1_values <- dt_wide_allNAs_values   # NO IMPUTATION
 l2_values <- dt_wide_zerovar0_values  # NO IMPUTATION
 l3_values <- dt_wide_zerovar1_values  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
 l4_values <- dt_wide_zerovar2_values  # INTERPOLATION
 
 l1_values_ind <- dt_wide_allNAs_ind_values   # NO IMPUTATION
 l2_values_ind <- dt_wide_zerovar0_ind_values  # NO IMPUTATION
 l3_values_ind <- dt_wide_zerovar1_ind_values  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
 l4_values_ind <- dt_wide_zerovar2_ind_values  # INTERPOLATION
 # summary(l4_values_ind)
 
 l1_values_var <- dt_wide_allNAs_var_values   # NO IMPUTATION
 l2_values_var <- dt_wide_zerovar0_var_values  # NO IMPUTATION
 l3_values_var <- dt_wide_zerovar1_var_values  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
 l4_values_var <- dt_wide_zerovar2_var_values  # INTERPOLATION
  summary(l1_values_var)
 
# locf ----
 l3_imputed_values <- lapply(l3_values, function(x) imputeTS::na.locf(x))
 
 l3_imputed_ind_values <- lapply(l3_values_ind, function(x) imputeTS::na.locf(x))
 
 l3_imputed_var_values <- lapply(l3_values_var, function(x) imputeTS::na.locf(x))

 

# linear interpolation ----
 l4_imputed_values <- lapply(l4_values, function(x) imputeTS::na.interpolation(x))
 
 l4_imputed_ind_values <- lapply(l4_values_ind, function(x) imputeTS::na.interpolation(x))

 l4_imputed_var_values <- lapply(l4_values_var, function(x) imputeTS::na.interpolation(x))
 
 

# RECOMBINATION OF DATASETS -----
 l_non_imputed_values <- Map(merge, l1_values, l2_values)
 l_imputed_values <-  Map(merge, l3_imputed_values, l4_imputed_values) 
 l_all_values <- Map(merge, l_non_imputed_values, l_imputed_values)
 
 l_non_imputed_values_ind <- Map(merge, l1_values_ind, l2_values_ind)
 l_imputed_values_ind <-  Map(merge, l3_imputed_ind_values, l4_imputed_ind_values) 
 l_all_values_ind <- Map(merge, l_non_imputed_values_ind, l_imputed_values_ind)
 
 l_non_imputed_values_var <- Map(merge, l1_values_var, l2_values_var)
 l_imputed_values_var <-  Map(merge, l3_imputed_var_values, l4_imputed_var_values) 
 l_all_values_var <- Map(merge, l_non_imputed_values_var, l_imputed_values_var)
 
summary(l_all_values_ind)
                         



# GET LONG FORMAT (Four variables dataset)
l_long_all_values <- lapply(l_all_values, function(x) gather(x, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")))
long_all_values <- do.call("rbind", l_long_all_values)
row.names(long_all_values) <- NULL
# tbl_df(long_all_values)


l_long_all_values_ind <- lapply(l_all_values_ind, function(x) gather(x, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")))
long_values_ind <- do.call("rbind", l_long_all_values_ind)
row.names(long_values_ind) <- NULL
# tbl_df(long_values_ind)


l_long_all_values_var <- lapply(l_all_values_var, function(x) gather(x, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")))
long_values_var <- do.call("rbind", l_long_all_values_var)
row.names(long_values_var) <- NULL
# tbl_df(long_values_var)
unique(long_values_ind$aquastatElement)
unique(long_values_var$aquastatElement)


# wide format -------
df_wide_imputed <- dcast(setDT(long_all_values), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))

df_wide_ind_imputed <- dcast(setDT(long_values_ind), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))

df_wide_var_imputed <- dcast(setDT(long_values_var), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))



df_wide_var_imputed[is.na(df_wide_var_imputed)] <- 0



# Evaluate the rules in the data.table
cast_data <- dcast(d, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value", "flagAquastat"),
                   drop = FALSE)






# Let's make some plots and see how 
plots_ind <- 
  df_wide_ind_imputed %>% 
  group_by(geographicAreaM49) %>% 
  nest() %>% 
  mutate(plot = map2(data, geographicAreaM49, ~ggplot(data = .x)))



# test_4253 <- df_wide_ind_imputed[, .(geographicAreaM49, timePointYears, Value_4253, Value_4251, Value_4252, Value_4250)]
# test_4103 <- df_wide_imputed[, .(geographicAreaM49, timePointYears, Value_4103, Value_4101, Value_4102)]
# test_4313 <- df_wide_imputed[, .(geographicAreaM49, timePointYears, Value_4313, Value_4311, Value_4312, Value_4316)]
# test_4185 <- df_wide_imputed[, .(geographicAreaM49, timePointYears, Value_4185, Value_4176, Value_4155)]
# test_4257 <- df_wide_imputed[, .(geographicAreaM49, timePointYears, Value_4257, Value_4253, Value_4104)]


# long_all_values %>% tbl_df() %>%  filter(aquastatElement %in% c( 4253, 4251, 4252, 4250))
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