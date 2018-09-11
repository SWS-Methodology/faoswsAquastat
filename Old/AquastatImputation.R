##' faoswsAquastatImputation exercise
##' Author: Francy Lisboa
##' Date: 01/08/2018
##' Purpose: this module perfoms the imputation of Aquastat elements using a split-apply-combine approach.
##' 
##' In the raw data the time-series of country-element combinations are expanded with the first and last years as boundaries.
##' 
##' Then, at the country level, data is subdivided into five mutually exclusive dataframes containing different sets of elements
##' 
##' Each dataframe has the dimensions geographicAreaM49, timePointYears and the selected elements. The selection criteria was:
##' Dataframe 1-Elements whose time-series is totally empty.
##' Dataframe 2-Elements whose time-series is totally full  - zero variance and no missing values.
##' Dataframe 3-Elements whose time-series has only one observed value
##' Dataframe 4-Elements whose time-series has zero variance and at least one missisng value
##' Dataframe 5-Elements whose time-series has nonzero variance and at least one missing value
##' 
##' The last observation carried forward/backwards method is applied to the dataframes 3 and 4
##' The linear interpolation method is applied to to dataframe 5
##' 
##' The five dataframes are recombined into a single resulting in a long format dataset with geographicAreaM49, timePointYears, aquastatElement, Value, and flagAquastat.
##' This data is saved in the SWS as aquastat_imputed.
##' 
##'




# Loading libraries
suppressMessages({
  library(faosws)
  library(faoswsUtil)
  library(faoswsFlag)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(magrittr)
  library(data.table)
  library(imputeTS)
})


R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/faoswsAquastatImputation/sws.yml")
  
  ## If you're not on the system, your settings will overwrite any others
  R_SWS_SHARE_PATH = SETTINGS[["share"]]
  
  ## Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])
  
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
  
}



# Getting rules 
rules_guidelines <- c("[4103]=[4101] + [4102]",
                      "[4105]=[4104] - [4106]",
                      "[4107]=[4104]/([4100]/100)",
                      "[4456]=[4160]+[4162]+[4168]+[4170]",
                      "[4108]=[4109]+[4110]",
                      "[4150]=[4100] * [4155] / 100000",
                      "[4157]=[4154]+[4155]-[4156]", 
                      "[4158]=[4157]*1000000/[4104]", 
                      "[4164]=[4160]+[4162]+[4168]",
                      "[4176]=[4160]+[4162]+[4168]-[4174]", 
                      "[4182]=[4176]+[4452]", "[4185]=[4176]+[4155]",
                      "[4187]=[4154]+[4452]", "[4188]=[4185]+[4187]-[4156]",
                      "[4190]=[4188]*1000000/[4104]", 
                      "[4192]=100*([4164]+[4452])/([4164]+[4452]+[4157])",
                      "[4509]=[4193]+[4194]" , "[4196]=[4509]+[4195]",
                      "[4253]=[4251]+[4252]+[4250]", "[4254]=[4250]/[4253]*100", 
                      "[4255]=[4251]/[4253]*100", "[4256]=[4252]/[4253]*100", 
                      "[4257]=[4253]*1000000/[4104]", 
                      "[4263]=[4253]-[4264]-[4265]-[4451]", 
                      "[4271]=100*[4260]/[4250]", "[4273]=100*[4250]/[4188]", 
                      "[4275]=100*[4263]/[4188]", "[4300]=[4303]+[4304]", 
                      "[4305]=100*[4300]/[4103]", "[4311]=[4308]+[4309]+[4310]", 
                      "[4313]=	[4311]+[4312]+[4316]", 
                      "[4317]=	[4313]+[4314]+[4315]", 
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

# describe the elements and indicators
# aquastatElement <- c(4100L, 4101L, 4102L, 4103L, 4104L, 4105L, 4106L, 4107L, 4108L, 
#                      4109L, 4110L, 4111L, 4112L, 4113L, 4114L, 4115L, 4116L, 4150L, 
#                      4151L, 4152L, 4153L, 4154L, 4155L, 4156L, 4157L, 4158L, 4159L, 
#                      4160L, 4161L, 4162L, 4164L, 4165L, 4166L, 4167L, 4168L, 4169L, 
#                      4170L, 4171L, 4172L, 4173L, 4174L, 4175L, 4176L, 4177L, 4178L, 
#                      4182L, 4183L, 4184L, 4185L, 4186L, 4187L, 4188L, 4189L, 4190L, 
#                      4191L, 4192L, 4193L, 4194L, 4195L, 4196L, 4197L, 4250L, 4251L, 
#                      4252L, 4253L, 4254L, 4255L, 4256L, 4257L, 4260L, 4261L, 4262L, 
#                      4263L, 4264L, 4265L, 4266L, 4267L, 4268L, 4269L, 4270L, 4271L, 
#                      4273L, 4275L, 4300L, 4303L, 4304L, 4305L, 4306L, 4307L, 4308L, 
#                      4309L, 4310L, 4311L, 4312L, 4313L, 4314L, 4315L, 4316L, 4317L, 
#                      4318L, 4319L, 4320L, 4321L, 4322L, 4323L, 4324L, 4325L, 4326L, 
#                      4327L, 4328L, 4329L, 4330L, 4331L, 4332L, 4333L, 4334L, 4335L, 
#                      4336L, 4337L, 4338L, 4339L, 4340L, 4341L, 4342L, 4343L, 4344L, 
#                      4345L, 4379L, 4400L, 4401L, 4402L, 4403L, 4410L, 4445L, 4446L, 
#                      4448L, 4449L, 4450L, 4451L, 4452L, 4454L, 4455L, 4456L, 4457L, 
#                      4458L, 4459L, 4461L, 4462L, 4463L, 4464L, 4465L, 4466L, 4467L, 
#                      4468L, 4470L, 4471L, 4509L, 4513L, 4514L, 4527L, 4531L, 4532L, 
#                      4533L, 4534L, 4535L, 4536L, 4538L, 4539L, 4540L, 4546L, 4547L, 
#                      4548L, 4549L, 4550L, 4551L, 4552L, 4553L, 4554L, 4555L, 4556L, 
#                      4557L)

# Replace brackets in rules to make them valid R code
proc_rules_guide <- str_replace_all(rules_guidelines, "\\[([0-9]+)\\]", "Value_\\1")

# Extract all the elements that are used in rules
rule_elements_guide <- unique(unlist(str_extract_all(rules_guidelines, regex("(?<=\\[)[0-9]+(?=\\])"))))


# LOAD DATA ---- (it will be replaceb by the data taken from SWS in QA)
# d <- GetData(swsContext.datasets[[1]]) 
imputKey <- DatasetKey(
  domain = "Aquastat",
  dataset = "aquastat_dan",
  dimensions = list(
    Dimension(name = "geographicAreaM49",
              keys = GetCodeList('Aquastat', 'aquastat_dan', 'geographicAreaM49')[type == 'country', code]),
    Dimension(name = "aquastatElement", keys = GetCodeList('Aquastat', 'aquastat_dan', 'aquastatElement')[, code]),
    Dimension(name = "timePointYears", keys = as.character(1961:2017))
  )
)

d <- GetData(imputKey, flags = TRUE)

# select relevant columns
dt <- d[,.(geographicAreaM49, timePointYears, aquastatElement, Value, flagAquastat)]

# expand time-series of elements so that all elements will intially have the same ts length
dtt <-
  tbl_df(dt) %>%
  group_by(geographicAreaM49, aquastatElement) %>%
  complete(timePointYears = as.character(seq(1961, 2017))) %>%
  ungroup() %>% 
  mutate(aquastatElement = paste0("Value_", aquastatElement)) %>% 
  setDT()

# use this expansion later to correct the time-series lengths of country-element combination
ts_to_correct <-
  tbl_df(dt) %>%
  group_by(geographicAreaM49, aquastatElement) %>%
  complete(timePointYears = as.character(seq(min(as.numeric(timePointYears), na.rm = TRUE), max(as.numeric(timePointYears), na.rm = TRUE)))) %>%
  ungroup() %>% 
  mutate(aquastatElement = paste0("Value_", aquastatElement)) %>% 
  setDT()



# Get wide format dataset (elements as columns)
dt_wide_value <- dcast(dtt, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))


# Create a  list of dataframes which is the starting point for the subsettings
list_value <- split(as.data.frame(dt_wide_value), dt_wide_value$geographicAreaM49)


# Breaking the raw data down
# Dataset: all missing value columns ----
AllMissingColumns <- function(df) as.vector(which(colSums(is.na(df)) == nrow(df))) 
dataframe1 <- lapply(list_value, function(x) {
  indices <- AllMissingColumns(x)
  cbind(x[, 1:2], x[indices])
  }) 


# Dataset: One observation only ----
OneObservationColumns <- function(df) as.vector(which(colSums(!is.na(df)) == 1)) 
dataframe2 <- lapply(list_value, function(x) {
  indices <- OneObservationColumns(x)
  cbind(x[, 1:2], x[indices])
  }) 


# Dataset: zero variance with no missing values -----
ZeroVarianceCols_noNA <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & colSums(is.na(df)) == 0)) 
dataframe3 <- lapply(list_value, function(x) {
  indices <- ZeroVarianceCols_noNA(x)
  cbind(x[, 1:2], x[indices])
  }) 


# Dataset: zero variance and at least one NA --------
ZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dataframe4 <- lapply(list_value, function(x) {
     indices <- ZeroVarianceCols(x)
    cbind(x[, 1:2], x[indices])
  })  


# Dataset: nonzero variance and at least one NA ----
NonZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) != 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dataframe5 <- lapply(list_value, function(x){
  indices <- NonZeroVarianceCols(x)
  cbind(x[, 1:2], x[indices])
})  



# IMPUTATION ACTION ---------
l1_values <- dataframe1  # NO IMPUTATION
l2_values <- dataframe2  # REPLACED BY THE ONLY OBSERVED VALUE
l3_values <- dataframe3  # NO IMPUTATION
l4_values <- dataframe4  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
l5_values <- dataframe5  # INTERPOLATION

# locf ----
l2_imputed_values <- lapply(l2_values, function(x){
  if (ncol(x) > 2) {
  fixed <- x[, 1:2]
  imputable <- x[, 3:ncol(x)]
  imputed <- imputeTS::na.locf(imputable)
  cbind(fixed, imputed)
  } else {
    x
  }} )  


l4_imputed_values <- lapply(l4_values, function(x){
  if (ncol(x) > 2) {
    fixed <- x[, 1:2]
    imputable <- x[, 3:ncol(x)]
    imputed <- imputeTS::na.locf(imputable)
    cbind(fixed, imputed)
  } else {
    x
  }} )


# linear interpolation ----
l5_imputed_values <- lapply(l5_values, function(x){
  if (ncol(x) > 2) {
    fixed <- x[, 1:2]
    imputable <- x[, 3:ncol(x)]
    imputed <- imputeTS::na.interpolation(imputable)
    cbind(fixed, imputed)
  } else {
    x
  }} )

# RECOMBINATION OF DATASETS -----
l01 <- Map(merge, l1_values, l2_imputed_values)
l02 <-  Map(merge,l4_imputed_values,l5_imputed_values) 
l_final<- Map(merge, l01, l02)


# GET LONG FORMAT (Four variables dataset) ----
l_long_all_values <- lapply(l_final, function(x) gather(x, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")))
long_all_values <- do.call("rbind", l_long_all_values) %>% tbl_df() 
long_all_values <- long_all_values %>% mutate(aquastatElement = as.integer(substring(aquastatElement, 7))) %>% arrange(geographicAreaM49, aquastatElement)
row.names(long_all_values) <- NULL



# getting the elements in rules_elements and filter out the missing values
long_values01 <- filter(long_all_values, aquastatElement %in% rule_elements_guide) 

# preparing data to save
data_to_save <- 
  left_join(select(tbl_df(ts_to_correct), -Value) %>% mutate(aquastatElement = as.integer(aquastatElement)), long_values01, by = c("geographicAreaM49", "aquastatElement", "timePointYears")) %>%  
  mutate(flagAquastat = ifelse(is.na(flagAquastat), "I", flagAquastat))
data_to_save <- as.data.table(data_to_save)
data_to_save2 <- data_to_save[!is.na(Value)]



# save data 
SaveData("Aquastat", "aquastat_imputed", data_to_save2, waitTimeout = 5000)
paste0("faoswsAquastatImputation module completed successfully!!!",
       
       stats$inserted,"observations written,",
       
       stats$ignored,"weren't updated,",
       
       stats$discarded,"had problems.")


