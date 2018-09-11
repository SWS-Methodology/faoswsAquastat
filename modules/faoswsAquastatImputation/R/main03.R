##' faoswsAquastatImputation exercise
##' Author: Francy Lisboa
##' Date: 10/09/2018
##' Purpose: this module perfoms the imputation of Aquastat elements using a split-apply-combine approach.
##' 
##' In the raw data the time-series of country-element combinations are expanded from 1961 to 2017
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


# The input data reside in the aquastat_clone dataset in SWS, which has been named aquastat_dan
# The modules works with this data and save the data in the aquastat_imputed dataset.
# Therefore, the session is the one of the aquastat_imputed dataset.
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

# get the data from the input
d <- GetData(imputKey, flags = TRUE)

# select relevant columns
dt <- d[,.(geographicAreaM49, timePointYears, aquastatElement, Value, flagAquastat)]

# expanding time-series so that the imputations will be perfomerd over the missing values falling between the first and last year
# of each country-element combination
dtt  <-
  tbl_df(dt) %>%
  group_by(geographicAreaM49, aquastatElement) %>%
  tidyr::complete(timePointYears = as.character(seq(1961, 2017))) %>%
  dplyr::ungroup() %>% 
  dplyr::mutate(aquastatElement = paste0("Value_", aquastatElement)) %>% 
  dplyr::arrange(geographicAreaM49, aquastatElement) 


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
  if (ncol(x) == 3) {
    name <- names(x)[3]
    fixed <- x[, 1:2]
    imputable <- x[, 3]
    imputed <- as.data.frame(imputeTS::na.locf(imputable))
    names(imputed) <- name
    x <- cbind(fixed, imputed)
  } else if (ncol(x) == 2) { 
    x <- x[, 1:2]
  } else {
    fixed <- x[, 1:2]
    imputable <- x[, 3:ncol(x)]
    imputed <- imputeTS::na.locf(imputable)
    x <- cbind(fixed, imputed)
  }})  


l4_imputed_values <- lapply(l4_values, function(x){
  if (ncol(x) == 3) {
    name <- names(x)[3]
    fixed <- x[, 1:2]
    imputable <- x[, 3]
    imputed <- as.data.frame(imputeTS::na.locf(imputable))
    names(imputed) <- name
    x <- cbind(fixed, imputed)
  } else if (ncol(x) == 2) { 
    x <- x[, 1:2]
  } else {
    fixed <- x[, 1:2]
    imputable <- x[, 3:ncol(x)]
    imputed <- imputeTS::na.locf(imputable)
    x <- cbind(fixed, imputed)
  }})  


# linear interpolation ----
l5_imputed_values <- lapply(l5_values, function(x){
  if (ncol(x) == 3) {
    name <- names(x)[3]
    fixed <- x[, 1:2]
    imputable <- x[, 3]
    imputed <- as.data.frame(imputeTS::na.interpolation(imputable))
    names(imputed) <- name
    x <- cbind(fixed, imputed)
  } else if (ncol(x) == 2) { 
    x <- x[, 1:2]
  } else {
    fixed <- x[, 1:2]
    imputable <- x[, 3:ncol(x)]
    imputed <- imputeTS::na.interpolation(imputable)
    x <- cbind(fixed, imputed)
  }})

# RECOMBINATION OF DATASETS -----

# recombination within countries
recombined01 <- plyr::mlply(unique(d$geographicAreaM49), .fun = function(x) {
  dplyr::full_join(l1_values[[x]], l2_imputed_values[[x]]) %>%
    dplyr::full_join(l4_imputed_values[[x]]) %>% 
    dplyr::full_join(l5_imputed_values[[x]])
})

# within each country, get the long format
l_long_format <- lapply(recombined01, function(x) {     
  data.table::melt(x , c("geographicAreaM49", "timePointYears"), 
                   variable.name = "aquastatElement", 
                   value.name = "Value", variable.factor = FALSE)
  
})

# recombination among countries 
recombined_dt <- as.data.frame(data.table::rbindlist(l_long_format))

# correct the ts length of country-element combinations by using the dtt data
filtering <- dplyr::left_join(dtt, recombined_dt, by = c("geographicAreaM49", "aquastatElement", "timePointYears"))
fixing_step01 <- dplyr::mutate(filtering, aquastatElement = substring(aquastatElement, 7))
fixing_step02 <- dplyr::mutate(fixing_step01, imputed = ifelse(is.na(Value.x) & !is.na(Value.y), "I", "O"))
fixing_step03 <- dplyr::mutate(fixing_step02, flagAquastat = ifelse(imputed == "I", "I", flagAquastat))
fixing_step04 <- dplyr::rename(fixing_step03, Value = Value.y) %>%  dplyr::select(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat)


# get rid of NAs
data_to_save <- data.table::setDT(fixing_step04)[!is.na(Value)]


# save data 
stats <- SaveData("Aquastat", "aquastat_imputed", data_to_save, waitTimeout = 5000)
paste0("faoswsAquastatImputation module completed successfully!!!",
       
       stats$inserted,"observations written,",
       
       stats$ignored,"weren't updated,",
       
       stats$discarded,"had problems.")


library(readr)
definitions <- read_csv("~./github/Aquastat/aquastat_meta.csv")
flags_guideline <- read_csv("~./github/Aquastat/aquastat_ref_table.csv")



test <- group_by(tbl_df(data_to_save), geographicAreaM49, aquastatElement) %>% 
  mutate(fifth = ifelse( (as.numeric(timePointYears) %% 5) == 0, "in", "out"),
         first_year = ifelse(as.numeric(timePointYears) == min(timePointYears), "first", "others"),
         fifth_ed = ifelse(first_year == "first", "in", fifth)) %>% 
  filter(fifth_ed == "in") %>% 
  ungroup()
       
  

