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

# The input data reside in the aquastat_clone dataset in SWS
# The modules works with this data and save the data in the aquastat_imputed dataset.
# Therefore, the session is the one of the aquastat_imputed dataset
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

# expand time-series of elements so that all elements will intially have the same ts length
dtt <-
  dplyr::tbl_df(dt) %>%
  group_by(geographicAreaM49, aquastatElement) %>%
  tidyr::complete(timePointYears = as.character(seq(1961, 2017))) %>%
  dplyr::ungroup() %>% 
  dplyr::mutate(aquastatElement = paste0("Value_", aquastatElement)) %>% 
  data.table::setDT()

# use this expansion later to correct the time-series lengths of country-element combination
ts_to_correct <-
  tbl_df(dt) %>%
  group_by(geographicAreaM49, aquastatElement) %>%
  tidyr::complete(timePointYears = as.character(seq(min(as.numeric(timePointYears), na.rm = TRUE), max(as.numeric(timePointYears), na.rm = TRUE)))) %>%
  dplyr::ungroup() %>% 
  dplyr::mutate(aquastatElement = paste0("Value_", aquastatElement)) %>% 
  data.table::setDT()



# Get wide format dataset (elements as columns)
dt_wide_value <- dcast(dtt, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))


# Create a  list of dataframes which is the starting point for the subsettings
list_value <- split(as.data.frame(dt_wide_value), dt_wide_value$geographicAreaM49)


# Breaking the raw data down
# Dataset: all missing value columns ----
dataframe1 <- lapply(list_value, function(x) {
  indices <- AllMissingColumns(x)
  cbind(x[, 1:2], x[indices])
  }) 


# Dataset: One observation only ----
dataframe2 <- lapply(list_value, function(x) {
  indices <- OneObservationColumns(x)
  cbind(x[, 1:2], x[indices])
  }) 


# Dataset: zero variance with no missing values -----
dataframe3 <- lapply(list_value, function(x) {
  indices <- ZeroVarianceCols_noNA(x)
  cbind(x[, 1:2], x[indices])
  }) 


# Dataset: zero variance and at least one NA --------
dataframe4 <- lapply(list_value, function(x) {
     indices <- ZeroVarianceCols(x)
    cbind(x[, 1:2], x[indices])
  })  


# Dataset: nonzero variance and at least one NA ----
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
names(recombined01) <- unique(d$geographicAreaM49)
# recombination among countries 
recombined <- as.data.frame(data.table::rbindlist(recombined01))


# GET LONG FORMAT (Four variables dataset) ----
long_format_df <- data.table::melt(recombined , c("geographicAreaM49", "timePointYears"), 
                    variable.name = "aquastatElement", 
                    value.name = "Value", variable.factor = FALSE)

# correct the ts length of country-element combinations by using the ts_to_correct data
filter_df <- dplyr::select(ts_to_correct, geographicAreaM49, aquastatElement, timePointYears, flagAquastat) %>%  tbl_df()
filtering <- dplyr::left_join(filter_df, long_format_df, by = c("geographicAreaM49", "aquastatElement", "timePointYears"))
fixing_elements <- dplyr::mutate(filtering, aquastatElement = substring(aquastatElement, 7))
fixing_flag <- dplyr::mutate(fixing_elements, flagAquastat = ifelse(is.na(flagAquastat), "I", flagAquastat))


# get rid of NAs
data_to_save <- data.table::setDT(fixing_flag)[!is.na(Value)]


# save data 
stats <- SaveData("Aquastat", "aquastat_imputed", data_to_save, waitTimeout = 5000)
paste0("faoswsAquastatImputation module completed successfully!!!",
       
       stats$inserted,"observations written,",
       
       stats$ignored,"weren't updated,",
       
       stats$discarded,"had problems.")


