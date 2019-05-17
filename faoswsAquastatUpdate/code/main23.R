print(paste0('Starting AQUASTAT at: ', Sys.time()))
options(scipen = 999, datatable.verbose = FALSE)
##' AquastatBaseline module
##' Author: Francy Lisboa
##' Date: 14/03/2019
##' Purpose: this R script adds new figures to the aquastat dataset by applying
##' calculation - imputation - recalculation sequentialy.
##' The output will be the new baseline for future updates of the Aquastat in the SWS context

# Loading libraries
suppressMessages({
  library(faosws)
  library(faoswsUtil)
  library(faoswsFlag)
  library(data.table)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(magrittr)
  library(zoo)
})



if(CheckDebug()){
  library(faoswsModules)
  SETTINGS = ReadSettings("~./github/faoswsAddIndicators/sws.yml")
  Sys.setenv("R_SWS_SHARE_PATH" = SETTINGS[["share"]])
  R_SWS_SHARE_PATH = SETTINGS[["share"]]
  SetClientFiles(SETTINGS[["certdir"]])
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

# The input data reside in the aquastat_clone dataset in SWS, which has been named aquastat_dan
imputKey <- DatasetKey(
  domain = "Aquastat",
  dataset = "aquastat_enr",
  dimensions = list(
    Dimension(name = "geographicAreaM49",
              keys = GetCodeList('Aquastat', 'aquastat_enr', 'geographicAreaM49')[type == 'country', code]),
    Dimension(name = "aquastatElement", keys = GetCodeList('Aquastat', 'aquastat_enr', 'aquastatElement')[, code]),
    Dimension(name = "timePointYears", keys = as.character(1961:2017))
  )
)

# get the data from the input
print(paste0('reading AQUASTAT enr at: ', Sys.time()))
raw_data <- GetData(imputKey, flags = TRUE)


if ('flagAquastatVisibility' %in% names(raw_data))
  raw_data[, flagAquastatVisibility := NULL]


if ('flagAquastat' %in% names(raw_data))
  setnames(raw_data, 'flagAquastat', 'flagObservationStatus')


print(paste0('read AQUASTAT enr at: ', Sys.time()))

# FAOSTAT ANNEXATION --------------------------------------------------------------------------------------------------------------------
# land
tf_land <- tempfile()
zip_fs_land_location <- 'http://fenixservices.fao.org/faostat/static/bulkdownloads/Inputs_LandUse_E_All_Data_(Normalized).zip'
download.file(zip_fs_land_location, tf_land)
td_land <- tempdir()
file.name <- unzip(tf_land, exdir = td_land)
land <- fread(file.name)

# population
tf_pop <- tempfile()
zip_fs_pop_location <- 'http://fenixservices.fao.org/faostat/static/bulkdownloads/Population_E_All_Data_(Normalized).zip'
download.file(zip_fs_pop_location, tf_pop)
td_pop <- tempdir()
file.name <- unzip(tf_pop, exdir = td_pop)
pop <- fread(file.name)

# Deflator
tf_def <- tempfile()
zip_fs_def_location <- 'http://fenixservices.fao.org/faostat/static/bulkdownloads/Deflators_E_All_Data_(Normalized).zip'
download.file(zip_fs_def_location, tf_def)
td_def <- tempdir()
file.name <- unzip(tf_def, exdir = td_def)
def <- fread(file.name)

print('FAOSTAT data has been sourced from http://fenixservices.fao.org/faostat/static/bulkdownloads/')

# datasets saved in file://hqlprsws1.hq.un.fao.org/sws_r_share/AquastatValidation/data
# land <- fread(file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/data', 'fs_land.csv'))
# pop <- fread(file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/data', 'fs_pop.csv'))
# def <- fread(file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/data', 'fs_deflator.csv'))


# harmonizing columns
fs_sws_names <- function(data) {
   df1 <- data[, mget(c('Area Code', 'Item Code', 'Element Code', 'Year', 'Value', 'Flag'))]
   names(df1) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value', 'flagObservationStatus')
   df1 <- df1 %>%  mutate_at(., c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'flagObservationStatus'), as.character) %>%  setDT()
   return(df1)
}

df_land <- fs_sws_names(land)
df_pop <- fs_sws_names(pop)
df_def <- fs_sws_names(def)

print('FAOSTAT names have been harmonized')


# read in aquastat_faostat_mapping
print('read in aquastat_faostat_mapping')
aqua_fs_map <- ReadDatatable('aquastat_faostat_mapping')
print('aquastat_faostat_mapping loaded')


df_land1 <- df_land[measuredItem %in% unique(aqua_fs_map$item_code) & measuredElement %in% unique(aqua_fs_map$element_code)][geographicAreaM49 < 5000]
df_pop1 <- df_pop[measuredItem %in% unique(aqua_fs_map$item_code) & measuredElement %in% unique(aqua_fs_map$element_code)][geographicAreaM49 < 5000]
df_def1 <- df_def[measuredItem %in% unique(aqua_fs_map$item_code) & measuredElement %in% unique(aqua_fs_map$element_code)][geographicAreaM49 < 5000]

# calculating GDP inflator
print('AQUASTAT Calculating GDP inflator')
df_gdp <- copy(df_def1)
df_gdp1 <- df_gdp[, GDP_2015 := as.numeric(ifelse(timePointYears == '2015', Value, NA)), by = .(geographicAreaM49)]
df_gdp2 <- df_gdp1[, Value_2015 := GDP_2015[which(!is.na(GDP_2015))], by = .(geographicAreaM49) ]


print('AQUASTAT Calculating Value_def := (Value/Value_2015)*100')
df_gdp3 <- df_gdp2[, Value_def := as.numeric(Value)/as.numeric(Value_2015)*100]


df_gdp4 <- df_gdp3[, .(geographicAreaM49, measuredItem, measuredElement, timePointYears, Value_def, flagObservationStatus)]
setnames(df_gdp4, 'Value_def', 'Value')
print('AQUASTAT GDP inflator calculated')

# Binding rows
fs_data <- rbind(df_land1, df_pop1, df_gdp4)
setnames(fs_data, 'measuredElement', 'element_code')
setnames(fs_data, 'measuredItem', 'item_code')
fs_data_merged <- merge(fs_data, aqua_fs_map, by = c('item_code', 'element_code'), all = TRUE)
fs_data_merged <- fs_data_merged[, .(geographicAreaM49, aqua_element_code, timePointYears, Value, flagObservationStatus)]
print('AQUASTAT binding rows is finished')
# convert to geographic M49 areas
fs_areas <- unique(fs_data_merged$geographicAreaM49)
aggregate_groups <- ReadDatatable('aggregate_groups', where = "var_type IN ('area')")
df_fs_areas <- unique(aggregate_groups[var_code %in% fs_areas, .(var_code, var_code_sws)])
df_fs_areas <- df_fs_areas[!is.na(var_code_sws)]
df_fs_areas <- df_fs_areas[!is.na(var_code)]
setnames(df_fs_areas, 'var_code', 'geographicAreaM49')
fs_data_merged_areas <- merge(fs_data_merged, df_fs_areas, by = c("geographicAreaM49"), all.x = TRUE)
fs_data_merged_areas <- fs_data_merged_areas[, .(var_code_sws, aqua_element_code, timePointYears, Value, flagObservationStatus)]
setnames(fs_data_merged_areas, 'var_code_sws', 'geographicAreaM49')
print('AQUASTAT convert geog. areas finished')
# last adjustments
fs_data_merged_areas[, flagObservationStatus := 'E']
setnames(fs_data_merged_areas, 'aqua_element_code', 'aquastatElement')
fs_data_merged_areas <- fs_data_merged_areas[!is.na(geographicAreaM49)]
print('AQUASTAT last adjustments line 147')

# Get countries that exist in both Aquastat and FAOSTAT
common_areas <- intersect(unique(raw_data$geographicAreaM49), unique(fs_data_merged_areas$geographicAreaM49))
fs_data_merged_areas <- fs_data_merged_areas[geographicAreaM49 %in% common_areas]
print('AQUASTAT merge with recoded FAOSTAT line 152')
# Add FAOSTAT data to AQUASAT raw data
fs_data_preproc <- copy(fs_data_merged_areas)
raw_data_01 <- raw_data[!(aquastatElement %in% unique(fs_data_preproc$aquastatElement))]
raw_data_fs_preproc <- rbind(raw_data_01, fs_data_preproc)
raw_data_fs_preproc <- raw_data_fs_preproc[timePointYears >= 1961 & timePointYears <= substring(Sys.Date(), 1, 4)]
raw_data_fs_preproc <- raw_data_fs_preproc[!is.na(Value) & !is.na(flagObservationStatus)]
print('FAOSTAT annexation is finished')


# copy data
copy_data <- copy(raw_data_fs_preproc)
copy_data <- copy_data[, Value := ifelse(flagObservationStatus == 'M', NA, Value)]
copy_data <- copy_data[!is.na(Value) & Value >= 0]


## AQUASTAT INITIALIZATION -------
print('AQUASTAT CALCULATION started')
# Generic objects
# long-term average variables
lta <- c(4150L, 4151L, 4154L, 4155L, 4156L, 4157L, 4159L, 4160L,
         4161L, 4162L, 4164L, 4165L, 4168L, 4170L, 4171L, 4172L, 4173L,
         4174L, 4176L, 4177L, 4178L, 4182L, 4185L, 4187L, 4188L, 4192L,
         4193L, 4194L, 4195L, 4196L, 4452L, 4453L, 4509L, 4536L,
         4549L)

# Preparing data for calculation
prep_for_calculations <- function(data){
  # read in calculation rules  from SWS
  calc <- ReadDatatable('calculation_rule')
  # relevant elements for calculations
  proc_rules <- stringr::str_replace_all(calc$calculation_rule, "\\[([0-9]+)\\]", "Value_\\1")
  rule_elements <- paste0("Value_", sort(unique(unlist(str_extract_all(calc$calculation_rule, regex("(?<=\\[)[0-9]+(?=\\])"))))))
  # Wipe out rules whose have at least one element not appearing in the raw data
  lhs <- paste0("Value_",sort(substring(calc$calculation_rule, 2, 5)))
  rhs <- sort(setdiff(rule_elements, lhs))
  intersection <- sort(intersect(rule_elements, paste0("Value_", sort(unique(data$aquastatElement)))))
  components_to_remove <- sort(setdiff(rhs, intersection))
  data <- data[, list(geographicAreaM49, aquastatElement, timePointYears, Value)]
  data[, aquastatElement := paste0("Value_", aquastatElement)]
  # wide format data for calculations
  data_for_calc <- data.table::dcast(data, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")
  return(list(data = data_for_calc,  rules = proc_rules, indicators = lhs, nonexisting = components_to_remove))
}

# call PrepForCalc function on copy_data
dataforcalc <- prep_for_calculations(copy_data)
missing_elements <-  dataforcalc$nonexisting
for (i in missing_elements) {
  dataforcalc$data[, i] <- NA
}

# reshaping to correct values
df_calc <- dataforcalc$data
measure_vars <- names(df_calc)[str_detect(names(df_calc), 'Value_')]
id_vars <- names(df_calc)[str_detect(names(df_calc), 'Value_') == FALSE]
df_calc[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]
df_melt_before <- melt(df_calc, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')

# Correcting specific elements
zeroelements <- paste0('Value_', c(4308, 4309, 4310, 4312, 4316, 4314, 4315, 4264, 4265, 4451))
df_melt_before0 <- copy(df_melt_before)
df_melt_before0[, ts_len := .N, by = c('geographicAreaM49', 'aquastatElement')]
df_melt_before0[, Value := ifelse(aquastatElement %in% zeroelements & sum(is.na(Value), na.rm = TRUE) == ts_len, 0, Value), by = c('geographicAreaM49', 'aquastatElement')]

# Get wide format for calculations
data_for_calc0 <- data.table::dcast(df_melt_before0, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")

# Calculation
calc_data <- copy(data_for_calc0)
for(i in dataforcalc$rules) calc_data <- within(calc_data, eval(parse(text = i)))

# Replace NAs by primary variable value when suitable
cg <- 'geographicAreaM49'
calc_data_copied <- copy(calc_data)
calc_data_copied[, Value_4263 := ifelse(is.na(Value_4263) & !is.na(Value_4253), Value_4253, Value_4263), by = cg ]
calc_data_copied[, Value_4311 := ifelse(is.na(Value_4311) & !is.na(Value_4308), Value_4308, Value_4311), by = cg ]
calc_data_copied[, Value_4313 := ifelse(is.na(Value_4313) & !is.na(Value_4311), Value_4311, Value_4313), by = cg ]
calc_data_copied[, Value_4317 := ifelse(is.na(Value_4317) & !is.na(Value_4313), Value_4313, Value_4317), by = cg ]
calc_data_copied[, Value_4459 := ifelse(is.na(Value_4459) & !is.na(Value_4309), Value_4309, Value_4459), by = cg ]

# Get long formaty
measure_vars <- names(calc_data_copied)[str_detect(names(calc_data_copied), 'Value_')]
id_vars <- names(calc_data_copied)[str_detect(names(calc_data_copied), 'Value_') == FALSE]
calc_data_copied[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]
df_melt <- melt(calc_data_copied, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
df_long <- df_melt[!is.na(Value)]
df_long <- df_long[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
df_long[, aquastatElement := substring(aquastatElement, 7)]


setnames(df_long, 'Value', 'Value_calc')
dfmerged_calc <- merge(df_long, copy_data, by = c("geographicAreaM49",'aquastatElement', "timePointYears"), all.x = TRUE)
dfmerged_calc <- dfmerged_calc[, Value := ifelse(!is.na(Value), Value, Value_calc)]
dfmerged_calc <- dfmerged_calc[!is.na(Value)]
dfmerged_calc <- dfmerged_calc[!is.infinite(Value)]
dfmerged_calc <- dfmerged_calc[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagObservationStatus)]
dfmerged_calc <- dfmerged_calc[, flagObservationStatus := ifelse(is.na(flagObservationStatus), 'C', flagObservationStatus)]
dfflagrecovery <- copy(dfmerged_calc)
print('AQUASTAT CALCULATION finished')

## IMPUTATION -------
# Time-series expansion
#' The aqua_expand_ts is a helper for expanding time-series at a user-defined level of granularity (grouping_key)
#' and so defining the number of NA to be filled by the imputation process.
#' @param data a dataset containing at least geographicAreaM49, aquastatElement, timePointYear, and Value
#' @param start_year a integer setting the year in which the expansion should start. If NULL the mininum year the series is used.
#' @param end_year a integer setting the year in which the expansion should stop. If NULL, the max year the series is used.
#' @grouping_key a vector of character strings indicating the granulatiry level defining the original time-series length
#' br()
#' The function outputsa data.table/data.frame object with increased number of rows.
#' The extension to which the data set passed as argument increases depends on the start and end year passed to the function.
#' If start_year argument is NULL, the mininum year the series is used.
#' If end_year argument is NULL, the max year the series is used.
#' If both start_year and end_year are NULL, min and max years of the series will set the expansion boundaries.
aqua_expand_ts <- function(data, start_year = NULL, end_year = NULL, grouping_key = NULL) {
  # ensure features as integers
  data$geographicAreaM49 <- as.integer(data$geographicAreaM49)
  data$aquastatElement<- as.integer(data$aquastatElement)
  data$timePointYears <- as.integer(data$timePointYears)
  # converts data into a tibble
  tbl <- tbl_df(data)
  # make aquastatElement an character
  tbl <- tbl %>% mutate(aquastatElement = paste0("Value_", aquastatElement))
  if (is.null(start_year) & !is.null(end_year)) {
    end_year <- as.integer(end_year)
    tbl %>%
      dplyr::group_by_(.dots = grouping_key) %>%
      tidyr::complete(timePointYears = seq(min(timePointYears, na.rm = TRUE), end_year)) %>%
      dplyr::ungroup() %>%
      dplyr::arrange_(.dots = grouping_key) -> df_exts
  }
  if (!is.null(start_year) & is.null(end_year)) {
    start_year <- as.integer(start_year)
    tbl %>%
      dplyr::group_by_(.dots = grouping_key) %>%
      tidyr::complete(timePointYears = seq(start_year, max(timePointYears, na.rm = TRUE))) %>%
      dplyr::ungroup() %>%
      dplyr::arrange_(.dots = grouping_key) -> df_exts
    }
  if (!is.null(start_year) & !is.null(end_year)) {
    start_year <- as.integer(start_year)
    end_year <- as.integer(end_year)
    tbl %>%
      dplyr::group_by_(.dots = grouping_key) %>%
      tidyr::complete(timePointYears = seq(start_year, end_year)) %>%
      dplyr::ungroup() %>%
      dplyr::arrange_(.dots = grouping_key) -> df_exts
  }
  if (is.null(start_year) & is.null(end_year)){
    tbl %>%
      dplyr::group_by(.dots = grouping_key) %>%
      tidyr::complete(timePointYears = seq(min(timePointYears, na.rm = TRUE), max(timePointYears, na.rm = TRUE))) %>%
      dplyr::ungroup() %>%
      dplyr::arrange_(.dots = grouping_key) -> df_exts
  }
  df_exts <- data.table(df_exts)
  return(df_exts)
}
grouping_key <- c("geographicAreaM49", "aquastatElement")

print('AQUASTAT ts expansions started')
df_exp <- aqua_expand_ts(dfflagrecovery, start_year = NULL, end_year = 2018, grouping_key = grouping_key)
df_exp[, flagObservationStatus := ifelse(is.na(flagObservationStatus), 'Im', flagObservationStatus)]
print('AQUASTAT ts expansions finished')

# Preparing for imputations
PrepImputationData <- function(data) {
        df <- df_exp
        # df <- copy(data)
        df[, ts_len := .N, by = grouping_key]
        # 1: all values in the Area - element time-series are missing
        # 2: the Area - element time-series only have one observation
        # 3: the Area - element time-series has more than two values
        df1 <- df[,  imp0 := ifelse(sum(is.na(Value), na.rm = TRUE) == ts_len, 1L,
                                          ifelse(sum(!is.na(Value), na.rm = TRUE) == 1, 2L,
                                                 ifelse(sum(!is.na(Value), na.rm = TRUE) >= 2, 3L))), by = grouping_key]
        # 31: If the ts has more than two values and they are different from each other
        # 32: if the ts has more than two values but the are the same
        df2 <- df1[, ts_len := NULL]
        df2[, imp0 := as.integer(ifelse(imp0 == 3 & var(Value, na.rm = TRUE) > 0, 31L,
                                        ifelse(imp0 == 3 & var(Value , na.rm = TRUE) == 0, 32L,
                                               imp0))), by = grouping_key]
        # 41: if aquastatElement is a long-term avergage variable and with vari imp0 == 4
        df3 <- df2[, imp0 := as.integer(ifelse(aquastatElement %in% paste0('Value_', lta) & imp0 == 31, 41, imp0)),by = grouping_key]
        df3[, imp0 := as.integer(ifelse(aquastatElement %in% paste0('Value_', lta) & imp0 == 32, 42, imp0)),by = grouping_key]
        df3
}
df_imp_pre <- PrepImputationData(df_exp)
df_imp_pre_copy <- copy(df_imp_pre)
# Get the elements that can directly be imputed (No depending on other elements)
#df_prim <- df_imp_pre[aquastatElement %in% paste0('Value_', primary_elements)]
# 2: carried forward
# 31: linear interpolation
# 32: carried forward
# 41: carried forward
# 42: carried forward
print('AQUASTAT IMPUTATION started, line 349')
df_imp <- df_imp_pre[, value := as.numeric(ifelse(imp0 == "2", zoo::na.locf(Value),
                                       ifelse(imp0 == "41", nth(Value, 2),
                                              ifelse(imp0 == "42", first(Value),
                                                     ifelse(imp0 == "32", zoo::na.locf(Value),
                                                            ifelse(imp0 == "31", imputeTS::na.interpolation(as.numeric(Value)), Value)))))), by = grouping_key]

df_prim_imp <- df_imp[, list(geographicAreaM49, aquastatElement, timePointYears, value, flagObservationStatus, imp0)]
setnames(df_prim_imp, 'value', "Value")
df_prim_imp[, aquastatElement := substring(aquastatElement, 7)]
df_prim_imp[!is.na(Value)]
print('AQUASTAT IMPUTATION finished line 370')
# RECALCULATION -----------------------------------------------------------------------------------------------------------------
print('AQUASTAT RECALCULATION started line 372')
dataforrecalc <- prep_for_calculations(df_prim_imp)
dataforrecalc$nonexisting
missing_elements <-  dataforrecalc$nonexisting
for (i in missing_elements) {
  dataforrecalc$data[, i] <- NA
}
data_for_recalc <- dataforrecalc$data
# Calculation
recalc_data <- copy(data_for_recalc)
for(i in dataforcalc$rules) recalc_data <- within(recalc_data, eval(parse(text = i)))
# Replace NAs by primary variable value when suitable
cg <- 'geographicAreaM49'
recalc_data_copied <- copy(recalc_data)
recalc_data_copied[, Value_4263 := ifelse(is.na(Value_4263) & !is.na(Value_4253), Value_4253, Value_4263), by = cg ]
recalc_data_copied[, Value_4311 := ifelse(is.na(Value_4311) & !is.na(Value_4308), Value_4308, Value_4311), by = cg ]
recalc_data_copied[, Value_4313 := ifelse(is.na(Value_4313) & !is.na(Value_4311), Value_4311, Value_4313), by = cg ]
recalc_data_copied[, Value_4317 := ifelse(is.na(Value_4317) & !is.na(Value_4313), Value_4313, Value_4317), by = cg ]
recalc_data_copied[, Value_4459 := ifelse(is.na(Value_4459) & !is.na(Value_4309), Value_4309, Value_4459), by = cg ]

# Get long format
measure_vars <- names(recalc_data_copied)[str_detect(names(recalc_data_copied), 'Value_')]
id_vars <- names(recalc_data_copied)[str_detect(names(recalc_data_copied), 'Value_') == FALSE]
recalc_data_copied[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]
df_long_rec <- melt(recalc_data_copied, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
setnames(df_long_rec, 'Value', 'Value_recalc')
df_long_rec[, aquastatElement := substring(aquastatElement, 7)]
dfmerged_recalc <- merge(df_long_rec, df_prim_imp, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'), all = TRUE)
dfmerged_recalc <- dfmerged_recalc[, Value := ifelse(!is.na(Value), Value, Value_recalc)]
dfmerged_recalc <- dfmerged_recalc[!is.na(Value)]
dfmerged_recalc <- dfmerged_recalc[!is.infinite(Value)]
dfmerged_recalc <- dfmerged_recalc[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
dfmerged_recalc <- dfmerged_recalc[order(geographicAreaM49, aquastatElement)]
print('AQUASTAT RECALCULATION finished line 405')

print('AQUASTAT 4551 RECALCULATION started line 407')
# Getting the Water use efficiency
sdg_df <- dfmerged_recalc[aquastatElement %in% c(4254, 4255, 4256, 4551, 4552, 4553, 4554)]
sdg_df[, aquastatElement := paste0('Value_', aquastatElement)]
sdg_for_calc <- data.table::dcast(sdg_df, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")
sdg_calc_data <- copy(sdg_for_calc)
for(i in dataforcalc$rules[68]) sdg_calc_data <- within(sdg_calc_data, eval(parse(text = i)))
measure_vars <- names(sdg_calc_data)[str_detect(names(sdg_calc_data), 'Value_')]
id_vars <- names(sdg_calc_data)[str_detect(names(sdg_calc_data), 'Value_') == FALSE]
sdg_calc_data[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]
df_melt_sdg <- melt(sdg_calc_data, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
df_melt_sdg[, aquastatElement := substring(aquastatElement, 7)]
print('AQUASTAT 4551 RECALCULATION finished line 408')
# Binding dataframes
dffinal <- rbind(dfmerged_recalc[!(aquastatElement %in% c(4254, 4255, 4256, 4551, 4552, 4553, 4554))], df_melt_sdg)
dffinal <- dffinal[!is.na(Value)]


# RECOVERYING FLAGS ---------
setnames(dffinal, 'Value', 'Value_final')
recover <- df_imp_pre_copy[, aquastatElement := substring(aquastatElement, 7)]
dfmerged_final <- merge(dffinal, df_prim_imp, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'),  all.x = TRUE)
dfmerged_final <- dfmerged_final[, .(geographicAreaM49, aquastatElement, timePointYears, Value_final, flagObservationStatus, imp0)]
setnames(dfmerged_final, 'Value_final', 'Value')
dfmerged_final <- dfmerged_final[, flagObservationStatus := ifelse(is.na(flagObservationStatus), 'C', flagObservationStatus)]
dfmerged_final <- dfmerged_final[!is.na(Value)]


# LTA correct
df_lta <- copy(dfmerged_final)
df_lta_corr <- df_lta[aquastatElement %in% lta]
df_lta_corr <- df_lta_corr[aquastatElement %in% lta,
                           flagObservationStatus := ifelse(str_detect(first(timePointYears), '2$'), nth(flagObservationStatus, 1), nth(flagObservationStatus, 2)),
       by = .(geographicAreaM49, aquastatElement)]
dfmerged_final0 <- dfmerged_final[!(aquastatElement %in% lta)]
dfmerged_final_proc <- rbind(dfmerged_final0, df_lta_corr)


print('AQUASTAT FLAG NORMALIZATION started line 445')
# flagObservationStatus
aqua_to_sws_flag <- function(data) {
  # if (is.na(grep("flagAquastat", names(data)))) {
  #   stop("Error: the data needs to have a flagAquastat column")
  # }
  # a function that maps aquastat flags to sws flags
  getFlagObservationStatus <- function(flags){
    new_flags <- rep(NA_character_, length(flags))
    new_flags[nchar(flags) > 1L] <- "#"
    new_flags[nchar(flags) == 0L] <- ""
    new_flags[flags == "E"] <- "X"
    new_flags[flags == "K"] <- "E"
    new_flags[flags == "L"] <- "E"
    new_flags[flags == "I"] <- "E"
    new_flags[flags == "F"] <- "E"
    new_flags[flags == "C"] <- "C"
    new_flags[flags == ""] <-  ""
    new_flags[flags == "M"] <- "Im"
    new_flags[flags == 'Im'] <- "Im"
    return(new_flags)
  }
  data$flagObservationStatus <- getFlagObservationStatus(data$flagObservationStatus)
  df_fix <- data[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagObservationStatus, imp0)]
  return(df_fix)
}
flaObs_data <- aqua_to_sws_flag(dfmerged_final_proc)

# flagMethod
dataflag <- flaObs_data[, flagMethod := ifelse(flagObservationStatus %in% c('Im') & imp0 != 31L , 't', NA)]
dataflag <- dataflag[, flagMethod := ifelse(flagObservationStatus %in% c('Im') & imp0 == 31L , 'e', flagMethod)]
dataflag <- dataflag[, flagMethod := ifelse(flagObservationStatus %in% c('C'), 'i', flagMethod)]
dataflag <- dataflag[, flagMethod := ifelse(flagObservationStatus %in% c('E'), '-', flagMethod)]
dataflag <- dataflag[, flagMethod := ifelse(flagObservationStatus %in% c(''), 'p', flagMethod)]
dataflag <- dataflag[, flagMethod := ifelse(flagObservationStatus %in% c('X'), 'c', flagMethod)]

# After correctly assigning C to i in flagMethod, C --> E in the flagObservation Status
dataflag <- dataflag[, flagObservationStatus := ifelse(flagObservationStatus == 'C', 'E', flagObservationStatus)]
dataflag <- dataflag[, flagObservationStatus := ifelse(flagObservationStatus == 'Im', 'I', flagObservationStatus)]
print('AQUASTAT FLAG NORMALIZATION finished line 473')

# Get the final dataset
final_data <- dataflag[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagObservationStatus, flagMethod)]
final_data <- final_data[!is.na(Value)]
final_data <- final_data[Value >= 0]



val_rules <- ReadDatatable('validation_rules_clone')
val_rules_100 <- val_rules[rhs %in% c("100.0", "100")]
val_rules_100[, lhs := substring(lhs, 7)]
final_data_corr <- final_data[, Value := ifelse(aquastatElement %in% val_rules_100$lhs & Value > 100, 100, Value)]
final_data_corr <- final_data_corr[, geographicAreaM49 := as.character(geographicAreaM49)]
final_data_corr <- final_data_corr[, timePointYears := as.character(timePointYears)]

# SAVE DATA -------
saveRDS(final_data_corr, file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/output', 'baseline.rds'))
stats <- SaveData("Aquastat", "aquastat_baseline", final_data_corr, waitTimeout = 10000)
paste0("AquastatBaseline module completed successfully!!!")
print(paste0('Ending at: ', Sys.time()))




