print('AQUASTAT UPDATE MODULE has started')
##' AquastatUpdate module
##' Author: Francy Lisboa
##' Date: 09/04/2019
##' Purpose: Updates the aquastat_legacy data with new data coming from questionnaries and aquastat_questionnaire

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
  SETTINGS = ReadSettings("~./github/faoswsAquastatUpdate/sws.yml")
  Sys.setenv("R_SWS_SHARE_PATH" = SETTINGS[["share"]])
  R_SWS_SHARE_PATH = SETTINGS[["share"]]
  SetClientFiles(SETTINGS[["certdir"]])
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

if (!CheckDebug()) {
  R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
}

print('AQUASTAT Load inputs and data tables started')
# Get the legacy data key
  imputKey <- DatasetKey(
    domain = "Aquastat",
    dataset = "aquastat_enr",
    dimensions = list(
      Dimension(name = "geographicAreaM49",
                keys = GetCodeList('Aquastat', "aquastat_enr", 'geographicAreaM49')[type == 'country', code]),
      Dimension(name = "aquastatElement", keys = GetCodeList('Aquastat', "aquastat_enr", 'aquastatElement')[, code]),
      Dimension(name = "timePointYears", keys = as.character(1961:substring(Sys.time(), 1, 4)))
    )
  )

  data_to_update<- GetData(imputKey, flags = TRUE)
  data_to_update$id <- 1L


# NEW INCOMING DATA

# get external data from SWS database
# imputKey <- DatasetKey(
#     domain = "Aquastat",
#     dataset = "aquastat_external",
#     dimensions = list(
#       Dimension(name = "geographicAreaM49",
#                 keys = GetCodeList('Aquastat', "aquastat_external", 'geographicAreaM49')[type == 'country', code]),
#       Dimension(name = "aquastatElement", keys = GetCodeList('Aquastat', "aquastat_external", 'aquastatElement')[, code]),
#       Dimension(name = "timePointYears", keys = as.character(1961:substring(Sys.time(), 1, 4)))
#     )
#   )
# data_external <- GetData(imputKey, flags = TRUE)
data_external <- readRDS(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/data/Update/aquastat_external.rds'))
data_external$id <- 2L
data_external <- data_external[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagObservationStatus, id)]


# get questionnaire data from share drive
data_quest <- fread(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/data/Update/aquastat_questionnaire.csv'))
data_quest$id <- 3L


# DATA TABLES
# get LTA variables
aqua_reference <- ReadDatatable("aquastat_reference")
lta <- aqua_reference[lta == 1L, element_code]

# get m49 geographic codes
cg <- ReadDatatable("a2017regionalgroupings_sdg_feb2017")
m49_areas <- unique(cg$m49_code)



print('AQUASTAT ColumnNameHarmonization has started')
# create a list of input datasets
ColumnNameHarmonization <- function(d){
        d <- copy(d)
        d[, (colnames(d)) := lapply(.SD, as.character), .SDcols = colnames(d)]
        d[, Value := as.numeric(Value)]

        pattern_geog = "geogr|geo|Area|area"
        pattern_elem = "Ele|elem|Element|element"
        pattern_year = "Years|years|Year|year"
        pattern_valu = "Valu|value|Value"
        pattern_flag = "flag|Status|flagA"
        p_area <- "geographicAreaM49"
        p_elem <- "aquastatElement"
        p_year <- "timePointYears"
        p_valu <- "Value"
        p_flag <- "flagAquastat"

        if(!is.null(grep(names(d), pattern = pattern_geog)))
          names(d)[grep(names(d), pattern = pattern_geog)] <- p_area

        if(!is.null(grep(names(d), pattern = pattern_elem)))
          names(d)[grep(names(d), pattern = pattern_elem)] <- p_elem

        if(!is.null(grep(names(d), pattern = pattern_year)))
          names(d)[grep(names(d), pattern = pattern_year)] <- p_year

        if(!is.null(grep(names(d), pattern = pattern_valu)))
          names(d)[grep(names(d), pattern = pattern_valu)] <- p_valu

        if(!is.null(grep(names(d), pattern = pattern_flag)))
          names(d)[grep(names(d), pattern = pattern_flag)] <- p_flag

        # get the first five columns
         dd <- d[, mget(c('geographicAreaM49', 'aquastatElement', 'timePointYears', 'Value', 'flagAquastat', 'id')), ]

         # dd[, flagAquastat := ifelse(flagAquastat == 'X' & id == 2L, 'E',flagAquastat)]
         # dd[, flagAquastat := ifelse(is.na(flagAquastat) & id == 3L, "",flagAquastat)]

         dd <- dd[geographicAreaM49 %in% m49_areas][order(geographicAreaM49, aquastatElement, timePointYears)]

        return(dd)
}

dt1 <- rbindlist(lapply(list(data_to_update, data_external, data_quest), ColumnNameHarmonization))

# Get the right starting dataset
# Creates an index variable for each dataset
# group by area, element, and year
# subset the Value variable withe the max index value. The max index is the new data coming from questionnaire
# the minimum index is the ata from legacy
# use the same strategy to get the flag from the new data from questionnaire
starting_input <- function() {
                d <- copy(dt1)

                dt <- d[,   # keep year value with the follwing hierarchy Quest > Ext > Legacy
                              .(val_new = Value[id == max(id)],
                               val_old = Value[id == min(id)],

                              # keep year value with the follwing hierarchy Quest > Ext > Legacy
                               year_new = timePointYears[id == max(id)],
                               year_old = timePointYears[id == min(id)],

                              # keep flag value with the follwing hierarchy Quest > Ext > Legacy
                               flag_new = flagAquastat[id == max(id)],
                               flag_old = flagAquastat[id == min(id)],

                              # get the possible duplicated value from legacy
                              # flagAquastat = flagAquastat[id == max(id)],
                               freq = .N,
                               maxID = max(id)), by = c('geographicAreaM49', 'aquastatElement', 'timePointYears')][,

                               `:=`(diff_abs = abs(val_new - val_old),
                                diff_rel = abs(1 - (val_new/(val_old))),
                                diff_year = abs(as.numeric(year_new) - as.numeric(year_old)))
                               ]

                data_start <- dt[, .(geographicAreaM49, aquastatElement, year_new, year_old, val_new, flag_new, flag_old, maxID)]
                data_start[, timePointYears := as.character(ifelse(!is.na(year_new), year_new, year_old))]
                data_start[, flagAquastat := as.character(ifelse(!is.na(flag_new), flag_new, flag_old))]
                data_start <- data_start[, .(geographicAreaM49, aquastatElement, timePointYears, val_new, flagAquastat, maxID)]

                setnames(data_start , c('val_new'), c('Value'))
                data_revision <- dt
                res_list <- list(data_start, data_revision)
                return(res_list)
}

copy_data <- starting_input()[[1]]
revision_data <- starting_input()[[2]]
print('AQUASTAT ColumnNameHarmonization has finished')

# FLAG CONVERSION --------------------------------------------------------------------------------------------------------------
getFlagObsStatus <- function() {
    if (!is.data.table(data)) data <- data.table(data)
    d <- copy(copy_data)
    dt <- d[, .(geographicAreaM49, aquastatElement, timePointYears, Value, maxID,
                   flagAquastat = case_when(
                     is.na(flagAquastat) & maxID %in% c(3L) ~ "",
                     flagAquastat == "X" & maxID %in% c(2L) ~ "X",
                     flagAquastat == "E" & maxID %in% c(1L, 3L) ~ "X",
                     flagAquastat == "K" & maxID %in% c(1L, 3L) ~ "E",
                     flagAquastat == "L" & maxID %in% c(1L, 3L) ~ "E",
                     flagAquastat == "I" & maxID %in% c(1L, 3L) ~ "E",
                     flagAquastat == "F" & maxID %in% c(1L, 3L) ~ "E",
                     flagAquastat == "C" & maxID %in% c(1L, 3L) ~ "C",
                     flagAquastat == ""  & maxID %in% c(1L, 3L) ~ "",
                     flagAquastat == "M" & maxID %in% c(1L, 3L) ~ "M")

    )]

    df_fix_for_obs <- dt[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, maxID)]
    df_fix_for_obs[order(geographicAreaM49, aquastatElement, timePointYears)][]


  }
copy_data_flagObs_corr <- getFlagObsStatus()


## AQUASTAT INITIALIZATION -------
print('AQUASTAT CALCULATION has started')
# Preparing data for calculation
prep_for_calculations <- function(data){
            data <- copy(data)
            # data <- copy_data_flagObs_corr
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
            data1 <- data[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
            data1[, aquastatElement := paste0("Value_", aquastatElement)]

            # wide format data for calculations
            data_for_calc <- data.table::dcast(data1, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")[]
            return(list(data = data_for_calc,  rules = proc_rules, indicators = lhs, nonexisting = components_to_remove))
}

# call PrepForCalc function on copy_data
dataforcalc <- prep_for_calculations(copy_data_flagObs_corr)

# Add nonexisting elements to the pivot_wider dataset so that the calculation do not crash.
missing_elements <-  dataforcalc$nonexisting
for (i in missing_elements) {
  dataforcalc$data[, i] <- as.numeric(NA)
}

# reshaping to correct values
PVapplicableElement <- function(){
        df_calc <- copy(dataforcalc$data)
        zeroelements <- paste0('Value_', c(4308, 4309, 4310, 4312, 4316, 4314, 4315, 4264, 4265, 4451))
        df_calc[, (zeroelements) := lapply(.SD, function(x) { x <- as.numeric(ifelse(all(is.na(x)), 0, x))}), .SDcols = zeroelements]
        df_calc[]
}
el2zero_dt <- PVapplicableElement()

# Get Indicators
AddIndicators_01 <- function(){
        d <- copy(el2zero_dt)
        calc_data <- d
        for(i in dataforcalc$rules) calc_data <- within(calc_data, eval(parse(text = i)))
        calc_data[]
}
data_calculated <- AddIndicators_01()
print('AQUASTAT INDICATORS have been added: first round')

# Replace NAs by primary variable value when suitable
pv_correction_01 <- function(data){
        cg <- 'geographicAreaM49'
        d <-  copy(data)
        d[, Value_4263 := ifelse(is.na(Value_4263) & !is.na(Value_4253), Value_4253, Value_4263), by = cg ]
        d[, Value_4311 := ifelse(is.na(Value_4311) & !is.na(Value_4308), Value_4308, Value_4311), by = cg ]
        d[, Value_4313 := ifelse(is.na(Value_4313) & !is.na(Value_4311), Value_4311, Value_4313), by = cg ]
        d[, Value_4317 := ifelse(is.na(Value_4317) & !is.na(Value_4313), Value_4313, Value_4317), by = cg ]
        d[, Value_4459 := ifelse(is.na(Value_4459) & !is.na(Value_4309), Value_4309, Value_4459), by = cg ]
}

pv_corrected_data <- pv_correction_01(data_calculated)

print('AQUASTAT PRIMARy VARIABLE CORRECTION has been done: first round')

# reshape and get back flags
reshape_data_01 <- function(){

      d <- copy(pv_corrected_data)

      measure_vars <- names(d)[str_detect(names(d), 'Value_')][]
      id_vars <- names(d)[str_detect(names(d), 'Value_') == FALSE]
      d[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]
      df_melt <- melt(d, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
      dd <- df_melt[!is.na(Value)]
      dd <- dd[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
      dd[, aquastatElement := substring(aquastatElement, 7)]
      setnames(dd, 'Value', 'Value_calc')
      dd1 <- merge(dd, copy_data_flagObs_corr, by = c("geographicAreaM49",'aquastatElement', "timePointYears"), all = TRUE)
      dd1[, Value := ifelse(!is.na(Value), Value, Value_calc)]
      dd1[, maxID := ifelse(!is.na(maxID), maxID, 4L)]
      dd1 <- dd1[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, maxID)]
      dd1[, flagAquastat := ifelse(is.na(flagAquastat), 'C', flagAquastat)]
      dd2 <- dd1[is.finite(Value)][]
      dd2[]
  }

finished_calculation_01 <- reshape_data_01()
print('AQUASTAT CALCULATION first round finished')

## IMPUTATION -----------------------------------------------------------------------------------------------------------
print('AQUASTAT INITIALIZING IMPUTATION PHASE')
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


di <- copy(finished_calculation_01)
di <- di[flagAquastat != 'M']
grouping_key <- c("geographicAreaM49", "aquastatElement")
df_exp <- aqua_expand_ts(di , start_year = NULL, end_year = substring(Sys.time(), 1, 4), grouping_key = grouping_key)
df_exp[, maxID := ifelse(is.na(maxID), 5L, maxID)]
df_exp[, flagAquastat := ifelse(is.na(flagAquastat), 'I', flagAquastat)]


# Preparing for imputations
PrepImputationData <- function() {

        lta_chr <- paste0('Value_', lta)

        d <- copy(df_exp)
        # df <- copy(data)
        d[, ts_len := .N, by = grouping_key]

        d[, imp0 := case_when(
          var(Value, na.rm = TRUE) > 0 ~ 31L,  # more than one observation and different values
          var(Value, na.rm = TRUE) == 0 ~ 32L, # more than one observation but with same value
          sum(!is.na(Value), na.rm = TRUE) == 1 ~ 2L,    # only one observation
          sum(is.na(Value), na.rm = TRUE) == ts_len ~ 1L # all missing
         ), by = grouping_key][,
           imp0 := ifelse(aquastatElement %in% lta_chr & imp0 == 32L , 42L,
                          ifelse(aquastatElement %in% lta_chr & imp0 == 31L, 41L,
                                 ifelse(aquastatElement %in% lta_chr & sum(is.na(Value), na.rm = TRUE) == 1, 5L, imp0)))
         , by = grouping_key][]
        d[, aquastatElement := substring(aquastatElement, 7)]
        d
        #
        # # 1: all values in the Area - element time-series are missing
        # # 2: the Area - element time-series only have one observation
        # # 3: the Area - element time-series has more than two values
        # d[,  imp0 := ifelse(sum(is.na(Value), na.rm = TRUE) == ts_len, 1L,
        #                                   ifelse(sum(!is.na(Value), na.rm = TRUE) == 1, 2L,
        #                                          ifelse(sum(!is.na(Value), na.rm = TRUE) >= 2, 3L))), by = grouping_key]
        # # 31: If the ts has more than two values and they are different from each other
        # # 32: if the ts has more than two values but the are the same
        # d[, ts_len := NULL]
        # d[, imp0 := as.integer(ifelse(imp0 == 3L & var(Value, na.rm = TRUE) > 0, 31L,
        #                                 ifelse(imp0 == 3L & var(Value , na.rm = TRUE) == 0, 32L,
        #                                        imp0))), by = grouping_key]
        # # 41: if aquastatElement is a long-term avergage variable and with vari > 0 imp0 == 4
        # # 42: if aquastatElement is a long-term avergage variable and with vari = 0 imp0 == 4
        # d[, imp0 := as.integer(ifelse(aquastatElement %in% paste0('Value_', lta) & imp0 == 31L, 41L, imp0)),by = grouping_key]
        # d[, imp0 := as.integer(ifelse(aquastatElement %in% paste0('Value_', lta) & imp0 == 32L, 42L, imp0)),by = grouping_key]
        # d[, aquastatElement := substring(aquastatElement, 7)]
        #
        # d[, (colnames(d)) := lapply(.SD, as.character), .SDcols = colnames(d)]
        # d[, Value := as.numeric(Value)]
        #
        # d
}
df_imp_pre <- PrepImputationData()
print('AQUASTAT: PrepImputationData function finished')


# Get the elements that can directly be imputed (No depending on other elements)
#df_prim <- df_imp_pre[aquastatElement %in% paste0('Value_', primary_elements)]
# 2: carried forward
# 31: linear interpolation
# 32: carried forward
# 41: carried forward
# 42: carried forward
# Get imputations
GetAquaImputations <- function(){
        d <- copy(df_imp_pre)
        df_imp <- d[, value := as.numeric(ifelse(imp0 == 2L, zoo::na.locf(Value),
                                            ifelse(imp0 %in% c(41L, 42L), zoo::na.locf(Value),
                                               ifelse(imp0 == 32L, zoo::na.locf(Value),
                                                 ifelse(imp0 == 31L, imputeTS::na.interpolation(as.numeric(Value)), Value))))),
                    by = grouping_key]

        df_prim_imp <- df_imp[, list(geographicAreaM49, aquastatElement, timePointYears, value, flagAquastat, maxID, imp0)]
        setnames(df_prim_imp, 'value', "Value")

        df_prim_imp <- df_prim_imp[!is.na(Value)]
        df_prim_imp[, (colnames(df_prim_imp)) := lapply(.SD, as.character), .SDcols = colnames(df_prim_imp)]
        df_prim_imp[, Value := as.numeric(Value)]
        df_prim_imp
}
imputed_data <- GetAquaImputations()


print('AQUASTAT: GetAquaImputations function finished')
# RECALCULATION -----------------------------------------------------------------------------------------------------------------
Recalculation <- function(){
      dataforrecalc <- prep_for_calculations(imputed_data)
      d <- copy(dataforrecalc$data)
      rules <- dataforrecalc$rules
      missing_elements <-  dataforrecalc$nonexisting
      for (i in missing_elements) {
        d[, i] <- NA
      }
      # Calculation
      dd <- copy(d)
      for(i in rules) dd <- within(dd, eval(parse(text = i)))
      dd
}
data_recalculated <- Recalculation()

# apply pv correction again
pv_corr_02 <- pv_correction_01(data_recalculated )

print('AQUASTAT: pv_correction_01 function finished')
# Get long format
reshape_data_02 <- function(){

            d <- copy(pv_corr_02)

            measure_vars <- names(d)[str_detect(names(d), 'Value_')]
            id_vars <- names(d)[str_detect(names(d), 'Value_') == FALSE]
            d[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]
            dd_rec <- melt(d, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
            setnames(dd_rec, 'Value', 'Value_recalc')
            dd_rec[, aquastatElement := substring(aquastatElement, 7)]

            dd <- merge(dd_rec, imputed_data, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'), all = TRUE)
            dd[, Value := ifelse(!is.na(Value), Value, Value_recalc)]
            dd <- dd[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, maxID, imp0)][order(geographicAreaM49, aquastatElement)]
            dd <- dd[!is.na(Value)]
            dd[, maxID := ifelse(is.na(maxID), 4L, maxID)]
            dd[, flagAquastat := ifelse(is.na(flagAquastat), 'C', flagAquastat)]
            dd[, imp0 := ifelse(is.na(imp0), '31', imp0)]
            dd[order(geographicAreaM49, aquastatElement)][]

}
finished_calculation_02 <- reshape_data_02()
print('AQUASTAT: pv_correction_01 function finished')


ExpandCoverageSDG641 <- function(){

          d <- copy(finished_calculation_02)

          sdg_df <- d[aquastatElement %in% c(4254, 4255, 4256, 4552, 4553, 4554)]
          sdg_df[, aquastatElement := paste0('Value_', aquastatElement)]
          sdg_for_calc <- data.table::dcast(sdg_df, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")
          for(i in dataforcalc$rules[68]) sdg_for_calc <- within(sdg_for_calc, eval(parse(text = i)))

          measure_vars <- names(sdg_for_calc)[str_detect(names(sdg_for_calc), 'Value_')]
          id_vars <- names(sdg_for_calc)[str_detect(names(sdg_for_calc), 'Value_') == FALSE]
          sdg_for_calc[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]

          df_melt_sdg <- melt(sdg_for_calc, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
          df_melt_sdg[, aquastatElement := substring(aquastatElement, 7)]
          df_melt_sdg <- df_melt_sdg[!is.na(Value)]

          setnames(df_melt_sdg, 'Value', 'Value_calc')
          df_merge_sdg <- merge(d, df_melt_sdg, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'), all = TRUE)
          df_merge_sdg[, Value := ifelse(!is.na(Value), Value, Value_calc)]
          df_merge_sdg[, maxID := ifelse(is.na(maxID), 4L, maxID)]
          df_merge_sdg[, flagAquastat := ifelse(is.na(flagAquastat), 'C', flagAquastat)]
          df_merge_sdg[, imp0 := ifelse(is.na(imp0), '31', imp0)]

          df_merge_sdg <- df_merge_sdg[, names(d), with = FALSE][order(geographicAreaM49, aquastatElement)]
          df_merge_sdg[]
}
df_sdg_4551 <- ExpandCoverageSDG641()

# Binding dataframes
# finished_calculation_02_sdg4551 <- rbind(finished_calculation_02[!(aquastatElement %in% unique(df_sdg_4551$aquastatElement))], df_sdg_4551)
# finished_calculation_02_sdg4551 <- finished_calculation_02_sdg4551[!is.na(Value)][]


ExpandCoverageSDG642 <- function(){
        d <- df_sdg_4551

        sdg_df <- d[aquastatElement %in% c(4263, 4188, 4549)]
        sdg_df[, aquastatElement := paste0('Value_', aquastatElement)]
        sdg_for_calc <- data.table::dcast(sdg_df, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")
        for(i in dataforcalc$rules[62]) sdg_for_calc <- within(sdg_for_calc, eval(parse(text = i)))

        measure_vars <- names(sdg_for_calc)[str_detect(names(sdg_for_calc), 'Value_')]
        id_vars <- names(sdg_for_calc)[str_detect(names(sdg_for_calc), 'Value_') == FALSE]
        sdg_for_calc[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]

        df_melt_sdg <- melt(sdg_for_calc, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
        df_melt_sdg[, aquastatElement := substring(aquastatElement, 7)]
        df_melt_sdg <- df_melt_sdg[!is.na(Value)]

        setnames(df_melt_sdg, 'Value', 'Value_calc')
        df_merge_sdg <- merge(d, df_melt_sdg, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'), all = TRUE)
        df_merge_sdg[, Value := ifelse(!is.na(Value), Value, Value_calc)]
        df_merge_sdg[, maxID := ifelse(is.na(maxID), 4L, maxID)]
        df_merge_sdg[, flagAquastat := ifelse(is.na(flagAquastat), 'C', flagAquastat)]
        df_merge_sdg[, imp0 := ifelse(is.na(imp0), '31', imp0)]

        df_merge_sdg <- df_merge_sdg[, names(d), with = FALSE][order(geographicAreaM49, aquastatElement)]
        df_merge_sdg[]
}

df_sdg_4550 <- ExpandCoverageSDG642()
print('AQUASTAT: ExpandCoverageSDG641 function has finished')


# # LTA correct
 LTACorrection <- function(){

    d <- copy(df_sdg_4550)

    # # get flags back
    # setnames(d, 'Value', 'Value_final')
    # d <- merge(d, imputed_data, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'),  all.x = TRUE)
    # d <- d[, .(geographicAreaM49, aquastatElement, timePointYears, Value_final, flagAquastat, imp0)]
    # d <- d[, flagAquastat := ifelse(is.na(flagAquastat), 'C', flagAquastat)]
    # d <- d[!is.na(Value_final)][order(geographicAreaM49, aquastatElement, timePointYears)]

    # correct
    dd <- d[, flagAquastat := ifelse(aquastatElement %in% c(41L, 42L) & str_detect(first(timePointYears), '2$|7$'),  nth(Value, 1),
                                                      nth(Value, 2)),
                               by = .(geographicAreaM49, aquastatElement)][order(geographicAreaM49, aquastatElement, timePointYears)]


    # rejoin data
    d0 <- d[!(aquastatElement %in% lta)]
    ddd <- rbind(d0, dd)
    setnames(ddd,  'Value_final', 'Value')
    ddd[order(geographicAreaM49, aquastatElement, timePointYears)][]
}
df_lta_corr <- LTACorrection()

if ( data_to_update == 'update'){
x <- fsetdiff(copy_data, df_lta_corr[, names(copy_data), with = FALSE])
x$imp0 <- NA
df_lta_corr <- rbind(df_lta_corr, x)

}
rbind(x, df_lta_corr)
print('AQUASTAT: LTACorrection  function has finished')
#
#     dfmerged_final0 <- dffinal[!(aquastatElement %in% lta)]
#     dfmerged_final_proc <- rbind(dfmerged_final0, df_lta_corr)
# # RECOVERYING FLAGS ---------
# setnames(dffinal, 'Value', 'Value_final')
# # recover <- imputed_data[, aquastatElement := substring(aquastatElement, 7)]
# dfmerged_final <- merge(dffinal, imputed_data, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'),  all.x = TRUE)
# dfmerged_final <- dfmerged_final[, .(geographicAreaM49, aquastatElement, timePointYears, Value_final, flagAquastat, imp0)]
# setnames(dfmerged_final, 'Value_final', 'Value')
# dfmerged_final <- dfmerged_final[, flagAquastat := ifelse(is.na(flagAquastat), 'C', flagAquastat)]
# dfmerged_final <- dfmerged_final[!is.na(Value)]
# # setorder(dfmerged_final, geographicAreaM49, aquastatElement)
#
# # LTA correct
# df_lta <- copy(dfmerged_final)
# df_lta_corr <- df_lta[aquastatElement %in% lta]
# df_lta_corr <- df_lta_corr[aquastatElement %in% lta,
#                            flagAquastat := ifelse(str_detect(first(timePointYears), '2$'), nth(flagAquastat, 1), nth(flagAquastat, 2)),
#        by = .(geographicAreaM49, aquastatElement)]
# dfmerged_final0 <- dfmerged_final[!(aquastatElement %in% lta)]
# dfmerged_final_proc <- rbind(dfmerged_final0, df_lta_corr)


# FLAG CONVERSION --------------------------------------------------------------------------------------------------------------
# if (data_to_update == 'legacy'){
#
# getFlagObsStatus <- function(data) {
#         if (!is.data.table(data)) data <- data.table(data)
#         dt <- data[, .(geographicAreaM49, aquastatElement, timePointYears, Value, imp0,
#                flagObservationStatus = case_when(
#                  flagAquastat == "E" ~ "X",
#                  flagAquastat == "K" ~ "E",
#                  flagAquastat == "L" ~ "E",
#                  flagAquastat == "I" ~ "E",
#                  flagAquastat == "F" ~ "E",
#                  flagAquastat == "C" ~ "C",
#                  flagAquastat == "" ~ "",
#                  flagAquastat == "M" ~ "I",
#                  flagAquastat == "Im" ~ "I")
#                  )]
#         df_fix_for_obs <- dt[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagObservationStatus, imp0)]
#         df_fix_for_obs[order(geographicAreaM49, aquastatElement, timePointYears)][]
#
# }
#
# data_flag_corr <- getFlagObsStatus(df_lta_corr)


getFlagMethod <- function(data) {
            if (!is.data.table(data)) data <- data.table(data)
            dt <- data[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagObservationStatus,
                           flagMethod = case_when(
                                       flagObservationStatus %in% c('I') & imp0 != 31L ~ "t",
                                       flagObservationStatus %in% c('I') & imp0 == 31L ~ 'e',
                                       flagObservationStatus %in% c('C') ~ 'i',
                                       flagObservationStatus %in% c('E') ~ '-',
                                       flagObservationStatus %in% c("")  ~ 'p',
                                       flagObservationStatus %in% c('X') ~ 'c')

            )]
            df_fix_for_method <- dt[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagObservationStatus, flagMethod)]
            df_fix_for_method <- df_fix_for_method[, flagObservationStatus := ifelse(flagObservationStatus == 'C', 'E', flagObservationStatus)]
            df_fix_for_method[order(geographicAreaM49, aquastatElement, timePointYears)][]

}

data_flag_corr <- getFlagMethod(data_flag_corr)

}



print('AQUASTAT: FLAG CONVERSION  function has finished')
# Get the final dataset
final_data <- data_flag_corr[complete.cases(data_flag_corr), ]


# right % values
cap_100_percent <- function(data){
      val_rules <- ReadDatatable('validation_rules_clone')
      val_rules_100 <- val_rules[rhs %in% c("100.0", "100")]
      val_rules_100[, lhs := substring(lhs, 7)]
      data_corr <- data[, Value := ifelse(aquastatElement %in% val_rules_100$lhs & Value > 100, 100, Value)]
      data_corr[, geographicAreaM49 := as.character(geographicAreaM49)]
      data_corr[, timePointYears := as.character(timePointYears)]
      data_corr[order(geographicAreaM49, aquastatElement, timePointYears)][]
}
final_corr <- cap_100_percent(final_data)

print('AQUASTAT: DATA READY TO SAVE')

# SAVE DATA -------------------------------------------------------------------------------------------------------------------
saveRDS(final_corr, file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/output', 'update.rds'))
stats <- SaveData("Aquastat", "aquastat_update", final_corr, waitTimeout = 10000)
paste0("Aquastat legacy data has been updated successfully!!!")
print(paste0('Ending at: ', Sys.time()))



