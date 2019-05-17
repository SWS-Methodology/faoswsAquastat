paste0(print('AQUASTAT STARTS'), Sys.time())
##' AquastatUpdate module
##' Author: Francy Lisboa
##' Date: 29/04/2019
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
  SETTINGS = ReadSettings("~./github/faoswsAQUASTAT/faoswsAquastatUpdate/sws.yml")
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


# USER PARAMETERS ------
first_time <- as.character(swsContext.computationParams$first_time)
subsetting <- as.character(swsContext.computationParams$return_data_subset)

# HELPERS -----
# Create a generic functioon that converts all features but Value into character vector
sameclass <- function(d){
    d[, (colnames(d)) := lapply(.SD, as.character), .SDcols = colnames(d)]
    d[, Value := as.numeric(Value)]
    d
}

# to remove any duplicates from inputs
removeDuplicates <- function(data) {
  unique(data) %>%  tbl_df() %>% distinct() %>% filter(!is.infinite(Value)) %>% setDT()
  }
# LOAD DATA TABLES --------
# get LTA variables
aqua_reference <- ReadDatatable("aquastat_reference")
lta <- aqua_reference[lta == 1L, element_code]

# get m49 geographic codes
cg <- ReadDatatable("a2017regionalgroupings_sdg_feb2017")
m49_areas <- unique(cg$m49_code)

# get the calculation rules
calc <- ReadDatatable('calculation_rule')

# GENERIC OBJECTS --------
proc_rules <- stringr::str_replace_all(calc$calculation_rule, "\\[([0-9]+)\\]", "Value_\\1")
rule_elements <- paste0("Value_", sort(unique(unlist(str_extract_all(calc$calculation_rule, regex("(?<=\\[)[0-9]+(?=\\])"))))))
lhs <- paste0("Value_",sort(substring(calc$calculation_rule, 2, 5)))
rhs <- sort(setdiff(rule_elements, lhs))


# LOAD INPUTS ----------
# Read in aquastat_external dataset
imputKey <- DatasetKey(
  domain = "Aquastat",
  dataset = "aquastat_external",
  dimensions = list(
    Dimension(name = "geographicAreaM49",
              keys = GetCodeList('Aquastat', "aquastat_external", 'geographicAreaM49')[type == 'country', code]),
    Dimension(name = "aquastatElement", keys = GetCodeList('Aquastat', "aquastat_external", 'aquastatElement')[, code]),
    Dimension(name = "timePointYears", keys = as.character(1961:substring(Sys.time(), 1, 4)))
  )
)
data_external <- GetData(imputKey, flags = TRUE)

if (nrow(data_external) < 1) {
data_external <- fread(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/output/External/aquastat_external.csv'))
}

data_external$id <- 2L
data_external <- sameclass(data_external)
data_external <- removeDuplicates(data_external)

# Read in aquastat_questionnaire data from share drive
data_quest <- fread(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/data/Update/aquastat_questionnaire.csv'))
data_quest$id <- 3L
data_quest <- sameclass(data_quest)
# Get geographic codes as UNSDM49
data_quest[, geographicAreaM49 := fs2m49(as.character(geographicAreaM49))]
data_quest <- removeDuplicates(data_quest)



# If the module is being executed by the first time,
# use aquastat_enr_all_countries as the data to update otherwise use aquastat_update
if (first_time == 'yes') {
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
          data_to_update <- GetData(imputKey, flags = TRUE)
          data_to_update$id <- 1L
          data_to_update <- sameclass(data_to_update)
          data_to_update <- removeDuplicates(data_to_update)

} else {
    imputKey <- DatasetKey(
        domain = "Aquastat",
        dataset = "aquastat_update",
        dimensions = list(
          Dimension(name = "geographicAreaM49",
                    keys = GetCodeList('Aquastat', "aquastat_update", 'geographicAreaM49')[type == 'country', code]),
          Dimension(name = "aquastatElement", keys = GetCodeList('Aquastat', "aquastat_update", 'aquastatElement')[, code]),
          Dimension(name = "timePointYears", keys = as.character(1961:substring(Sys.time(), 1, 4)))
        )
      )
      data_to_update <- GetData(imputKey, flags = TRUE)

      if (nrow(data_to_update) < 1)
          data_to_update <- fread(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/output/Update/aquastat_update.csv'))

      data_to_update <- data_to_update[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagObservationStatus, flagMethod)]
      data_to_update$id <- 1L
      data_to_update <- sameclass(data_to_update)
      data_to_update <- removeDuplicates(data_to_update)
}
print('AQUASTAT INPUTS PRE - PROCESSED')

# create a list of input datasets
ColumnNameHarmonization <- function(d){
        d <- sameclass(d)
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

         dd <- dd[geographicAreaM49 %in% sort(m49_areas)][order(geographicAreaM49, aquastatElement, timePointYears)]

        return(dd)
}

dt1 <- rbindlist(lapply(list(data_to_update, data_external, data_quest[, flagAquastat := ""]), ColumnNameHarmonization))




#####################################################################################################
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
if (first_time == 'yes')
    copy_data <- copy_data[!aquastatElement %in% c('4100')]

# Get the revision data to checks
revision_data <- starting_input()[[2]]
print('AQUASTAT DATA SOURCE MERGING has finished')



# FLAG OBSERVATION --------------------------------------------------------------------------------------------------------------
if (first_time %in% 'yes') {
        getFlagObsStatus <- function(data) {
                        d <- copy(data)
                        dt <- d[, .(geographicAreaM49, aquastatElement, timePointYears, Value, maxID,
                                       flagAquastat = case_when(
                                         is.na(flagAquastat) & maxID %in% c(3L) ~ "",
                                         flagAquastat == "X" & maxID %in% c(2L) ~ "X",
                                         flagAquastat == "E" & maxID %in% c(1L, 2L) ~ "X",
                                         flagAquastat == "K" & maxID %in% c(1L, 2L) ~ "E",
                                         flagAquastat == "L" & maxID %in% c(1L, 2L) ~ "E",
                                         flagAquastat == "I" & maxID %in% c(1L, 2L) ~ "E",
                                         flagAquastat == "F" & maxID %in% c(1L, 2L) ~ "E",
                                         flagAquastat == "C" & maxID %in% c(1L, 2L) ~ "C",
                                         flagAquastat == ""  & maxID %in% c(1L, 2L) ~ "",
                                         flagAquastat == "M" & maxID %in% c(1L, 2L) ~ "M")

                        )]

                        df_fix_for_obs <- dt[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, maxID)]
                        df_fix_for_obs[order(geographicAreaM49, aquastatElement, timePointYears)][]
                    }
        copy_data_flagObs_corr <- getFlagObsStatus(copy_data)
        copy_data_flagObs_corr[, flagAquastat := ifelse(is.na(flagAquastat) & maxID == 3L, "", flagAquastat)]
        copy_data_flagObs_corr <- copy_data_flagObs_corr[order(geographicAreaM49, aquastatElement, timePointYears)][]
} else {
       copy_data_flagObs_corr <- copy(copy_data)
       copy_data_flagObs_corr <- copy_data_flagObs_corr[order(geographicAreaM49, aquastatElement, timePointYears)][]
}


## AQUASTAT INITIALIZATION -------
print('AQUASTAT CALCULATION has started')
# Preparing data for calculation
prep_for_calculations <- function(data){
            d <- copy(data)
            intersection <- sort(intersect(rule_elements, paste0("Value_", sort(unique(d$aquastatElement)))))
            components_to_remove <- sort(setdiff(rhs, intersection))
            data1 <- d[, .(geographicAreaM49, aquastatElement = paste0("Value_", aquastatElement), timePointYears, Value)]
            data1 <- sameclass(data1)
            data_for_calc <- data.table::dcast(data1, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = 'Value')
            return(list(data = data_for_calc,  rules = proc_rules, indicators = lhs, nonexisting = components_to_remove))
}
# call PrepForCalc function on copy_data
dataforcalc <- prep_for_calculations(copy_data_flagObs_corr)

# Add nonexisting elements to the pivot_wider dataset so that the calculation do not crash.
missing_elements <-  dataforcalc$nonexisting
for (i in missing_elements) {
  dataforcalc$data[, i] <- as.numeric(NA)
}

# CALCULATIONS -- FIRST ROUND
GetCalculations <- function(data){
      d <- copy(data)
      for(i in dataforcalc$rules) d <- within(d, eval(parse(text = i)))
      d[order(geographicAreaM49, timePointYears)]
}

if (first_time == 'yes') {
# reshaping to correct values
      PVapplicableElement <- function(data){
            df_calc <- copy(data)
            zeroelements <- paste0('Value_', c('4308', '4309', '4310', '4312', '4316', '4314', '4315', '4264', '4265', '4451'))
            df_calc[, (zeroelements) := lapply(.SD, function(x) { x <- as.numeric(ifelse(all(is.na(x)), 0, x))}), .SDcols = zeroelements]
            df_calc[]
      }
      to_calc <- dataforcalc$data
      data_calculated <- GetCalculations(PVapplicableElement(to_calc))
} else {
      to_calc <- dataforcalc$data
      data_calculated <- GetCalculations(to_calc)
}
print('AQUASTAT INDICATORS have been added: first round')


# Reapply correction if firste_time == 'yes'
primaryVariableCorretion <- function(data) {
  cg <- 'geographicAreaM49'
  d <-  copy(data)
  if (first_time == 'yes') {
    # Replace NAs by primary variable value when suitable
    pvc <- function(data){
      d[, Value_4263 := ifelse(is.na(Value_4263) & !is.na(Value_4253), Value_4253, Value_4263), by = cg ]
      d[, Value_4311 := ifelse(is.na(Value_4311) & !is.na(Value_4308), Value_4308, Value_4311), by = cg ]
      d[, Value_4313 := ifelse(is.na(Value_4313) & !is.na(Value_4311), Value_4311, Value_4313), by = cg ]
      d[, Value_4317 := ifelse(is.na(Value_4317) & !is.na(Value_4313), Value_4313, Value_4317), by = cg ]
      d[, Value_4459 := ifelse(is.na(Value_4459) & !is.na(Value_4309), Value_4309, Value_4459), by = cg ]
      d
    }
    pv_corrected_data <- pvc (data)
    pv_corrected_data
  } else {
    pv_corrected_data <- copy(data)
    pv_corrected_data
  }
  pv_corrected_data
}
pv_corrected_data <- primaryVariableCorretion(data_calculated)
print('AQUASTAT PRIMARy VARIABLE CORRECTION has been done: first round')


# reshape and get back flags
reshapeAquastat <- function(data){
        d <- copy(data)
        measure_vars <- names(d)[str_detect(names(d), 'Value_')][]
        id_vars <- names(d)[str_detect(names(d), 'Value_') == FALSE]
        df_melt <- melt(d, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
        df_melt[, aquastatElement := substring(aquastatElement, 7)][order(geographicAreaM49, aquastatElement, timePointYears)]
      }
reshaped_data <- reshapeAquastat(pv_corrected_data)

mergeAquastat <- function(data){
  # NOTE: At this point, if the dataset to update is the aquastat_update and the new data
  # (questionnaire and/or external) DO NOT BRING NEW YEARS, the output of mergeAquastat function
  # should be a dataset with the SAME size as the initial data_to_update input dataset.
        by_grp = c("geographicAreaM49",'aquastatElement', "timePointYears")
        d_proc <- copy(reshaped_data)
        d_origin <- copy(copy_data_flagObs_corr)
        d_proc <- d_proc[!is.na(Value)][,.(geographicAreaM49, aquastatElement, timePointYears, Value)]
        d_merged <- merge(d_proc, d_origin, by = by_grp, all = TRUE)

        res <- d_merged[, `:=` (Value = ifelse(!is.na(Value.y), Value.y, Value.x),
                         maxID = ifelse(!is.na(maxID), maxID, 4L),
                         flagAquastat = ifelse(is.na(flagAquastat), 'C', flagAquastat))][,
                                                                                         .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, maxID)]
        res[!is.infinite(Value)]
  }
calculation_finished <- mergeAquastat(reshaped_data)
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


if (first_time == 'yes'){
    di <- copy(calculation_finished)
    di <- di[flagAquastat != 'M']
} else {
    di <- copy(calculation_finished)
}


grouping_key <- c("geographicAreaM49", "aquastatElement")
if (first_time == 'yes'){
  df_exp <- aqua_expand_ts(di , start_year = NULL, end_year = substring(Sys.time(), 1, 4), grouping_key = grouping_key)
  df_exp[, maxID := ifelse(is.na(maxID), 5L, maxID)]
  df_exp[, flagAquastat := ifelse(is.na(flagAquastat), 'I', flagAquastat)]
} else {
  df_exp <- aqua_expand_ts(di , start_year = NULL, end_year = max(di$timePointYears), grouping_key = grouping_key)
  df_exp[, maxID := ifelse(is.na(maxID), 5L, maxID)]
  df_exp[, flagAquastat := ifelse(is.na(flagAquastat), 'I', flagAquastat)]
}

paste0(print('AQUASTAT TS EXPANSION is done'), Sys.time())


# Preparing for imputations
PrepImputationData <- function() {
              lta_chr <- paste0('Value_', lta)
              d <- copy(df_exp)
              # df <- copy(data)
              d[, ts_len := .N, by = grouping_key]
              d[, imp0 := as.integer(case_when(
                var(Value, na.rm = TRUE) > 0 ~ 31L,  # more than one observation and different values
                var(Value, na.rm = TRUE) == 0 ~ 32L, # more than one observation but with same value
                sum(!is.na(Value), na.rm = TRUE) == 1 ~ 2L,    # only one observation
                sum(is.na(Value), na.rm = TRUE) == ts_len ~ 1L # all missing
               )), by = grouping_key][,
                 imp0 := as.integer(ifelse(aquastatElement %in% lta_chr & imp0 == 32L , 42L,
                                ifelse(aquastatElement %in% lta_chr & imp0 == 31L, 41L,
                                       ifelse(aquastatElement %in% lta_chr & sum(is.na(Value), na.rm = TRUE) == 1, 5L, imp0))))
               , by = grouping_key][]
              d[, aquastatElement := substring(aquastatElement, 7)]
              d[order(geographicAreaM49, aquastatElement, timePointYears)]
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
        # d[, Value := ifelse(is.infinite(Value), NA, Value)]
        df_imp <- d[, value := as.numeric(ifelse(imp0 == 2L, zoo::na.locf(Value),
                                            ifelse(imp0 %in% c(41L, 42L), zoo::na.locf(Value),
                                               ifelse(imp0 == 32L, zoo::na.locf(Value),
                                                 ifelse(imp0 == 31L, imputeTS::na.interpolation(as.numeric(Value)), Value))))),
                    by = grouping_key]

        df_prim_imp <- df_imp[, list(geographicAreaM49, aquastatElement, timePointYears, value, flagAquastat, maxID, imp0)]
        setnames(df_prim_imp, 'value', "Value")

        df_prim_imp <- df_prim_imp[!is.na(Value)]
        df_prim_imp[, (colnames(df_prim_imp)) := lapply(.SD, as.character), .SDcols = colnames(df_prim_imp)]
        df_prim_imp[, Value := as.numeric(Value)][order(geographicAreaM49, aquastatElement)]
        print('AQUASTAT: LTA correction has started')
        df_lta <- df_prim_imp[aquastatElement %in% lta][,
          Value := Value[timePointYears == first(timePointYears[str_detect(timePointYears, '2$|7$')])], by = grouping_key
        ]

        df_prim_imp <- rbind(df_prim_imp[!aquastatElement %in% lta],   df_lta)

        print('AQUASTAT: LTA correction has finished')
        df_prim_imp[!is.na(Value)][order(geographicAreaM49, aquastatElement, timePointYears)][]

}
imputed_data <- removeDuplicates(sameclass(GetAquaImputations()))
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


# Correct PV again
pv_corrected_data <- primaryVariableCorretion(data_recalculated)
print('AQUASTAT PRIMARY VARIABLE CORRECTION has been done: second round')

# reshape it again
reshaped_data <- suppressWarnings(reshapeAquastat(pv_corrected_data))

# Merge and recovery data brought by recalculation
mergeAquastat2 <- function(data){
        by_grp = c("geographicAreaM49",'aquastatElement', "timePointYears")
        d_proc <- copy(data)
        d_origin <- copy(imputed_data)
        d_proc <- d_proc[!is.na(Value)][,.(geographicAreaM49, aquastatElement, timePointYears, Value)]
        d_merged <- merge(d_proc, d_origin, by = by_grp, all = TRUE)
        res <- d_merged[, `:=` (Value = ifelse(!is.na(Value.y), Value.y, Value.x),
                         maxID = ifelse(!is.na(maxID), maxID, 4L),
                         flagAquastat = ifelse(is.na(flagAquastat), 'C', flagAquastat))][,
                                                                                         .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, maxID, imp0)]
        res[order(geographicAreaM49, aquastatElement, timePointYears)]
}
recalculation_finished <- mergeAquastat2(reshaped_data)
print('AQUASTAT RECALCULATION finished')


print('AQUASTAT EXPANDING GEO COVER SDG if needed')
ExpandCoverageSDG641 <- function(){
          d <- copy(recalculation_finished)

          sdg_df <- d[aquastatElement %in% c(4254, 4255, 4256, 4552, 4553, 4554)]
          sdg_df[, aquastatElement := paste0('Value_', aquastatElement)]
          sdg_for_calc <- data.table::dcast(sdg_df, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")
          for(i in dataforcalc$rules[66]) sdg_for_calc <- within(sdg_for_calc, eval(parse(text = i)))

          measure_vars <- names(sdg_for_calc)[str_detect(names(sdg_for_calc), 'Value_')]
          id_vars <- names(sdg_for_calc)[str_detect(names(sdg_for_calc), 'Value_') == FALSE]
          sdg_for_calc[, (measure_vars) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = measure_vars]

          df_melt_sdg <- melt(sdg_for_calc, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
          df_melt_sdg[, aquastatElement := substring(aquastatElement, 7)]
          df_melt_sdg <- df_melt_sdg[!is.na(Value)]

          setnames(df_melt_sdg, 'Value', 'Value_calc')
          df_merge_sdg <- merge(d, df_melt_sdg, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'), all = TRUE)
          df_merge_sdg[, Value := ifelse(!is.na(Value), Value, ifelse(!is.na(Value_calc), Value_calc, NA))]
          df_merge_sdg[, maxID := ifelse(is.na(maxID), 4L, maxID)]
          df_merge_sdg[, flagAquastat := ifelse(is.na(flagAquastat), 'C', flagAquastat)]
          df_merge_sdg[, imp0 := ifelse(is.na(imp0), '31', imp0)]
          df_merge_sdg <- df_merge_sdg[, names(d), with = FALSE][order(geographicAreaM49, aquastatElement, timePointYears)]
          df_merge_sdg
}
df_sdg_4551 <- removeDuplicates(sameclass(ExpandCoverageSDG641()))


ExpandCoverageSDG642 <- function(){
        d <- copy(df_sdg_4551)
        sdg_df <- d[aquastatElement %in% c(4263, 4188, 4549)]
        sdg_df[, aquastatElement := paste0('Value_', aquastatElement)]
        sdg_for_calc <- data.table::dcast(sdg_df, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")
        for(i in dataforcalc$rules[60]) sdg_for_calc <- within(sdg_for_calc, eval(parse(text = i)))

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

        df_merge_sdg <- df_merge_sdg[, names(d), with = FALSE][order(geographicAreaM49, aquastatElement, timePointYears)]
        df_merge_sdg[!is.infinite(Value)]
}
df_sdg_4550 <- removeDuplicates(sameclass(ExpandCoverageSDG642()))
print('AQUASTAT: ExpandCoverageSDG641 and SDG642 function have finished')

print('AQUASTAT: flagMethod definition on the fly has started')
# FLAG METHOD -----------------------------------------------------------------------------------------------------------------------------
if (first_time == 'yes') {
  getFlagMethod <- function() {
    d <- removeDuplicates(copy(df_sdg_4550))
    dt <- d[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat,maxID,
                flagMethod = case_when(
                  flagAquastat %in% c('I') & imp0 != 31L ~ "t",
                  flagAquastat %in% c('I') & imp0 == 31L ~ 'e',
                  flagAquastat %in% c('C') ~ 'i',
                  flagAquastat %in% c('E') ~ '-',
                  flagAquastat %in% c("")  ~ 'p',
                  flagAquastat %in% c('X') ~ 'c')

    )]
    dt1 <- dt[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, flagMethod, maxID)]
    dt1[, flagAquastat := ifelse(flagAquastat == 'C', 'E', flagAquastat)]
    dt1[maxID == 3L, flagMethod := 'q']
    setnames(dt1, 'flagAquastat', 'flagObservationStatus')
    dt1[order(geographicAreaM49, aquastatElement, timePointYears)][]

  }
  data_final <- getFlagMethod()
  data_final <- data_final [complete.cases(data_final), ]

} else {
  getFlagMethod <- function() {
    d <- removeDuplicates(copy(df_sdg_4550))
    dt <- d[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, maxID,
                flagMethod = case_when(
                  flagAquastat %in% c('I') & imp0 != 31L  ~ "t",
                  flagAquastat %in% c('I') & imp0 == 31L  ~ 'e',
                  flagAquastat %in% c('E') ~ 'i',
                  flagAquastat %in% c('C') ~ 'i',
                  flagAquastat %in% c("")  ~ 'p',
                  flagAquastat %in% c('X') ~ 'c')

    )]
    dt1 <- dt[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, flagMethod,maxID)]
    dt1[, flagAquastat := ifelse(flagAquastat == 'C', 'E', flagAquastat)]
    dt1[maxID == 3L, flagMethod := 'q']
    setnames(dt1, 'flagAquastat', 'flagObservationStatus')
    dt1[order(geographicAreaM49, aquastatElement, timePointYears)][]

  }
  data_final <- getFlagMethod()
  data_final <- data_final [complete.cases(data_final),]

}
print('AQUASTAT: getFlagMethod  function has finished')

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
data_to_save <- removeDuplicates(sameclass(cap_100_percent(data_final)))
paste0(print('AQUASTAT: DATA READY TO SAVE '), Sys.time())

paste0(print('AQUASTAT GETTING METADATA'), Sys.time())
# Save metada SOURCE, element COMMENT
if (first_time == 'yes') {
  metadata2save <- copy(data_to_save)
  metadata2save <- metadata2save[, `:=` (Metadata = 'SOURCE',
                                         Metadata_Element = 'COMMENT',
                                         Metadata_Value = case_when(
                                         maxID == 1L ~ 'From latest SWS aquastat_enr',
                                         maxID == 2L ~ 'From latest SWS aquastat_external',
                                         maxID == 3L ~ 'From Share Drive aquastat_questionnaire',
                                         maxID == 4L ~ 'Calculated on the fly',
                                         maxID == 5L ~ 'Imputation on the fly',
                                         TRUE ~ 'unknown'),
                                         Metadata_Language = 'en')]

  metadata2save <- na.omit(metadata2save)
  metadata2save <- metadata2save[, c('flagObservationStatus', 'flagMethod', 'Value') := NULL][order(geographicAreaM49, aquastatElement, timePointYears)]
  metadata2save[, maxID := NULL]

} else {
  metadata2save <- copy(data_to_save)
  metadata2save <- metadata2save[, `:=` (Metadata = 'SOURCE',
                                         Metadata_Element = 'COMMENT',
                                         Metadata_Value = case_when(
                                         maxID == 1L ~ 'From latest SWS aquastat_update',
                                         maxID == 2L ~ 'From latest SWS aquastat_external',
                                         maxID == 3L ~ 'From Share Drive aquastat_questionnaire',
                                         maxID == 4L ~ 'Calculated on the fly',
                                         maxID == 5L ~ 'Imputation on the fly',
                                         TRUE ~ 'unknown'),
                                         Metadata_Language = 'en')]


  metadata2save <- na.omit(metadata2save)
  metadata2save <- metadata2save[, c('flagObservationStatus', 'flagMethod', 'Value') := NULL][order(geographicAreaM49, aquastatElement, timePointYears)]
  metadata2save[, maxID := NULL]
  }


# remove maxID
data_to_save[, maxID := NULL]

# SAVE DATA -------------------------------------------------------------------------------------------------------------------
saveRDS(data_to_save, file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/output/Update/', 'aquastat_update.rds'))
write.csv(data_to_save, file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/output/Update/', 'aquastat_update.csv'), row.names = FALSE)

if (subsetting == 'no') {
  paste0(print('AQUASTAT SAVING'), Sys.time())
  stats <- SaveData("aquastat", dataset = swsContext.datasets[[1]]@dataset,
                    data = data_to_save,
                    metadata = metadata2save,
                    waitTimeout = 10000)

} else {
  paste0(print('AQUASTAT SAVING'), Sys.time())
  stats <- SaveData("aquastat", dataset = swsContext.datasets[[1]]@dataset,
                    data = data_to_save[1:10000],
                    metadata = metadata2save[1:10000],
                    waitTimeout = 10000)
}

paste0(print('AQUASTAT SAVED'), Sys.time())
paste0("AquastatUpdate module ran successfully!!!",
       stats$inserted, " observations written, ",
       stats$ignored, " weren't updated, ",
       stats$discarded, " had problems.")



