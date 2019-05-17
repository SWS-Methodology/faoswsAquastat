##' AquastatExternal module
##' Author: Francy Lisboa
##' Date: 22/03/2019
##' Purpose: Download and harmonize external AQuASTAT data.

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
  library(xlsx)
})



if(CheckDebug()){
  library(faoswsModules)
  SETTINGS = ReadSettings("~./github/faoswsAquastatExternal/sws.yml")
  Sys.setenv("R_SWS_SHARE_PATH" = SETTINGS[["share"]])
  R_SWS_SHARE_PATH = SETTINGS[["share"]]
  SetClientFiles(SETTINGS[["certdir"]])
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}



# Download aquastat_external_sources data table
sources <- fread('~/github/aqua_docs/FrancyJGLisboa.github.io/tables/aqua_external_sources.csv')
src <- sources[, .(element_code, source, source_item_code, source_element_code, data_link)]

# JMP(WHO/UNICEF)


jmp <- 'JMP(WHO/UNICEF)'
jmp_link <- as.character(src[source %in% jmp, c(data_link)][1])
tf_jmp <- tempfile()
download.file(jmp_link, tf_jmp, mode = "wb" )
jmp_df <- read_xlsx(tf_jmp, 'Water', col_names = TRUE)[-c(1,2), c(1, 2, 3, 6, 11, 16)]
jmp_names <- c('geographicAreaM49_description', 'ISO3', 'timePointYears', 'Value_4114', 'Value_4115', 'Value_4116')
names(jmp_df) <- jmp_names
jmp_df <- data.table(jmp_df)[!is.na(timePointYears)]
jmp_df[!is.na(Value_4114)]
jmp_df[!is.na(Value_4115)]
jmp_df[!is.na(Value_4116)]
measure_vars <- names(jmp_df)[str_detect(names(jmp_df), 'Value_')]
id_vars <- names(jmp_df)[str_detect(names(jmp_df), 'Value_') == FALSE]
jmp_proc <- melt(jmp_df, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')






# UNDP ------------------------------------------------------------------------------------------------------------

undp <- 'UNDP'
undp_link <- as.character(src[source %in% unpd , c(data_link)][1])
tf_undp <- tempfile()
download.file(undp_link, tf_undp, mode = "wb" )
undp_df <- data.table(read_xlsx(tf_undp, col_names = TRUE))

# filter
source_codes <- src[source %in% unpd, source_element_code]
undp_df <- undp_df[indicator_id %in% source_codes]
undp_to_select <- names(undp_df)[str_detect(names(undp_df), 'id|name|iso|[0-9]')]
undp_df <- undp_df[, undp_to_select, with = FALSE]

#  pivot long
id_vars <- names(undp_df)[str_detect(names(undp_df), 'id|name|iso')]
measure_vars <- names(undp_df)[str_detect(names(undp_df), 'id|name|iso') == FALSE]
undp_proc <- melt(undp_df, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'timePointYears', value.name = 'Value')
undp_proc <- undp_proc[timePointYears != '9999']
undp_proc <- undp_proc[,indicator_id := as.character(indicator_id)]

# merge
src_aqua_code <- src[, .(element_code,source_element_code)]
aqua_source_merged <- merge(undp_proc, src_aqua_code, by.x = 'indicator_id', by.y = 'source_element_code')


aqua_source_merged <- aqua_source_merged[, .(country_name, iso3, element_code, timePointYears, Value)]
undp_processed <- aqua_source_merged[!is.na(Value)]
names(undp_processed) <- c('geographicAreaM49_description', 'ISO3', 'aquastatElement', 'timePointYears', 'Value')






# ILO ---------------------------------------------------------------------------------------------------------------------------------

# Read in the data
ilo <- 'ILO'
ilo_link <- as.character(src[source %in% ilo , c(data_link)])
tf_ilo <- tempfile(fileext = '.tar.gz')
download.file(ilo_link, tf_ilo, mode = 'wb')
ilo_df <- data.table(read.csv(gzfile(tf_ilo)))

# Filter
ilo_df <- ilo_df[sex == 'SEX_T' & classif1 == 'AGE_AGGREGATE_TOTAL']
ilo_df_filter <- ilo_df[, .(ref_area, indicator, time, obs_value)]

# merge
src_aqua_code <- src[, .(element_code,source_item_code)]
aqua_source_merged <- merge(ilo_df_filter, src_aqua_code, by.x = 'indicator', by.y = 'source_item_code')
ilo_processed <- aqua_source_merged[!is.na(obs_value)]
ilo_processed <- ilo_processed[, .(ref_area, element_code, time, obs_value)]
names(ilo_processed) <- c('ISO3', 'aquastatElement', 'timePointYears', 'Value')



# FAOSTAT ----------------------------------------------------------------------------------------------------------------------

# get all data links
fs <- 'FAOSTAT'
fs_link <- as.character(src[source %in% fs , c(data_link)])



# filter links by FAOSTAT topic
land_link <- fs_link[str_detect(fs_link, 'Land')][1]
pop_link <- fs_link[str_detect(fs_link, 'Population')][1]
food_link <- fs_link[str_detect(fs_link, 'Food')][1]
macro_link <- fs_link[str_detect(fs_link, 'Macro')][1]

# source codes
item_code <- src[source %in% fs, c(source_item_code)]
element_code <- src[source %in% fs, c(source_element_code)]


# land
tf_land <- tempfile()
download.file(land_link, tf_land)
td_land <- tempdir()
file.name <- unzip(tf_land, exdir = td_land)
land <- fread(file.name)
land <- land[`Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]




# population
tf_pop <- tempfile()
download.file(pop_link, tf_pop)
td_pop <- tempdir()
file.name <- unzip(tf_pop, exdir = td_pop)
pop <- fread(file.name)
pop <- pop[`Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]



# macro
tf_macro <- tempfile()
download.file(macro_link, tf_macro)
td_macro <- tempdir()
file.name <- unzip(tf_macro, exdir = td_macro)
macro <- fread(file.name)
macro <- macro[`Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]

