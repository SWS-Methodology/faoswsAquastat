print(paste0('AQUASTAT external starts at :', Sys.time()))
##' AquastatExternal module
##' Author: Francy Lisboa
##' Date: 27/03/2019
##' Purpose: Automates the data aquisition from external sources that will serve the faoswsAquastatUpdate module

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
  library(openxlsx)
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
sources <- ReadDatatable('aqua_external_sources')
src <- sources[, .(element_code, source, source_item_code, source_element_code, data_link)]

# download iso -> m49 data table
m49_to_iso <- ReadDatatable('m49_fs_iso_mapping')
m49_to_iso <- m49_to_iso[, lapply(.SD, as.character)]
names(m49_to_iso) <- c('group_area_code', 'group_area_name', 'fs_area_code', 'fs_area_name', 'M49', 'ISO2', 'ISO3')

# global variables
start_year <- 1961
end_year <- str_sub(Sys.time(),1, 4)

# ocde_link <- 'http://stats.oecd.org/Download.ashx?type=csv&Delimiter=%2c&IncludeTimeSeriesIdentifiers=False&LabelType=CodeAndLabel&LanguageCode=en'
# tf_ocde <- tempfile()
# download.file(ocde_link, tf_ocde)
# ocde_df<- fread(ocde_jmp)


## Download aquastat_external_sources data table

# print('AQUASTAT external: JMP 1')
# # JMP(WHO/UNICEF) ----------------------------------------------------------------------------------------------------------------------
# jmp_link <- src$data_link[str_detect(src$data_link, 'washdata')][1]
# tf_jmp <- paste(tempfile(), '.xlsx', sep = '')
# download.file(jmp_link, tf_jmp, mode = 'wb')
#
# # if any change happens to the position of relevant columns, modify the line below accordingly.
# jmp_df<- data.table(read.xlsx(tf_jmp, sheet = 'Water', colNames = TRUE)[-c(1,2), c(1, 2, 3, 6, 11, 16)])
# jmp_names <- c('geographicAreaM49_description', 'ISO3', 'timePointYears', 'Value_4114', 'Value_4115', 'Value_4116')
# names(jmp_df) <- jmp_names
# jmp_df <- data.table(jmp_df)[!is.na(timePointYears)]
# jmp_df <- jmp_df[!is.na(Value_4114)]
# jmp_df <- jmp_df[!is.na(Value_4115)]
# jmp_df <- jmp_df[!is.na(Value_4116)]
# measure_vars <- names(jmp_df)[str_detect(names(jmp_df), 'Value_')]
# id_vars <- names(jmp_df)[str_detect(names(jmp_df), 'Value_') == FALSE]
# jmp_proc <- melt(jmp_df, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
#
#
#
# # ISO to M49
# # jmp_proc <- jmp_proc[, Value := suppressWarnings(as.numeric(as.character(Value)))]
# jmp_proc <- jmp_proc[, aquastatElement := as.character(aquastatElement)]
#
# area_map <- m49_to_iso[, .(ISO3, M49)]
#
# jmp_source_merged <- dplyr::left_join(jmp_proc, area_map, by = c('ISO3')) %>%
#   tbl_df() %>%
#   dplyr::arrange(M49, aquastatElement) %>%
#   #filter out  missing m49 codes
#   filter(!is.na(M49)) %>%
#   data.table()
#
# setnames(jmp_source_merged, 'M49', 'geographicAreaM49')
# jmp_processed <- jmp_source_merged[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
# jmp_processed <- jmp_processed[, aquastatElement := substring(aquastatElement, 7)]
# jmp_processed <- jmp_processed[, flagObservationStatus := 'X']
# jmp_processed <- jmp_processed[, flagMethod := 'c']
# jmp_processed <- jmp_processed[complete.cases(jmp_processed),]
#


#
# print('AQUASTAT external: UNDP 1')
# # UNDP ------------------------------------------------------------------------------------------------------------
# datasource <- src$source[str_detect(src$source, 'UNDP')][1]
# undp_link <- src$data_link[str_detect(src$data_link, 'undp')][1]
# tf_undp <- paste(tempfile(), '.xlsx', sep = '')
# download.file(undp_link, tf_undp, mode = 'wb' )
#
# print('READ UNDP')
# undp_df <- data.table(read.xlsx(tf_undp, sheet = 'Data', colNames = TRUE))
# print('READ UNDP FIN')
# # filter
# source_codes <- src[source %in% datasource, source_element_code]
# undp_df <- undp_df[indicator_id %in% source_codes]
# undp_to_select <- names(undp_df)[str_detect(names(undp_df), 'id|name|iso|[0-9]')]
# undp_df <- undp_df[, undp_to_select, with = FALSE]
#
# #  pivot longer
# id_vars <- names(undp_df)[str_detect(names(undp_df), 'id|name|iso')]
# measure_vars <- names(undp_df)[str_detect(names(undp_df), 'id|name|iso') == FALSE]
# undp_proc <- melt(undp_df, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'timePointYears', value.name = 'Value')
# undp_proc <- undp_proc[timePointYears != '9999']
# undp_proc <- undp_proc[,indicator_id := as.character(indicator_id)]
#
# # merge
# src_aqua_code <- src[, .(element_code,source_element_code)]
# undp_source_merged <- merge(undp_proc, src_aqua_code, by.x = 'indicator_id', by.y = 'source_element_code')
#
# undp_source_merged <- undp_source_merged[, .(country_name, iso3, element_code, timePointYears, Value)]
# undp_proc <- undp_source_merged[!is.na(Value)]
# names(undp_proc) <- c('geographicAreaM49_description', 'ISO3', 'aquastatElement', 'timePointYears', 'Value')
#
# undp_processed <- dplyr::left_join(undp_proc, area_map, by = c('ISO3')) %>%
#   tbl_df() %>%
#   dplyr::arrange(M49, aquastatElement) %>%
#   #filter out  missing m49 codes
#   dplyr::filter(!is.na(M49)) %>%
#   data.table()
#
# setnames(undp_processed, 'M49', 'geographicAreaM49')
# undp_processed <- undp_processed[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
# undp_processed <- undp_processed[, flagObservationStatus := 'X']
# undp_processed <- undp_processed[, flagMethod := 'c']
# undp_processed <- undp_processed[complete.cases(undp_processed),]


# print('AQUASTAT external: ILO 1')
# # ILO ---------------------------------------------------------------------------------------------------------------------------------
# # Read in the data
# datasource <- src$source[str_detect(src$source, 'ILO')][1]
# ilo_link <- src$data_link[str_detect(src$data_link, 'ilostat')][1]
# tf_ilo <- paste(tempfile(), '.tar.gz', sep = '')
# download.file(ilo_link, tf_ilo, mode = 'wb', method = 'wget')
# ilo_df <- data.table(read.csv(gzfile(tf_ilo)))
#
# # Filter
# ilo_df <- ilo_df[sex == 'SEX_T' & classif1 == 'AGE_AGGREGATE_TOTAL']
#
# # Select
# ilo_df <- ilo_df[, .(ref_area, indicator, time, obs_value)]
#
# # merge
# ilo_map <- src[data_link %in% ilo_link, .(element_code,source_item_code, source_element_code)]
# ilo_source_merged <- merge(ilo_df, ilo_map, by.x = 'indicator', by.y = 'source_item_code')
# ilo_proc1 <- ilo_source_merged[!is.na(obs_value)]
# ilo_proc2 <- ilo_proc1[, .(ref_area, element_code, time, obs_value)]
# names(ilo_proc2) <- c('ISO3', 'aquastatElement', 'timePointYears', 'Value')
#
# ilo_proc2 <- ilo_proc2[, ISO3 := as.character(ISO3)]
# ilo_processed <- left_join(ilo_proc2, area_map, by = c('ISO3')) %>%
#   tbl_df() %>%
#   arrange(M49, aquastatElement) %>%
#   #filter out  missing m49 codes
#   filter(!is.na(M49)) %>%
#   data.table()
#
# setnames(ilo_processed, 'M49', 'geographicAreaM49')
# ilo_processed <- ilo_processed[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
# ilo_processed <- ilo_processed[, flagObservationStatus := 'X']
# ilo_processed <- ilo_processed[, flagMethod := 'c']
# ilo_processed <- ilo_processed[complete.cases(ilo_processed),]
#
#
#'
#' # print('AQUASTAT external: FAOSTAT 1')
#' # FAOSTAT ----------------------------------------------------------------------------------------------------------------------
#' # get all data links
#' datasource <- src$source[str_detect(src$source, 'FAOSTAT')]
#' fs_link <- as.character(src[source %in% datasource , c(data_link)])
#'
#' # source codes
#' item_code <- src[source %in% datasource, c(source_item_code)]
#' element_code <- src[source %in% datasource, c(source_element_code)]
#'
#' # filter links by FAOSTAT topic
#' land_link <- fs_link[str_detect(fs_link, 'Land')][1]
#' pop_link <- fs_link[str_detect(fs_link, 'Population')][1]
#' food_link <- fs_link[str_detect(fs_link, 'Food')][1]
#' macro_link <- fs_link[str_detect(fs_link, 'Macro')][1]
#' deflator_link <- fs_link[str_detect(fs_link, 'Deflator')][1]
#'
#'
#' print('AQUASTAT external: FS LAND 1')
#' # land----
#' tf_land <- tempfile()
#' download.file(land_link, tf_land)
#' td_land <- tempdir()
#' file.name <- unzip(tf_land, exdir = td_land)
#' land <- fread(file.name)
#' land2 <- land[`Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
#' names(land2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
#' land2[, geographicAreaM49 := as.character(geographicAreaM49)]
#' land2[, measuredItem := as.character(measuredItem)]
#' land2[, measuredElement := as.character(measuredElement)]
#' land2[, timePointYears := as.character(timePointYears)]
#' land2[, Value := suppressWarnings(as.numeric(Value))]
#'
#'
#' # to map fs to aquastat
#' land_map <- src[data_link %in% land_link,.(element_code, source_item_code, source_element_code)]
#' land_map[, element_code := as.character(element_code)]
#' land_map[, source_item_code := as.character(source_item_code)]
#' land_map[, source_element_code := as.character(source_element_code)]
#'
#' setnames(land_map, 'source_item_code', 'measuredItem')
#' setnames(land_map, 'source_element_code', 'measuredElement')
#'
#' land_source_merged <- left_join(land2, land_map, by = c('measuredItem','measuredElement')) %>%
#'   tbl_df() %>%
#'   arrange(measuredItem, element_code) %>%
#'   data.table()
#' land_source_merged <- land_source_merged[complete.cases(land_source_merged),]
#'
#' print('AQUASTAT external: FS pop 1')
#' # population----
#' tf_pop <- tempfile()
#' download.file(pop_link, tf_pop)
#' td_pop <- tempdir()
#' file.name <- unzip(tf_pop, exdir = td_pop)
#' pop <- fread(file.name)
#' pop2 <- pop[`Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
#' names(pop2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
#' pop2[, geographicAreaM49 := as.character(geographicAreaM49)]
#' pop2[, measuredItem := as.character(measuredItem)]
#' pop2[, measuredElement := as.character(measuredElement)]
#' pop2[, timePointYears := as.character(timePointYears)]
#' pop2[, Value := suppressWarnings(as.numeric(Value))]
#'
#'
#' # to map fs to aquastat
#' pop_map <- src[data_link %in% pop_link,.(element_code, source_item_code, source_element_code)]
#' pop_map[, element_code := as.character(element_code)]
#' pop_map[, source_item_code := as.character(source_item_code)]
#' pop_map[, source_element_code := as.character(source_element_code)]
#'
#' setnames(pop_map, 'source_item_code', 'measuredItem')
#' setnames(pop_map, 'source_element_code', 'measuredElement')
#'
#' pop_source_merged <- left_join(pop2, pop_map, by = c('measuredItem','measuredElement')) %>%
#'   tbl_df() %>%
#'   arrange(measuredItem, element_code) %>%
#'   data.table()
#' pop_source_merged <- pop_source_merged[complete.cases(pop_source_merged),]
#'
#'
#' print('AQUASTAT external: FS MACRO 1')
#' # macro----
#' tf_macro <- tempfile()
#' download.file(macro_link, tf_macro)
#' td_macro <- tempdir()
#' file.name <- unzip(tf_macro, exdir = td_macro)
#' macro <- fread(file.name)
#' macro2 <- macro[`Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
#' names(macro2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
#' macro2[, geographicAreaM49 := as.character(geographicAreaM49)]
#' macro2[, measuredItem := as.character(measuredItem)]
#' macro2[, measuredElement := as.character(measuredElement)]
#' macro2[, timePointYears := as.character(timePointYears)]
#' macro2[, Value := suppressWarnings(as.numeric(Value))]
#'
#' # to map fs to aquastat
#' macro_map <- src[data_link %in% macro_link,.(element_code, source_item_code, source_element_code)]
#' macro_map[, element_code := as.character(element_code)]
#' macro_map[, source_item_code := as.character(source_item_code)]
#' macro_map[, source_element_code := as.character(source_element_code)]
#'
#' setnames(macro_map, 'source_item_code', 'measuredItem')
#' setnames(macro_map, 'source_element_code', 'measuredElement')
#'
#' macro_source_merged <- dplyr::left_join(macro2, macro_map, by = c('measuredItem','measuredElement')) %>%
#'   tbl_df() %>%
#'   dplyr::arrange(measuredItem, element_code) %>%
#'   data.table()
#' macro_source_merged <- macro_source_merged[complete.cases(macro_source_merged),]
#'
#'
#' print('AQUASTAT external: FS DEFLATOR 1')
#' # deflator ----
#' tf_deflator <- tempfile()
#' download.file(deflator_link, tf_deflator)
#' td_deflator <- tempdir()
#' file.name <- unzip(tf_deflator, exdir = td_deflator)
#' deflator <- fread(file.name)
#' deflator2 <- deflator[`Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
#' names(deflator2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
#' deflator2[, geographicAreaM49 := as.character(geographicAreaM49)]
#' deflator2[, measuredItem := as.character(measuredItem)]
#' deflator2[, measuredElement := as.character(measuredElement)]
#' deflator2[, timePointYears := as.character(timePointYears)]
#' deflator2[, Value := suppressWarnings(as.numeric(Value))]
#'
#' # to map fs to aquastat
#' deflator_map <- src[data_link %in% deflator_link,.(element_code, source_item_code, source_element_code)]
#' deflator_map[, element_code := as.character(element_code)]
#' deflator_map[, source_item_code := as.character(source_item_code)]
#' deflator_map[, source_element_code := as.character(source_element_code)]
#'
#' setnames(deflator_map, 'source_item_code', 'measuredItem')
#' setnames(deflator_map, 'source_element_code', 'measuredElement')
#'
#' deflator_source_merged <- left_join(deflator2, deflator_map, by = c('measuredItem','measuredElement')) %>%
#'   tbl_df() %>%
#'   arrange(measuredItem, element_code) %>%
#'   data.table()
#' deflator_source_merged <- deflator_source_merged[complete.cases(deflator_source_merged),]
#'
#'
#'
#' print('AQUASTAT external: FS FOOD 1')
#' # food security----
#' tf_food <- tempfile()
#' download.file(food_link, tf_food)
#' td_food <- tempdir()
#' file.name <- unzip(tf_food, exdir = td_food)
#' food <- fread(file.name)
#' food2 <- food[`Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
#'
#' names(food2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
#' food2[, geographicAreaM49 := as.character(geographicAreaM49)]
#' food2[, measuredItem := as.character(measuredItem)]
#' food2[, measuredElement := as.character(measuredElement)]
#' food2[, timePointYears := as.character(timePointYears)]
#' food2[, Value := suppressWarnings(as.numeric(Value))]
#'
#' # to map fs to aquastat
#' food_map <- src[data_link %in% food_link,.(element_code, source_item_code, source_element_code)]
#' food_map[, element_code := as.character(element_code)]
#' food_map[, source_item_code := as.character(source_item_code)]
#' food_map[, source_element_code := as.character(source_element_code)]
#'
#' setnames(food_map, 'source_item_code', 'measuredItem')
#' setnames(food_map, 'source_element_code', 'measuredElement')
#'
#' food_source_merged <- dplyr::left_join(food2, food_map, by = c('measuredItem','measuredElement')) %>%
#'   tbl_df() %>%
#'   dplyr::arrange(measuredItem, element_code) %>%
#'   data.table()
#' food_source_merged <- food_source_merged[complete.cases(food_source_merged),]
#'
#' print('AQUASTAT external: FS BINDING 1')
#' # binding FAOSTAT sources ----
#' fao_processed <- rbindlist(list(land_source_merged, pop_source_merged, macro_source_merged, deflator_source_merged, food_source_merged))
#' setnames(fao_processed, 'element_code', 'aquastatElement')
#' fao_processed <- fao_processed[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
#' fao_processed <- fao_processed[, timePointYears:= as.character(ifelse(nchar(timePointYears) > 4,
#'                                                                       substring(timePointYears, 6, 9),
#'                                                                       timePointYears))]
#' fao_processed <- fao_processed[complete.cases(fao_processed)]
#'
#'
#' # FAOSTAT to M49
#' area_map_fs <- unique(m49_to_iso[, .(fs_area_code, M49)])
#' setnames(area_map_fs, 'fs_area_code', 'geographicAreaM49')
#' fao_processed <- merge(fao_processed, area_map_fs, by = c("geographicAreaM49"), all.x = TRUE)
#' fao_processed <- fao_processed[!is.na(M49)]
#' fao_processed <- fao_processed[!is.na(aquastatElement)]
#' fao_processed <- fao_processed[!is.na(Value)]
#' fao_processed <- fao_processed[, .(M49, aquastatElement, timePointYears, Value)]
#' setnames(fao_processed, 'M49', 'geographicAreaM49')
#' fao_processed <- fao_processed[, flagObservationStatus := 'X']
#' fao_processed <- fao_processed[, flagMethod := 'c']
#' fao_processed <- fao_processed[!is.na(Value)]
#'
#'
#' # print(head(fao_processed, 10))
#' #'
#' #'
#' print('AQUASTAT external: UNSD 1')
#' # UNSD -----------------------------------------------------------------------------------------------------------------------
datasource <- src$source[str_detect(src$source, 'UNSD')]
unsd_link <- as.character(src[source %in% datasource , c(data_link)])[1]
tf_unsd <- tempfile()
download.file(unsd_link, tf_unsd, mode = 'wb', method = 'internal')
td_unsd <- tempdir()
file.name <- unzip(tf_unsd, exdir = td_unsd)
unsdb <- fread(file.name)
print(head(unsdb, 10))
#' unsdb <- unsdb[!(Item %in% c("Total Value Added"))]
#' names(unsdb) <- c('geographicAreaM49', 'timePointYears', 'Item', 'Value')
#' unsdb$source_element_code <- unlist(str_extract(unsdb$Item, '\\([^()]+\\)'))
#'
#' # spread data for calculations
#' unsdb_dataforcalc <- data.table::dcast(unsdb, geographicAreaM49 + timePointYears ~ source_element_code, value.var = "Value")
#'
#' # Get value add by industry
#' unsdb_proc <- unsdb_dataforcalc[, Value_4546 := `(ISIC C-E)` +  `(ISIC F)`]
#' unsdb_proc <- unsdb_dataforcalc[, Value_4547 := `(ISIC G-H)` + `(ISIC I)` + `(ISIC J-P)`]
#' unsdb_proc <- unsdb_dataforcalc[, Value_4548 := `(ISIC A-B)`]
#'
#'
#' # Pivot longer
#' measure_vars <- names(unsdb_proc)[str_detect(names(unsdb_proc), 'Value_')]
#' id_vars <- names(unsdb_proc)[str_detect(names(unsdb_proc), 'geo|Year')]
#' unsdb_pivot_longer <- melt(unsdb_proc, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
#'
#'
#' # Get M49 codes
#' unsdb_processed <- unsdb_pivot_longer[, aquastatElement := substring(aquastatElement, 7)]
#'
#' area_map_unsd <- unique(m49_to_iso[, .(area_name, M49)])
#' setnames(area_map_unsd, 'area_name', 'geographicAreaM49')
#' unsdb_processed <- merge(unsdb_processed, area_map_unsd, by = c("geographicAreaM49"), all.x = TRUE)
#'
#' unsdb_processed <- unsdb_processed[, .(M49, aquastatElement, timePointYears, Value)]
#' setnames(unsdb_processed, 'M49', 'geographicAreaM49')
#' unsdb_processed <- unsdb_processed[, flagObservationStatus := 'E']
#' unsdb_processed <- unsdb_processed[, flagMethod := 's']
#' unsdb_processed <- unsdb_processed[complete.cases(unsdb_processed),]
#'
#' #'
#' # BINDING ALL SOURCES -----------------------------------------------------------------------------------------------------------
#' all_sources <- rbindlist(list(jmp_processed, undp_processed, ilo_processed, fao_processed, unsdb_processed))
#' all_sources <- all_sources[, timePointYears := as.character(timePointYears)]
#' all_sources_proc <- all_sources[timePointYears >= start_year & timePointYears <= as.integer(end_year) ]
#' external_data <- all_sources_proc[complete.cases(all_sources_proc),]
#'
#'
#' print('AQUASTAT external: EMAIL 1')
#' # EMAIL ATTACHMENTS ---------------------------------------------------------------------------------------------------------------
#' all_sources_mail <- nameData('aquastat', 'aquastat_dan', external_data)
#' filepattern <-'AquastatExternal_output'
#' dtemp_csv <- tempfile(pattern = filepattern,  fileext = c(".csv.gz"))
#' write.csv(all_sources_mail, gzfile(dtemp_csv), row.names = FALSE)
#'
#' # SEND EMAIL -------------------------------------------------------------------------------------------------------------------
#' # SEND OUTPUT TO USER
#' #' Send e-mail
#' #' @param from E-mail address of the sender (optional).
#' #' @param to E-mail address of the recipient.
#' #' @param subject String indicating the subject of the e-mail.
#' #' @param body String scalar or vector with the object of the
#' #'   e-mail. If it is a vector, the elements can be file names
#' #'   and these files will be sent as attachments to the e-mail.
#' #' @param remove Logical value indicating wheter to remove the
#' #'   files indicated as attachments. Defaults to \code{FALSE}.
#' #'
#' #' @examples
#' #' \dontrun{
#' #' # E-mail with a simple string in body
#' #' send_mail(from = 'someone@fao.org', to = 'someoneelse@fao.org'
#' #'   subject = 'Results', body = 'Some results')
#' #'
#' #' # E-mail with attachments
#' #' send_mail(from = 'someone@fao.org', to = 'someoneelse@fao.org'
#' #'   subject = 'Results', body = c('See file', '/location/of/file.xls'))
#' #' }
#'
#' send_mail <- function(from = NA, to = NA, subject = NA,
#'                       body = NA, remove = FALSE) {
#'
#'   if (missing(from)) from <- 'no-reply@fao.org'
#'
#'   if (missing(to)) {
#'     if (exists('swsContext.userEmail')) {
#'       to <- swsContext.userEmail
#'     }
#'   }
#'
#'   if (is.null(to)) {
#'     stop('No valid email in `to` parameter.')
#'   }
#'
#'   if (missing(subject)) stop('Missing `subject`.')
#'
#'   if (missing(body)) stop('Missing `body`.')
#'
#'   if (length(body) > 1) {
#'     body <-
#'       sapply(
#'         body,
#'         function(x) {
#'           if (file.exists(x)) {
#'             # https://en.wikipedia.org/wiki/Media_type
#'             file_type <-
#'               switch(
#'                 tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
#'                 txt  = 'text/plain',
#'                 csv  = 'text/csv',
#'                 png  = 'image/png',
#'                 jpeg = 'image/jpeg',
#'                 jpg  = 'image/jpeg',
#'                 gif  = 'image/gif',
#'                 xls  = 'application/vnd.ms-excel',
#'                 xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
#'                 doc  = 'application/msword',
#'                 docx = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
#'                 pdf  = 'application/pdf',
#'                 zip  = 'application/zip',
#'                 # https://stackoverflow.com/questions/24725593/mime-type-for-serialized-r-objects
#'                 rds  = 'application/octet-stream'
#'               )
#'
#'             if (is.null(file_type)) {
#'               stop(paste(tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
#'                          'is not a supported file type.'))
#'             } else {
#'               return(sendmailR:::.file_attachment(x, basename(x), type = file_type))
#'             }
#'
#'             if (remove) {
#'               unlink(x)
#'             }
#'           } else {
#'             return(x)
#'           }
#'         }
#'       )
#'   } else if (!is.character(body)) {
#'     stop('`body` should be either a string or a list.')
#'   }
#'
#'   sendmailR::sendmail(from, to, subject, as.list(body))
#' }
#'
#' from <- 'sws@fao.org'
#' to <- swsContext.userEmail
#' result <-  paste0('The AquastatExternal module output is ready!')
#' body <- c(result, dtemp_csv)
#' send_mail(from = from, to = to, subject = result , body = body)
#'
#'
#' # SAVE --------------------------------------------------------------------------------------------------------------------------
#' print('AQUASTAT external: SAVE')
#' # SAVE DATA -------
#' saveRDS(external_data, file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/output', 'aquastat_external.rds'))
#' stats <- SaveData("Aquastat", "aquastat_external", external_data, waitTimeout = 10000)
#' paste0("AquastatExternal module completed successfully!!!")
#' print(paste0('AQUASTAT external ends at :', Sys.time()))
#'
