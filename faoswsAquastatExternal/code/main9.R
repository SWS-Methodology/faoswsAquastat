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

 })



if(CheckDebug()){
  library(faoswsModules)
  SETTINGS = ReadSettings("~./github/faoswsAQUASTAT/faoswsAquastatExternal/sws.yml")
  Sys.setenv("R_SWS_SHARE_PATH" = SETTINGS[["share"]])
  R_SWS_SHARE_PATH = SETTINGS[["share"]]
  SetClientFiles(SETTINGS[["certdir"]])
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}


if (!CheckDebug()) {

  R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

}

#  Read external data that will not be harvested by this module
list_elements <- c('4114', '4115', '4116', '4449', '4472',
                   '4537', '4541', '4542', '4543', '4546',
                   '4547', '4548', '4111', '4549')
imputKey <- DatasetKey(
  domain = "Aquastat",
  dataset = "aquastat_enr_all_countries",
  dimensions = list(
    Dimension(name = "geographicAreaM49",
              keys = GetCodeList('Aquastat', "aquastat_enr_all_countries", 'geographicAreaM49')[type == 'country', code]),
    Dimension(name = "aquastatElement", keys = list_elements),
    Dimension(name = "timePointYears", keys = as.character(1961:substring(Sys.time(), 1, 4)))
  )
)
data_nonharv <- GetData(imputKey, flags = TRUE)
data_nonharv[, flagAquastat := 'X']
data_nonharv$flagMethod <- 'c'
data_nonharv <- data_nonharv[, .(geographicAreaM49, aquastatElement, timePointYears, Value, flagAquastat, flagMethod)]
setnames(data_nonharv, 'flagAquastat', 'flagObservationStatus')


# Read data tables
sources <- ReadDatatable('aqua_external_sources')
src <- sources[, .(element_code, source, source_item_code, source_element_code, data_link)]
src <- src[, data_link := str_trim(data_link)]
# download iso -> m49 data table
m49_to_iso <- ReadDatatable('m49_fs_iso_mapping')
m49_to_iso <- m49_to_iso[, lapply(.SD, as.character)]
names(m49_to_iso) <- c('group_area_code', 'group_area_name', 'fs_area_code', 'fs_area_name', 'M49', 'ISO2', 'ISO3')

# global variables
start_year <- 1961
end_year <- str_sub(Sys.time(),1, 4)

# print('AQUASTAT external: FAOSTAT 1')
# FAOSTAT ----------------------------------------------------------------------------------------------------------------------
# get all data links
datasource <- src$source[str_detect(src$source, 'FAOSTAT')]
fs_link <- as.character(src[source %in% datasource , c(data_link)])

# source codes
item_code <- src[source %in% datasource, c(source_item_code)]
element_code <- src[source %in% datasource, c(source_element_code)]

# filter links by FAOSTAT topic
land_link <- fs_link[str_detect(fs_link, 'Land')][1]
pop_link <-  fs_link[str_detect(fs_link, 'Population')][1]
food_link <- fs_link[str_detect(fs_link, 'Food')][1]
macro_link <- fs_link[str_detect(fs_link, 'Macro')][1]
deflator_link <- fs_link[str_detect(fs_link, 'Deflator')][1]


print('AQUASTAT external: FS LAND 1')
# land----
tf_land <- tempfile()
download.file(land_link, tf_land, mode = 'wb')
td_land <- tempdir()
file.name <- unzip(tf_land, exdir = td_land)
land <- fread(file.name)
land2 <- land[`Area Code` < 5000 & `Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
names(land2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
land2[, (colnames(land2)) := lapply(.SD, as.character), .SDcols = colnames(land2)]
land2[, Value := as.numeric(Value)]
#
# land2[, geographicAreaM49 := as.character(geographicAreaM49)]
# land2[, measuredItem := as.character(measuredItem)]
# land2[, measuredElement := as.character(measuredElement)]
# land2[, timePointYears := as.character(timePointYears)]
# land2[, Value := suppressWarnings(as.numeric(Value))]
# land2[measuredItem == 6611]

# to map fs to aquastat
land_map <- src[data_link %in% land_link, .(element_code, source_item_code, source_element_code)]
land_map[, (colnames(land_map)) := lapply(.SD, as.character), .SDcols = colnames(land_map)]

# land_map[, element_code := as.character(element_code)]
# land_map[, source_item_code := as.character(source_item_code)]
# land_map[, source_element_code := as.character(source_element_code)]

setnames(land_map, 'source_item_code', 'measuredItem')
setnames(land_map, 'source_element_code', 'measuredElement')

land_source_merged <- left_join(land2, land_map, by = c('measuredItem','measuredElement')) %>%
  tbl_df() %>%
  arrange(measuredItem, element_code) %>%
  data.table()
land_source_merged <- land_source_merged[complete.cases(land_source_merged),]


print('AQUASTAT external: FS pop 1')
# population----
tf_pop <- tempfile()
download.file(pop_link, tf_pop, mode = 'wb')
td_pop <- tempdir()
file.name <- unzip(tf_pop, exdir = td_pop)
pop <- fread(file.name)
pop2 <- pop[`Area Code` < 5000 & `Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
names(pop2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
pop2[, geographicAreaM49 := as.character(geographicAreaM49)]
pop2[, measuredItem := as.character(measuredItem)]
pop2[, measuredElement := as.character(measuredElement)]
pop2[, timePointYears := as.character(timePointYears)]
pop2[, Value := suppressWarnings(as.numeric(Value))]


# to map fs to aquastat
pop_map <- src[data_link %in% pop_link,.(element_code, source_item_code, source_element_code)]
pop_map[, element_code := as.character(element_code)]
pop_map[, source_item_code := as.character(source_item_code)]
pop_map[, source_element_code := as.character(source_element_code)]

setnames(pop_map, 'source_item_code', 'measuredItem')
setnames(pop_map, 'source_element_code', 'measuredElement')

pop_source_merged <- left_join(pop2, pop_map, by = c('measuredItem','measuredElement')) %>%
  tbl_df() %>%
  arrange(measuredItem, element_code) %>%
  data.table()
pop_source_merged <- pop_source_merged[complete.cases(pop_source_merged),]


print('AQUASTAT external: FS MACRO 1')
# macro----
tf_macro <- tempfile()
download.file(macro_link, tf_macro, mode = 'wb')
td_macro <- tempdir()
file.name <- unzip(tf_macro, exdir = td_macro)
macro <- fread(file.name)
macro2 <- macro[`Area Code` < 5000 & `Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
names(macro2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
macro2[, geographicAreaM49 := as.character(geographicAreaM49)]
macro2[, measuredItem := as.character(measuredItem)]
macro2[, measuredElement := as.character(measuredElement)]
macro2[, timePointYears := as.character(timePointYears)]
macro2[, Value := suppressWarnings(as.numeric(Value))]

# to map fs to aquastat
macro_map <- src[data_link %in% macro_link,.(element_code, source_item_code, source_element_code)]
macro_map[, element_code := as.character(element_code)]
macro_map[, source_item_code := as.character(source_item_code)]
macro_map[, source_element_code := as.character(source_element_code)]

setnames(macro_map, 'source_item_code', 'measuredItem')
setnames(macro_map, 'source_element_code', 'measuredElement')

macro_source_merged <- dplyr::left_join(macro2, macro_map, by = c('measuredItem','measuredElement')) %>%
  tbl_df() %>%
  dplyr::arrange(measuredItem, element_code) %>%
  data.table()
macro_source_merged <- macro_source_merged[complete.cases(macro_source_merged),]


print('AQUASTAT external: FS DEFLATOR 1')
# deflator ----
tf_deflator <- tempfile()
download.file(deflator_link, tf_deflator, mode = 'wb')
td_deflator <- tempdir()
file.name <- unzip(tf_deflator, exdir = td_deflator)
deflator <- fread(file.name)
deflator2 <- deflator[`Area Code` < 5000 & `Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]
names(deflator2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
deflator2[, geographicAreaM49 := as.character(geographicAreaM49)]
deflator2[, measuredItem := as.character(measuredItem)]
deflator2[, measuredElement := as.character(measuredElement)]
deflator2[, timePointYears := as.character(timePointYears)]
deflator2[, Value := suppressWarnings(as.numeric(Value))]

# to map fs to aquastat
deflator_map <- src[data_link %in% deflator_link,.(element_code, source_item_code, source_element_code)]
deflator_map[, element_code := as.character(element_code)]
deflator_map[, source_item_code := as.character(source_item_code)]
deflator_map[, source_element_code := as.character(source_element_code)]

setnames(deflator_map, 'source_item_code', 'measuredItem')
setnames(deflator_map, 'source_element_code', 'measuredElement')

deflator_source_merged <- left_join(deflator2, deflator_map, by = c('measuredItem','measuredElement')) %>%
  tbl_df() %>%
  arrange(measuredItem, element_code) %>%
  data.table()
deflator_source_merged <- deflator_source_merged[complete.cases(deflator_source_merged),]

# calculating GDP inflator
print('AQUASTAT Calculating GDP inflator')
df_gdp <- copy(deflator_source_merged)
df_gdp1 <- df_gdp[, GDP_2015 := as.numeric(ifelse(timePointYears == '2015', Value, NA)), by = .(geographicAreaM49)]
df_gdp2 <- df_gdp1[, Value_2015 := GDP_2015[which(!is.na(GDP_2015))], by = .(geographicAreaM49) ]

print('AQUASTAT Calculating Value_def := (Value/Value_2015)*100')
df_gdp3 <- df_gdp2[, Value_def := as.numeric(Value)/as.numeric(Value_2015)*100]

deflator_source_merged <- df_gdp3[, .(geographicAreaM49, measuredItem, measuredElement, timePointYears, Value_def, element_code)]
setnames(deflator_source_merged, 'Value_def', 'Value')


print('AQUASTAT external: FS FOOD 1')
# food security----
tf_food <- tempfile()
download.file(food_link, tf_food, mode = 'wb')
td_food <- tempdir()
file.name <- unzip(tf_food, exdir = td_food)
food <- fread(file.name)
food2 <- food[`Area Code` < 5000 & `Item Code` %in% item_code & `Element Code` %in% element_code, .(`Area Code`, `Item Code`, `Element Code`, Year, Value)]

names(food2) <- c('geographicAreaM49', 'measuredItem', 'measuredElement', 'timePointYears', 'Value')
food2[, geographicAreaM49 := as.character(geographicAreaM49)]
food2[, measuredItem := as.character(measuredItem)]
food2[, measuredElement := as.character(measuredElement)]
food2[, timePointYears := as.character(timePointYears)]
food2[, Value := suppressWarnings(as.numeric(Value))]

# to map fs to aquastat
food_map <- src[data_link %in% food_link,.(element_code, source_item_code, source_element_code)]
food_map[, element_code := as.character(element_code)]
food_map[, source_item_code := as.character(source_item_code)]
food_map[, source_element_code := as.character(source_element_code)]

setnames(food_map, 'source_item_code', 'measuredItem')
setnames(food_map, 'source_element_code', 'measuredElement')

food_source_merged <- dplyr::left_join(food2, food_map, by = c('measuredItem','measuredElement')) %>%
  tbl_df() %>%
  dplyr::arrange(measuredItem, element_code) %>%
  data.table()
food_source_merged <- food_source_merged[complete.cases(food_source_merged),]

print('AQUASTAT external: FS BINDING 1')
# binding FAOSTAT sources ----
fao_bind <- rbindlist(list(land_source_merged, pop_source_merged, macro_source_merged, deflator_source_merged, food_source_merged))
setnames(fao_bind , 'element_code', 'aquastatElement')
fao_proc01 <- fao_bind[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
fao_proc01  <- fao_proc01[, timePointYears:= as.character(ifelse(nchar(timePointYears) > 4,
                                                                      substring(timePointYears, 6, 9),
                                                                      timePointYears))]
# fao_processed <- fao_proc01[complete.cases(fao_proc01)]


# FAOSTAT to M49
area_map_fs <- unique(m49_to_iso[, .(fs_area_code, M49)])
setnames(area_map_fs, 'fs_area_code', 'geographicAreaM49')
fao_processed <- merge(fao_proc01, area_map_fs, by = c("geographicAreaM49"), all.x = TRUE)
fao_processed <- fao_processed[, .(M49, aquastatElement, timePointYears, Value)]
setnames(fao_processed, 'M49', 'geographicAreaM49')
fao_processed <- fao_processed[, flagObservationStatus := 'X']
fao_processed <- fao_processed[, flagMethod := 'c']
fao_processed <- fao_processed[timePointYears >= start_year & timePointYears <= end_year]



# Get data without missing values
external_data_processed <- fao_processed[complete.cases(fao_processed),]
external_data_processed <- rbind(data_nonharv, external_data_processed)
external_data_processed <- external_data_processed[order(geographicAreaM49, aquastatElement, timePointYears)]
external_data_processed[, Value := ifelse(aquastatElement == '4112', Value*1000000, Value)][order(geographicAreaM49, aquastatElement, timePointYears)]
external_data_processed[, Value := ifelse(aquastatElement == '4473', Value*1000, Value)][order(geographicAreaM49, aquastatElement, timePointYears)]
external_data_processed[, flagObservationStatus := 'X']
external_data_processed[, flagMethod := 'c'][order(geographicAreaM49, aquastatElement, timePointYears)]


# Save a backup in the share drive
# saveRDS(external_data_processed, file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/output/External', 'aquastat_external.rds'))
# write.csv(external_data_processed, file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'AquastatValidation/output/External', 'aquastat_external.csv'), row.names = FALSE)
# Save metadata
metadata2save <- copy(external_data_processed)
metadata2save <- metadata2save[, `:=` (Metadata = 'SOURCE', # type
                                       Metadata_Element = 'COMMENT',
                                       Metadata_Value = 'FAOSTAT database: http://www.fao.org/faostat/en/#data',
                                       Metadata_Language = 'en')][aquastatElement %in% list_elements,
                                                                                       `:=` (Metadata_Value = 'Other External sources eg. UNSD, ILO, WRI',
                                                                                             Metadata_Language = 'en')]



metadata2save <- na.omit(metadata2save)
metadata2save <- metadata2save[, c('flagObservationStatus', 'flagMethod', 'Value') := NULL][order(geographicAreaM49, aquastatElement, timePointYears)]

print('Saving data and metadata')
save <- SaveData(domain = "aquastat", dataset = swsContext.datasets[[1]]@dataset,
                 data = external_data_processed, metadata = metadata2save, waitTimeout = 1000)
paste0("AquastatExternal module completed successfully!!!",
       save$inserted, " observations written, ",
       save$ignored, " weren't updated, ",
       save$discarded, " had problems.")


