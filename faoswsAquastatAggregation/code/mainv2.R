##' AquastatAggregation module
##' Author: Francy Lisboa
##' Date: 29/03/2019
##' Purpose: Aggragates SDG indicators using rules pre-defined

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
  SETTINGS = ReadSettings("~./github/faoswsAquastatAggregation/sws.yml")
  Sys.setenv("R_SWS_SHARE_PATH" = SETTINGS[["share"]])
  R_SWS_SHARE_PATH = SETTINGS[["share"]]
  SetClientFiles(SETTINGS[["certdir"]])
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

if (!CheckDebug()) {
    R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
  }

# get the data
data <- readRDS(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/output/Baseline/baseline.rds'))

# read calculation rule data table for AQUASTAT aggregates
aqua_sdg_agg <- fread(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/data/aqua_sdg_agg.csv'), stringsAsFactors = FALSE, na.strings = c(""))
aqua_sdg_agg[, (colnames(aqua_sdg_agg)) := lapply(.SD, as.character), .SDcols = colnames(aqua_sdg_agg)]

# Get SDG codes and M49 correspondence
CountryGroup <- ReadDatatable("a2017regionalgroupings_sdg_feb2017")
cg <- copy(CountryGroup)
cg <- unique(cg[, .(m49_code, sdgregion_code, m49_level1_code, m49_level2_code)])
cg[, (colnames(cg)) := lapply(.SD, as.character), .SDcols = colnames(cg)]

# Get all variables as character vectors
d <- copy(data)
d <- d[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
d[, geographicAreaM49 := as.character(geographicAreaM49)]
d[, aquastatElement := as.character(aquastatElement)]
d[, timePointYears := as.character(timePointYears)]


# Add sgdregional and subregional codes
dmerged <- merge(d, cg, by.x = 'geographicAreaM49', by.y = 'm49_code', all.x = TRUE)

# Get pivot long format so that we have one single variable for all relevant regional hierarchies
dmelt <- data.table::melt(dmerged, measure = patterns('_code'), variable.name = 'region_to_agg', value.name = 'group_order' )

# Get rid of missing values in the group order variable
dmelt <- dmelt[complete.cases(dmelt),]

# Relevant elements
all_elements <- c(str_trim(unique(unlist(str_split(aqua_sdg_agg$el_to_sum_and_divide_reg, ',')))),
                  str_trim(unique(unlist(str_split(aqua_sdg_agg$el_to_sum_in_country, ',')))),
                  str_trim(unique(unlist(str_split(aqua_sdg_agg$el_to_subtract_reg, ',')))))



dmelt_filtered <- dmelt[aquastatElement %in% all_elements]

# grpidx <- unique(aqua_sdg_agg[, .(group_order, group_order_name, region_to_agg)])
# dmelt_merged <- merge(dmelt_filtered, grpidx, by.x = 'region_to_agg', by.y = 'region_to_agg',allow.cartesian = TRUE, all.x = TRUE)

dmelt_filtered[ , regional_sum_4263 :=  sum(X4263, na.rm = TRUE), by = c('group_order', 'timePointYears')]
dmelt_filtered[ , regional_sum_4157 :=  sum(X4157, na.rm = TRUE), by = c('group_order', 'timePointYears')]
dmelt_filtered[ , regional_sum_4549 :=  sum(X4549, na.rm = TRUE), by = c('group_order', 'timePointYears')]
dmelt_filtered[ , regional_sum_4188 :=  sum(X4549, na.rm = TRUE), by = c('group_order', 'timePointYears')]
