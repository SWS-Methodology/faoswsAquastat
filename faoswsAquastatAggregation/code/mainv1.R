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


d <- copy(data)
d <- d[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
d[, geographicAreaM49 := as.character(geographicAreaM49)]
d[, aquastatElement := as.character(aquastatElement)]
d[, timePointYears := as.character(timePointYears)]


# Relevant elements
all_elements <- c(str_trim(unique(unlist(str_split(aqua_sdg_agg$el_to_sum_and_divide_reg, ',')))),
                  str_trim(unique(unlist(str_split(aqua_sdg_agg$el_to_sum_in_country, ',')))),
                  str_trim(unique(unlist(str_split(aqua_sdg_agg$el_to_subtract_reg, ',')))))

# Retain relevant elements
d <- d[aquastatElement %in% all_elements]


# add sgdregional and subregional codes
dmerged <- merge(d, cg, by.x = 'geographicAreaM49', by.y = 'm49_code', all.x = TRUE)


# Get wide format dataset
dwider <- data.table::dcast(dmerged, geographicAreaM49 + timePointYears + sdgregion_code + m49_level1_code + m49_level2_code ~
                  aquastatElement, value.var = 'Value' )
names(dwider) <- make.names(colnames(dwider))



# sdgregion_code <- unique(aqua_sdg_agg$sdgregion_code[!is.na(aqua_sdg_agg$sdgregion_code)])
# m49_level1_code <- unique(aqua_sdg_agg$m49_level1_code[!is.na(aqua_sdg_agg$m49_level1_code)])
# m49_level2_code <- unique(aqua_sdg_agg$m49_level2_code[!is.na(aqua_sdg_agg$m49_level2_code)])
#
df1 <- dmerged[sdgregion_code %in% sdgregion_code][, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
df2 <- dmerged[m49_level1_code %in% m49_level1_code][, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
df3 <- dmerged[m49_level2_code %in% m49_level2_code][, .(geographicAreaM49, aquastatElement, timePointYears, Value)]


# dwider[sdgregion_code %in% sdgregion_code]





# Change names
setnames(aqua_sdg_agg, 'el_to_sum_country', 'geographicAreaM49')
setnames(aqua_sdg_agg, 'el_to_sum_in_country', 'aquastatElement')

grpidx <- unique(aqua_sdg_agg[, .(geographicAreaM49, group_order, group_order_name)])
dwider_country <- merge(dwider, grpidx, by = 'geographicAreaM49', allow.cartesian = TRUE, all.x = TRUE)
dwider_country <- dwider_country[, group_order := paste0('g', group_order)]


all_elements <- c(str_trim(unique(unlist(str_split(aqua_sdg_agg$el_to_sum_and_divide_reg, ',')))),
str_trim(unique(unlist(str_split(aqua_sdg_agg$aquastatElement, ',')))),
str_trim(unique(unlist(str_split(aqua_sdg_agg$el_to_subtract_reg, ',')))))



dwd <- copy(dwider_country)
dwd01 <- dwd[ , addition:=  case_when(
      # LAC
      group_order == 'g1' & geographicAreaM49 == '484'   ~ X4160 + X4162 - X4174,
      # Southern Asia
      group_order == 'g2' & geographicAreaM49 == '356'   ~ X4160,
      # SouthEastern Asia
      group_order == 'g3' & geographicAreaM49 == '458'   ~ X4160,
      # Europe
      group_order == 'g4' & geographicAreaM49 == '643'   ~ X4168 + X4160,
      # North America and Europe
      group_order == 'g5' & geographicAreaM49 == '643'   ~ X4168 + X4160,
      # Central Asia
      group_order == 'g6' & geographicAreaM49 == '762'   ~ X4162,
      # Central Asia and Southern Asia
      group_order == 'g7' & geographicAreaM49 == '4'     ~ X4160,
      # Eastern Asia
      group_order == 'g8' & geographicAreaM49 == '156'   ~ X4160,
      # Eastern Asia and Southern Asia
      group_order == 'g9' & geographicAreaM49 == '156'   ~ X4160,
      # Western Asia
      group_order == 'g10' & geographicAreaM49 == '51'   ~ X4168,
      group_order == 'g10' & geographicAreaM49 == '31'   ~ X4168,
      group_order == 'g10' & geographicAreaM49 == '268'  ~ X4160 + X4162 ,
      # Western Asia and Northern Africa
      group_order == 'g11' & geographicAreaM49 == '51'   ~ X4168,
      group_order == 'g11' & geographicAreaM49 == '31'   ~ X4168,
      group_order == 'g11' & geographicAreaM49 == '268'  ~ X4160 + X4162,
      group_order == 'g11' & geographicAreaM49 == '729'  ~ X4160 + X4162 ,
      # Northern Africa
      group_order == 'g12' & geographicAreaM49 == '729'  ~ X4160 + X4162,
      # Sub-Saharan Africa
      group_order == 'g13' & geographicAreaM49 == '728'  ~ X4174,
      # North America
      group_order == 'g14' & geographicAreaM49 == '484'  ~ X4174)
  ]

# aggregate  Eastern Asia and Southern Asia and Western Asia and Northern Africa groups by timePointYear
dwd02 <- dwd01[, addition := mean(ifelse(group_order %in% c('g10', 'g11'), sum(addition, na.rm = TRUE), addition)), by = c('group_order', 'timePointYears')]




dwd01 <- dwd[ , addition:=  case_when(
  # LAC
  sdgregion_code %in% sdgregion_code & geographicAreaM49 == '484'   ~ X4160 + X4162 - X4174,
  # Southern Asia
  m49_level1_code == m49_level1_code & geographicAreaM49 == '356'   ~ X4160,
  # SouthEastern Asia
  group_order == 'g3' & geographicAreaM49 == '458'   ~ X4160,
  # Europe
  group_order == 'g4' & geographicAreaM49 == '643'   ~ X4168 + X4160,
  # North America and Europe
  group_order == 'g5' & geographicAreaM49 == '643'   ~ X4168 + X4160,
  # Central Asia
  group_order == 'g6' & geographicAreaM49 == '762'   ~ X4162,
  # Central Asia and Southern Asia
  group_order == 'g7' & geographicAreaM49 == '4'     ~ X4160,
  # Eastern Asia
  group_order == 'g8' & geographicAreaM49 == '156'   ~ X4160,
  # Eastern Asia and Southern Asia
  group_order == 'g9' & geographicAreaM49 == '156'   ~ X4160,
  # Western Asia
  group_order == 'g10' & geographicAreaM49 == '51'   ~ X4168,
  group_order == 'g10' & geographicAreaM49 == '31'   ~ X4168,
  group_order == 'g10' & geographicAreaM49 == '268'  ~ X4160 + X4162 ,
  # Western Asia and Northern Africa
  group_order == 'g11' & geographicAreaM49 == '51'   ~ X4168,
  group_order == 'g11' & geographicAreaM49 == '31'   ~ X4168,
  group_order == 'g11' & geographicAreaM49 == '268'  ~ X4160 + X4162,
  group_order == 'g11' & geographicAreaM49 == '729'  ~ X4160 + X4162 ,
  # Northern Africa
  group_order == 'g12' & geographicAreaM49 == '729'  ~ X4160 + X4162,
  # Sub-Saharan Africa
  group_order == 'g13' & geographicAreaM49 == '728'  ~ X4174,
  # North America
  group_order == 'g14' & geographicAreaM49 == '484'  ~ X4174)
  ]







# 'Cast' the group_order based on addition
dwd02_wider <- dcast(dwd02, geographicAreaM49 + timePointYears + sdgregion_code + m49_level1_code + m49_level2_code +
             X4157 + X4160 + X4162 + X4168 + X4174 + X4263 + X4549 ~ group_order, value.var = 'addition' )


dwd03 <- dwd02[ , regional_sum_4263 :=  sum(X4263, na.rm = TRUE), by = c('group_order', 'timePointYears')]
dwd03[ , regional_sum_4157 :=  sum(X4157, na.rm = TRUE), by = c('group_order', 'timePointYears')]
dwd03[ , regional_sum_4549 :=  sum(X4549, na.rm = TRUE), by = c('group_order', 'timePointYears')]

dwd04 <- dwd03[, .(group_order, timePointYears, addition, regional_sum_4263, regional_sum_4157, regional_sum_4549)]


dwd05 <- dwd04[complete.cases(dwd04), ][order(group_order)]


dwd06 <- dwd05[, Value := (regional_sum_4263/regional_sum_4157) + addition - regional_sum_4549]
