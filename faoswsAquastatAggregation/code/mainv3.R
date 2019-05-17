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
data[, (colnames(data)) := lapply(.SD, as.character), .SDcols = colnames(data)]
data[, Value := as.numeric(Value)]


# read calculation rule data table for AQUASTAT aggregates
aqua_sdg_agg <- fread(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/data/aqua_sdg_agg.csv'), stringsAsFactors = FALSE, na.strings = c(""))
aqua_sdg_agg[, (colnames(aqua_sdg_agg)) := lapply(.SD, as.character), .SDcols = colnames(aqua_sdg_agg)]

# Get SDG codes and M49 correspondence
cg <- ReadDatatable("a2017regionalgroupings_sdg_feb2017")
cg[, (colnames(cg)) := lapply(.SD, as.character), .SDcols = colnames(cg)]



# Relevant elements
all_elements <- c(str_trim(unique(unlist(str_split(aqua_sdg_agg$region_el_code, ',')))), unique(aqua_sdg_agg$relevant_el_code))


# Keep dataset with all relevant elements
d <- copy(data)
d <- d[aquastatElement %in% all_elements]


# add sgdregional and subregional codes
d <- merge(d, cg, by.x = 'geographicAreaM49', by.y = 'm49_code', all.x = TRUE)
d
# Get wide format dataset
dwider <- data.table::dcast(d, geographicAreaM49 + timePointYears + sdgregion_code + m49_level1_code + m49_level2_code ~
                  aquastatElement, value.var = 'Value' )
names(dwider) <- make.names(colnames(dwider))



# get a list of  data tables with the information necessary for the calculations
region_list <- split(aqua_sdg_agg, aqua_sdg_agg$group_order_name)


lapply(region_list, function(l){

  dw <- copy(dwider)
  # dw[complete.cases(dw),]

  # copy the initial dataset
  l <- region_list$`Oceania`

  # get the countries
  countries <- unique(l$country_el_code)

  # get the region name
  region_name <- unique(l$group_order_name)

  # variables
  var_in_country <- paste0('X', unique(l$relevant_el_code))

  # column to filter region
  colum_to_filter_reg <- unique(l$column_to_select)

  # get the sdgcode
  sdgcode <-  unique(l$sdg_region)

  # get rid of irrelevant regional columns
  columns_to_remove <- setdiff(colnames(dwider)[str_detect(colnames(dwider), 'code')], colum_to_filter_reg)
  dw[, (columns_to_remove) := NULL]
  names(dw)[3] <- 'regcode'


if (transboundary == '1') {

        if (region_name == "LAC"){
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4160 + X4162 - X4174)]
        }
        if (region_name == "Southern Asia"){
          # get values that need to be added
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4160)]
        }
        if (region_name == "SouthEastern Asia"){
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4160)]
        }
        if (region_name == "Europe"){
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4168 + X4160)]
        }
        if (region_name == "North America and Europe"){
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4168 + X4160)]
        }
        if (region_name == "Central Asia and Southern Asia"){
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4160)]
        }
        if (region_name == "Eastern Asia"){
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4160)]
        }
        if (region_name == "Eastern Asia and Southern Asia"){
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4160)]
        }
        if (region_name == "Western Asia"){
          dtab1 <- dw[regcode %in% sdgcode & geographicAreaM49 == '51', .(timePointYears, addition1 = X4168)]
          dtab2 <- dw[regcode %in% sdgcode & geographicAreaM49 == '31', .(timePointYears, addition2 = X4168)]
          dtab3 <- dw[regcode %in% sdgcode & geographicAreaM49 == '268', .(timePointYears, addition3 = X4160 + X4162)]
          setkey(dtab1, 'timePointYears'); setkey(dtab2, 'timePointYears'); setkey(dtab3, 'timePointYears')
          dt1 <- dtab1[dtab2,][dtab3,][, .(timePointYears, addition = addition1 + addition2 + addition3)][!is.na(addition)]
        }
        if (region_name == "Western Asia and Northern Africa"){
          dtab1 <- dw[regcode %in% sdgcode & geographicAreaM49 == '51', .(timePointYears, addition1 = X4168)]
          dtab2 <- dw[regcode %in% sdgcode & geographicAreaM49 == '31', .(timePointYears, addition2 = X4168)]
          dtab3 <- dw[regcode %in% sdgcode & geographicAreaM49 == '268', .(timePointYears, addition3 = X4160 + X4162)]
          dtab4 <- dw[regcode %in% sdgcode & geographicAreaM49 == '729', .(timePointYears, addition4 = X4160 + X4162)]
          setkey(dtab1, 'timePointYears'); setkey(dtab2, 'timePointYears'); setkey(dtab3, 'timePointYears'); setkey(dtab4, 'timePointYears')
          dt1 <- dtab1[dtab2,][dtab3,][dtab4,][] [.(timePointYears, addition = addition1 + addition2 + addition3 + addition4)][!is.na(addition)]
        }
        if (region_name == "Northern Africa"){
          dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = X4160 + X4162)][!is.na(addition)]
        }
        if (region_name == "Sub-Saharan Africa"){
          dt1 <- dw[geographicAreaM49 %in% countries, .(timePointYears, addition = X4174)][!is.na(addition)]
        }
        if (region_name == "North America"){
          dt1 <- dw[geographicAreaM49 %in% countries, .(timePointYears, addition1 = X4174)][!is.na(addition)]
        }

        # Get the second dataset at the regional level and aggregate elements

  if (transboundary == '1') {
         dt2 <- dw[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                           regionalX4157 = sum(X4157, na.rm = TRUE),
                                           regionalX4549 = sum(X4549, na.rm = TRUE)),
                                           by = .(regcode, timePointYears)]

         dt_merge <- merge(dt2, dt1, by = 'timePointYears', all.x = TRUE)
         dt_merge <- dt_merge[complete.cases(dt_merge),]

         dt_final <- dt_merge[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4157 + addition - regionalX4549))]

  } else {

          dt2 <- dw[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                             regionalX4157 = sum(X4188, na.rm = TRUE),
                                             regionalX4549 = sum(X4549, na.rm = TRUE)),
                                              by = .(regcode, timePointYears)]
          dt_final <- dt_merge[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4188 - regionalX4549))]
  }


