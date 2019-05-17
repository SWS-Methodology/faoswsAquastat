##' AquastatAggregation module
##' Author: Francy Lisboa
##' Date: 08/04/2019
##' Purpose: Aggragates SDG indicators (6.4.1 and 6.4.2) using pre-defined rules by the technical unit

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

data <- GetData(swsContext.datasets[[1]], flags = TRUE)

#data <- readRDS(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/output/aquastat_update.rds'))
# data[, (colnames(data)) := lapply(.SD, as.character), .SDcols = colnames(data)]
# data[, Value := as.numeric(Value)]

# Read in data tables for aggregations
aqua_sdg_agg <- ReadDatatable("aqua_sdg_agg")
aqua_sdg_agg[, (colnames(aqua_sdg_agg)) := lapply(.SD, as.character), .SDcols = colnames(aqua_sdg_agg)]
aqua_sdg_agg_4550 <- aqua_sdg_agg[indicator == '4550']
aqua_sdg_agg_4551 <- aqua_sdg_agg[indicator == '4551']

# Get SDG codes and M49 correspondence (from Loos and Waste domain)
cg <- ReadDatatable("a2017regionalgroupings_sdg_feb2017")
cg[, (colnames(cg)) := lapply(.SD, as.character), .SDcols = colnames(cg)]

# Relevant elements
all_elements_4550 <- c(str_trim(unique(unlist(str_split(aqua_sdg_agg_4550$region_el_code, ',')))), unique(aqua_sdg_agg[indicator == '4550']$relevant_el_code))
all_elements_4550 <- all_elements_4550[complete.cases(all_elements_4550)]

all_elements_4551 <- c(str_trim(unique(unlist(str_split(aqua_sdg_agg_4551$region_el_code, ',')))), unique(aqua_sdg_agg[indicator == '4551']$relevant_el_code))
all_elements_4551 <- all_elements_4551[complete.cases(all_elements_4551)]

# Keep dataset with all relevant elements
d1 <- copy(data)
d1 <- d1[aquastatElement %in% all_elements_4550]

d2 <- copy(data)
d2 <- d2[aquastatElement %in% all_elements_4551]


# add sgdregional and subregional codes for 4551 (d1) 4550 (d2)
d1 <- merge(d1, cg, by.x = 'geographicAreaM49', by.y = 'm49_code', all.x = TRUE)
d2 <- merge(d2, cg, by.x = 'geographicAreaM49', by.y = 'm49_code', all.x = TRUE)

# Get wide format dataset for 4550
dwider1 <- data.table::dcast(d1, geographicAreaM49 + timePointYears +
                              sdgregion_code + m49_level1_code +
                              m49_level2_code + ldcs_code + lldcssids_code ~
                  aquastatElement, value.var = 'Value' )
names(dwider1) <- make.names(colnames(dwider1))
#dworld1 <- copy(dwider1)

# Get wide format dataset for 4551
dwider2 <- data.table::dcast(d2, geographicAreaM49 + timePointYears +
                               sdgregion_code + m49_level1_code +
                               m49_level2_code + ldcs_code + lldcssids_code ~
                               aquastatElement, value.var = 'Value' )
names(dwider2) <- make.names(colnames(dwider2))
#dworld2 <- copy(dwider2)



# AGGREGATIONS ------------------------------------------------------------------------------------------------------------------------
# user parameters in SWS
# region <- as.character(str_trim(unlist(swsContext.computationParams)))
region <- 'LAC'

# FOR 4550 (d1, dw1, dwider1) ----

GetSDGAggregations <- function(){

if (region != 'All' & region != 'World') {

    dw1 <- copy(dwider1)
    countries <- unique(aqua_sdg_agg_4550[group_order_name %in% region, country_el_code])
    sdgcode <- unique(aqua_sdg_agg_4550[group_order_name %in% region, sdg_region])
    colum_to_filter_reg <- unique(aqua_sdg_agg_4550[group_order_name %in% region, column_to_select])
    columns_to_remove <- setdiff(colnames(dwider1)[str_detect(colnames(dwider1), 'code')], colum_to_filter_reg)
    column_names <- setdiff(colnames(dw1), columns_to_remove)
    dww <- dw1[, mget(column_names)]
    names(dww)[3] <- 'regcode'
    transboundary <- unique(aqua_sdg_agg_4550[group_order_name %in% region, transboundary])
    adding <- unique(aqua_sdg_agg_4550[group_order_name %in% region, addition])

    if (length(transboundary) > 0){

              if (length(countries) < 2) {
                  dt1 <- dww[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = eval(parse(text = adding)))]
              } else {
                  if (sdgcode == "145"){
                  dtab1 <- dww[regcode %in% sdgcode & geographicAreaM49 == countries[1], .(timePointYears, addition1 = eval(parse(text = adding[1])))]
                  dtab2 <- dww[regcode %in% sdgcode & geographicAreaM49 == countries[2], .(timePointYears, addition2 = eval(parse(text = adding[1])))]
                  dtab3 <- dww[regcode %in% sdgcode & geographicAreaM49 == countries[3], .(timePointYears, addition3 = eval(parse(text = adding[2])))]
                  setkey(dtab1, 'timePointYears'); setkey(dtab2, 'timePointYears'); setkey(dtab3, 'timePointYears')
                  dt1 <- dtab1[dtab2,][dtab3,][, .(timePointYears, addition = addition1 + addition2 + addition3)][!is.na(addition)]
                }
                if (sdgcode == "747"){
                  dtab1 <- dww[regcode %in% sdgcode & geographicAreaM49 == '51', .(timePointYears, addition1 = eval(parse(text = adding[1])))]
                  dtab2 <- dww[regcode %in% sdgcode & geographicAreaM49 == '31', .(timePointYears, addition2 = eval(parse(text = adding[1])))]
                  dtab3 <- dww[regcode %in% sdgcode & geographicAreaM49 == '268', .(timePointYears, addition3 = eval(parse(text = adding[2])))]
                  dtab4 <- dww[regcode %in% sdgcode & geographicAreaM49 == '729', .(timePointYears, addition4 = eval(parse(text = adding[2])))]
                  setkey(dtab1, 'timePointYears'); setkey(dtab2, 'timePointYears'); setkey(dtab3, 'timePointYears'); setkey(dtab4, 'timePointYears')
                  dt1 <- dtab1[dtab2,][dtab3,][dtab4,][.(timePointYears, addition = addition1 + addition2 + addition3 + addition4)][!is.na(addition)]
                }
              }

                dt2 <- dww[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                                   regionalX4157 = sum(X4157, na.rm = TRUE),
                                                   regionalX4549 = sum(X4549, na.rm = TRUE)),
                                                   by = .(regcode, timePointYears)]
                dt_merge <- merge(dt2, dt1, by = 'timePointYears', all.x = TRUE)
                dt_merge <- dt_merge[complete.cases(dt_merge),]

                if (sdgcode != "202"){
                dt_agg_4550 <- dt_merge[, .(regcode, timePointYears, Value = 100*(regionalX4263/(regionalX4157 + addition - regionalX4549)))]
                } else {
                dt_agg_4550 <- dt_merge[, .(regcode, timePointYears, Value = 100*(regionalX4263/(regionalX4157 - addition - regionalX4549)))]
                }

    } else {

                dt2 <- dww[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                         regionalX4188 = sum(X4188, na.rm = TRUE),
                                         regionalX4549 = sum(X4549, na.rm = TRUE)),
                                      by = .(regcode, timePointYears)]
                dt_agg_4550 <- dt2[, .(regcode, timePointYears, Value = 100*(regionalX4263/(regionalX4188 - regionalX4549)))]

    }


}

# If user chooses All regions
if (region == 'All') {
              # get a list of  data tables with the information necessary for the calculations
              region_list <- split(aqua_sdg_agg_4550, aqua_sdg_agg_4550$group_order_name)
              # getting aggregations
              sdg_aggregations_list_4550  <- lapply(region_list, function(l){
                                dw1 <- copy(dwider1)

                                countries <- unique(l$country_el_code)
                                sdgcode <- unique(l$sdg_region)
                                colum_to_filter_reg <- unique(l$column_to_select)
                                columns_to_remove <- setdiff(colnames(dwider1)[str_detect(colnames(dwider1), 'code')], colum_to_filter_reg)
                                column_names <- setdiff(colnames(dw1), columns_to_remove)
                                dw1 <- dw1[, mget(column_names)]
                                names(dw1)[3] <- 'regcode'
                                transboundary <- as.character(unique(l$transboundary))
                                adding <- unique(l$addition)


                                if (length(countries) < 2) {
                                    dt1 <- dw1[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = eval(parse(text = adding)))]
                                } else {
                                  if (sdgcode == "145"){
                                    dtab1 <- dw1[regcode %in% sdgcode & geographicAreaM49 == countries[1], .(timePointYears, addition1 = eval(parse(text = adding[1])))]
                                    dtab2 <- dw1[regcode %in% sdgcode & geographicAreaM49 == countries[2], .(timePointYears, addition2 = eval(parse(text = adding[1])))]
                                    dtab3 <- dw1[regcode %in% sdgcode & geographicAreaM49 == countries[3], .(timePointYears, addition3 = eval(parse(text = adding[2])))]
                                    setkey(dtab1, 'timePointYears'); setkey(dtab2, 'timePointYears'); setkey(dtab3, 'timePointYears')
                                    dt1 <- dtab1[dtab2,][dtab3,][, .(timePointYears, addition = addition1 + addition2 + addition3)][!is.na(addition)]
                                  }
                                  if (sdgcode == "747"){
                                    dtab1 <- dw1[regcode %in% sdgcode & geographicAreaM49 == '51', .(timePointYears, addition1 = eval(parse(text = adding[1])))]
                                    dtab2 <- dw1[regcode %in% sdgcode & geographicAreaM49 == '31', .(timePointYears, addition2 = eval(parse(text = adding[1])))]
                                    dtab3 <- dw1[regcode %in% sdgcode & geographicAreaM49 == '268', .(timePointYears, addition3 = eval(parse(text = adding[2])))]
                                    dtab4 <- dw1[regcode %in% sdgcode & geographicAreaM49 == '729', .(timePointYears, addition4 = eval(parse(text = adding[2])))]
                                    setkey(dtab1, 'timePointYears'); setkey(dtab2, 'timePointYears'); setkey(dtab3, 'timePointYears'); setkey(dtab4, 'timePointYears')
                                    dt1 <- dtab1[dtab2,][dtab3,][dtab4,][.(timePointYears, addition = addition1 + addition2 + addition3 + addition4)][!is.na(addition)]
                                  }
                                }


                          # Get the second dataset at the regional level and aggregate elements
                              if (transboundary == '1' & region != 'World') {
                                  dt2 <- dw1[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                                                   regionalX4157 = sum(X4157, na.rm = TRUE),
                                                                   regionalX4549 = sum(X4549, na.rm = TRUE)),
                                                                   by = .(regcode, timePointYears)]

                                  dt_merge <- merge(dt2, dt1, by = 'timePointYears', all.x = TRUE)
                                  dt_merge <- dt_merge[complete.cases(dt_merge),]

                                  if (sdgcode != "202"){
                                    dt_agg_4550 <- dt_merge[, .(regcode, timePointYears, Value = 100*(regionalX4263/(regionalX4157 + addition - regionalX4549)))]
                                  } else {
                                    dt_agg_4550 <- dt_merge[, .(regcode, timePointYears, Value = 100*(regionalX4263/(regionalX4157 - addition - regionalX4549)))]
                                  }

                              } else {

                                   dt2 <- dw1[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                                                     regionalX4188 = sum(X4188, na.rm = TRUE),
                                                                     regionalX4549 = sum(X4549, na.rm = TRUE)),
                                                                      by = .(regcode, timePointYears)]
                                  dt_agg_4550 <- dt2[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4188 - regionalX4549))]
                              }

                        dt_agg_4550[]
                  }
                  )
              dt_agg_4550 <- rbindlist(sdg_aggregations_list_4550)
              # get for all countries
              dt2world1 <- dwider1[,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                    regionalX4188 = sum(X4188, na.rm = TRUE),
                                    regionalX4549 = sum(X4549, na.rm = TRUE)),
                                 by = .(timePointYears)]
              dt2world1$regcode <- '1'
              dt_agg_4550_world <- dt2world1[, .(regcode, timePointYears, Value = 100*(regionalX4263/(regionalX4188 - regionalX4549)))]

              # bind world and other regions
              dt_agg_4550 <- rbind(dt_agg_4550_world, dt_agg_4550)
              dt_agg_4550 <- dt_agg_4550[order(regcode)]
}

# If the user only chooses world
if (region == 'World') {
# get for all countries
dt2world1 <- dworld1[,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                      regionalX4188 = sum(X4188, na.rm = TRUE),
                      regionalX4549 = sum(X4549, na.rm = TRUE)),
                   by = .(timePointYears)]
dt2world1$regcode <- '1'
dt_agg_4550 <- dt2world1[, .(regcode, timePointYears, Value = 100*(regionalX4263/(regionalX4188 - regionalX4549)))]
}

#
# For 4551 (d2, dw2, dwider2) ----
if (region != 'All' & region != 'World') {
        dw2 <- copy(dwider2)
        sdgcode <- unique(aqua_sdg_agg_4551[group_order_name %in% region, sdg_region])
        colum_to_filter_reg <- unique(aqua_sdg_agg_4551[group_order_name %in% region, column_to_select])
        columns_to_remove <- setdiff(colnames(dwider2)[str_detect(colnames(dwider2), 'code')], colum_to_filter_reg)
        column_names <- setdiff(colnames(dw2), columns_to_remove)
        dww <- dw2[, mget(column_names)]
        # Get step3_num <- "((X4548 / X4558) *100)*(X4555/100)" at the national level first
        names(dww)[3] <- 'regcode'
        adding <- str_trim(unique(aqua_sdg_agg_4551$addition))[!is.na(unique(aqua_sdg_agg_4551$addition))]
        dww <- dww[, step3_num := eval(parse(text = adding[1]))][]

        cols_to_aggregate <- c(colnames(dww)[grep( '^X', colnames(dww))], 'step3_num')
        #cols_to_aggregate_step3_num <- c('step3_num')
        #cols_to_aggregate_sum <- setdiff(cols_to_aggregate, cols_to_aggregate_mean)
        dwwlac <- dww[regcode %in% sdgcode & timePointYears == '2018']
        dww_sum <- dwwlac[regcode %in% sdgcode][, lapply(.SD, sum, na.rm = TRUE), by = c('timePointYears', 'regcode'), .SDcols =   cols_to_aggregate][]
        #dww_sum[, X4555 := NULL]
        #dww_mean <- dww[regcode %in% sdgcode][, lapply(.SD, sum, na.rm = TRUE), by = c('timePointYears', 'regcode'), .SDcols =  cols_to_aggregate_mean][]
        #dmerge01 <- merge(dww_sum, dww_mean, by = c('timePointYears', 'regcode'))[]

        # calculating 4551 (SDG 6.4.1 Water use efficiency) intermediates
        # dt2 <- dww_sum[, `=`.(regcode, timePointYears,
        #              X4556 = eval(parse(text = adding[2])),
        #              X4555 = eval(parse(text = adding[3])),
        #              X4552 = eval(parse(text = adding[4])),
        #              X4553 = eval(parse(text = adding[5])),
        #              X4554 = eval(parse(text = adding[6])),
        #              Value = eval(parse(text = adding[7])))][]

               dww_sum[,X4556 := eval(parse(text = adding[2]))][]
               dww_sum[,X4555 := eval(parse(text = adding[3]))][]
               dww_sum[,X4552 := eval(parse(text = adding[4]))][]
               dww_sum[,X4553 := eval(parse(text = adding[7]))][]
               dww_sum[,X4554 := eval(parse(text = adding[5]))][]
               dww_sum[,Value := eval(parse(text = adding[6]))][]

        dt_agg_4551 <- dww_sum[complete.cases(dt2), .(regcode, timePointYears, Value)]
}

if (region == 'All') {
            # get a list of  data tables with the information necessary for the calculations
            region_list_4551 <- split(aqua_sdg_agg_4551, aqua_sdg_agg_4551$group_order_name)

            # getting aggregations
            sdg_aggregations_list_4551  <- lapply(region_list_4551, function(l){
                        dw2 <- copy(dwider2)
                        #countries <- unique(l$country_el_code)
                        sdgcode <- unique(l$sdg_region)
                        colum_to_filter_reg <- unique(l$column_to_select)
                        columns_to_remove <- setdiff(colnames(dwider2)[str_detect(colnames(dwider2), 'code')], colum_to_filter_reg)
                        column_names <- setdiff(colnames(dw2), columns_to_remove)
                        dww2 <- dw2[, mget(column_names)]
                        names(dww2)[3] <- 'regcode'
                        adding <- str_trim(unique(aqua_sdg_agg_4551$addition))[!is.na(unique(aqua_sdg_agg_4551$addition))]

                        cols_to_aggregate <- colnames(dwider2)[grep( '^X', colnames(dwider2))]
                        cols_to_aggregate_mean <- c('X4254', 'X4255', 'X4256', 'X4557')
                        cols_to_aggregate_sum <- setdiff(cols_to_aggregate, cols_to_aggregate_mean)

                        dww_sum <- dww2[regcode %in% sdgcode][, lapply(.SD, sum, na.rm = TRUE), by = c('timePointYears', 'regcode'), .SDcols =  cols_to_aggregate_sum][]
                        dww_mean <- dww2[regcode %in% sdgcode][, lapply(.SD, sum, na.rm = TRUE), by = c('timePointYears', 'regcode'), .SDcols =  cols_to_aggregate_mean][]
                        dmerge02 <- merge(dww_sum, dww_mean, by = c('timePointYears', 'regcode'))[]

                        # calculating 4551 (SDG 6.4.1 Water use efficiency) intermediates
                        dt2 <- dmerge02[, .(regcode, timePointYears,
                                            X4556 = eval(parse(text = adding[1])),
                                            X4555 = eval(parse(text = adding[2])),
                                            X4552 = eval(parse(text = adding[3])),
                                            X4553 = eval(parse(text = adding[4])),
                                            X4554 = eval(parse(text = adding[5])),
                                            Value = eval(parse(text = adding[6])))][]

                        dt_agg_4551 <- dt2[complete.cases(dt2), .(regcode, timePointYears, Value)][]

            }
            )
      dt_agg_4551 <- rbindlist(sdg_aggregations_list_4551)

      # for the world
      adding <- str_trim(unique(aqua_sdg_agg_4551$addition))[!is.na(unique(aqua_sdg_agg_4551$addition))]
      cols_to_aggregate <- colnames(dwider2)[grep( '^X', colnames(dwider2))]
      cols_to_aggregate_mean <- c('X4254', 'X4255', 'X4256', 'X4557')
      cols_to_aggregate_sum <- setdiff(cols_to_aggregate, cols_to_aggregate_mean)


      dww_sum2 <- dwider2[, lapply(.SD, sum, na.rm = TRUE), by = c('timePointYears'), .SDcols =  cols_to_aggregate_sum][]
      dww_mean2 <- dwider2[, lapply(.SD, sum, na.rm = TRUE), by = c('timePointYears'), .SDcols =  cols_to_aggregate_mean][]
      dmerge022 <- merge(dww_sum2, dww_mean2, by = c('timePointYears'))[]

      # calculating 4551 (SDG 6.4.1 Water use efficiency) intermediates
      dt2world2 <- dmerge022[, .(timePointYears,
                          X4556 = eval(parse(text = adding[1])),
                          X4555 = eval(parse(text = adding[2])),
                          X4552 = eval(parse(text = adding[3])),
                          X4553 = eval(parse(text = adding[4])),
                          X4554 = eval(parse(text = adding[5])),
                          Value = eval(parse(text = adding[6])))][]
      dt2world2$regcode <- '1'
      dt_agg_4551_world <- dt2world2[complete.cases(dt2world2), .(regcode, timePointYears, Value)][]


      dt_agg_4551 <- rbind(dt_agg_4551_world, dt_agg_4551)
      dt_agg_4551 <- dt_agg_4551[order(regcode)]


}

if (region == 'World'){

          adding <- str_trim(unique(aqua_sdg_agg_4551$addition))[!is.na(unique(aqua_sdg_agg_4551$addition))]
          cols_to_aggregate <- colnames(dwider2)[grep( '^X', colnames(dwider2))]
          cols_to_aggregate_mean <- c('X4254', 'X4255', 'X4256', 'X4557')
          cols_to_aggregate_sum <- setdiff(cols_to_aggregate, cols_to_aggregate_mean)

          # for the world
          dww_sum2 <- dwider2[, lapply(.SD, sum, na.rm = TRUE), by = c('timePointYears'), .SDcols =  cols_to_aggregate_sum][]
          dww_mean2 <- dwider2[, lapply(.SD, sum, na.rm = TRUE), by = c('timePointYears'), .SDcols =  cols_to_aggregate_mean][]
          dmerge022 <- merge(dww_sum2, dww_mean2, by = c('timePointYears'))[]

          # calculating 4551 (SDG 6.4.1 Water use efficiency) intermediates
          dt2world2 <- dmerge022[, .(timePointYears,
                                     X4556 = eval(parse(text = adding[1])),
                                     X4555 = eval(parse(text = adding[2])),
                                     X4552 = eval(parse(text = adding[3])),
                                     X4553 = eval(parse(text = adding[4])),
                                     X4554 = eval(parse(text = adding[5])),
                                     Value = eval(parse(text = adding[6])))][]
          dt2world2$regcode <- '1'
          dt_agg_4551 <- dt2world2[complete.cases(dt2world2), .(regcode, timePointYears, Value)][]

}
          # setnames(dt_agg, 'regcode', 'geographicAreaM49')
          # nameData('aquastat', 'aquastat_dan', dt_agg)
          metadata <- rbind(data.table(sdg_region = '1', group_order_name = 'World'), unique(aqua_sdg_agg[, .(sdg_region, group_order_name)]))
          setnames(metadata, 'sdg_region', 'regcode')
          dt_proc_4550 <- merge(dt_agg_4550, metadata, by = 'regcode', allow.cartesian = TRUE)
          dt_proc_4551 <- merge(dt_agg_4551, metadata, by = 'regcode', allow.cartesian = TRUE)



          dt_proc_4550$aquastatElement <- '4550'
          dt_proc_4551$aquastatElement <- '4551'
          dt_proc_4550$aquastatElement_description <- 'SDG 6.4.2. Water Stress'
          dt_proc_4551$aquastatElement_description <- 'SDG 6.4.1. Water Use Efficiency'
          dt_proc_4550$unit <- '%'
          dt_proc_4551$unit <- 'USD/m3'



          dt_4550 <- dt_proc_4550[, .(group_order_name, regcode, aquastatElement, aquastatElement_description, timePointYears, Value, unit)]
          dt_4551 <- dt_proc_4551[, .(group_order_name, regcode, aquastatElement, aquastatElement_description, timePointYears, Value, unit)]

          dt_final <- rbind(dt_4550, dt_4551)
          dt_final <- dt_final[complete.cases(dt_final), ]
          dt_final <- dt_final[, Value := format(round(Value, 2), nsmall = 2)][]
          setnames(dt_final, c('regcode', 'group_order_name'), c('geographicAreaM49', 'geographicAreaM49_description'))

          dt_final


}

agg_data <- GetSDGAggregations()
# POST PROCESSING -------------------------------------------------------------------------------------------------------------------
# SEND EMAIL --------------------------------------------------------------------------------------------
filepattern <- paste0('SWS_AQUA_SDG_AGGREGATIONS')
dsdg_csv <- tempfile(pattern = filepattern,  fileext = c(".csv"))
write.csv(agg_data , file = dsdg_csv, row.names = FALSE)


# SEND OUTPUT TO USER
#' Send e-mail
#' @param from E-mail address of the sender (optional).
#' @param to E-mail address of the recipient.
#' @param subject String indicating the subject of the e-mail.
#' @param body String scalar or vector with the object of the
#'   e-mail. If it is a vector, the elements can be file names
#'   and these files will be sent as attachments to the e-mail.
#' @param remove Logical value indicating wheter to remove the
#'   files indicated as attachments. Defaults to \code{FALSE}.
#'
#' @examples
#' \dontrun{
#' # E-mail with a simple string in body
#' send_mail(from = 'someone@fao.org', to = 'someoneelse@fao.org'
#'   subject = 'Results', body = 'Some results')
#'
#' # E-mail with attachments
#' send_mail(from = 'someone@fao.org', to = 'someoneelse@fao.org'
#'   subject = 'Results', body = c('See file', '/location/of/file.xls'))
#' }

send_mail <- function(from = NA, to = NA, subject = NA,
                      body = NA, remove = FALSE) {

  if (missing(from)) from <- 'no-reply@fao.org'

  if (missing(to)) {
    if (exists('swsContext.userEmail')) {
      to <- swsContext.userEmail
    }
  }

  if (is.null(to)) {
    stop('No valid email in `to` parameter.')
  }

  if (missing(subject)) stop('Missing `subject`.')

  if (missing(body)) stop('Missing `body`.')

  if (length(body) > 1) {
    body <-
      sapply(
        body,
        function(x) {
          if (file.exists(x)) {
            # https://en.wikipedia.org/wiki/Media_type
            file_type <-
              switch(
                tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                txt  = 'text/plain',
                csv  = 'text/csv',
                png  = 'image/png',
                jpeg = 'image/jpeg',
                jpg  = 'image/jpeg',
                gif  = 'image/gif',
                xls  = 'application/vnd.ms-excel',
                xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                doc  = 'application/msword',
                docx = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                pdf  = 'application/pdf',
                zip  = 'application/zip',
                # https://stackoverflow.com/questions/24725593/mime-type-for-serialized-r-objects
                rds  = 'application/octet-stream'
              )

            if (is.null(file_type)) {
              stop(paste(tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                         'is not a supported file type.'))
            } else {
              return(sendmailR:::.file_attachment(x, basename(x), type = file_type))
            }

            if (remove) {
              unlink(x)
            }
          } else {
            return(x)
          }
        }
      )
  } else if (!is.character(body)) {
    stop('`body` should be either a string or a list.')
  }

  sendmailR::sendmail(from, to, subject, as.list(body))
}

from <- 'sws@fao.org'
to <- swsContext.userEmail
result <-  'The SDG regional aggregations are done!'
body <- c(result, list(dsdg_csv))

print('sending email to the user')
send_mail(from = from, to = to, subject = result , body = body)
print('Module has finished, check your email')
