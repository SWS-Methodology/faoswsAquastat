##' AquastatAggregation module
##' Author: Francy Lisboa
##' Date: 05/04/2019
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
all_elements <- all_elements[complete.cases(all_elements)]

# Keep dataset with all relevant elements
d <- copy(data)
d <- d[aquastatElement %in% all_elements]



# add sgdregional and subregional codes
d <- merge(d, cg, by.x = 'geographicAreaM49', by.y = 'm49_code', all.x = TRUE)
d
# Get wide format dataset
dwider <- data.table::dcast(d, geographicAreaM49 + timePointYears +
                              sdgregion_code + m49_level1_code +
                              m49_level2_code + ldcs_code + lldcssids_code ~
                  aquastatElement, value.var = 'Value' )
names(dwider) <- make.names(colnames(dwider))


# AGGREGATIONS ------------------------------------------------------------------------------------------------------------------------
# user parameters
region <- as.character(str_trim(unlist(swsContext.computationParams)))

if (region != 'All') {
    dw <- copy(dwider)
    countries <- unique(aqua_sdg_agg[group_order_name %in% region, country_el_code])
    sdgcode <- unique(aqua_sdg_agg[group_order_name %in% region, sdg_region])
    colum_to_filter_reg <- unique(aqua_sdg_agg[group_order_name %in% region, column_to_select])
    columns_to_remove <- setdiff(colnames(dwider)[str_detect(colnames(dwider), 'code')], colum_to_filter_reg)
    column_names <- setdiff(colnames(dw), columns_to_remove)
    dww <- dw[, mget(column_names)]
    names(dww)[3] <- 'regcode'
    transboundary <- unique(aqua_sdg_agg[group_order_name %in% region, transboundary])
    adding <- unique(aqua_sdg_agg[group_order_name %in% region, addition])

    if (transboundary == '1'){

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
                dt_agg_4550 <- dt_merge[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4157 + addition - regionalX4549))]
                } else {
                dt_agg_4550 <- dt_merge[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4157 - addition - regionalX4549))]
                }

    } else {

                dt2 <- dww[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                         regionalX4188 = sum(X4188, na.rm = TRUE),
                                         regionalX4549 = sum(X4549, na.rm = TRUE)),
                                      by = .(regcode, timePointYears)]
                dt_agg_4550 <- dt2[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4188 - regionalX4549))]

    }


}


# If user chooses All regions
if (region == 'All') {
              # get a list of  data tables with the information necessary for the calculations
              region_list <- split(aqua_sdg_agg, aqua_sdg_agg$group_order_name)

              # getting aggregations
              sdg_aggregations_list  <- lapply(region_list, function(l){
                                dw <- copy(dwider)
                                countries <- unique(l$country_el_code)
                                sdgcode <- unique(l$sdg_region)
                                colum_to_filter_reg <- unique(l$column_to_select)
                                columns_to_remove <- setdiff(colnames(dwider)[str_detect(colnames(dwider), 'code')], colum_to_filter_reg)
                                column_names <- setdiff(colnames(dw), columns_to_remove)
                                dw <- dw[, mget(column_names)]
                                names(dw)[3] <- 'regcode'
                                transboundary <- as.character(unique(l$transboundary))
                                adding <- unique(l$addition)


                                if (length(countries) < 2) {
                                    dt1 <- dw[regcode %in% sdgcode & geographicAreaM49 %in% countries, .(timePointYears, addition = eval(parse(text = adding)))]
                                } else {
                                  if (sdgcode == "145"){
                                    dtab1 <- dw[regcode %in% sdgcode & geographicAreaM49 == countries[1], .(timePointYears, addition1 = eval(parse(text = adding[1])))]
                                    dtab2 <- dw[regcode %in% sdgcode & geographicAreaM49 == countries[2], .(timePointYears, addition2 = eval(parse(text = adding[1])))]
                                    dtab3 <- dw[regcode %in% sdgcode & geographicAreaM49 == countries[3], .(timePointYears, addition3 = eval(parse(text = adding[2])))]
                                    setkey(dtab1, 'timePointYears'); setkey(dtab2, 'timePointYears'); setkey(dtab3, 'timePointYears')
                                    dt1 <- dtab1[dtab2,][dtab3,][, .(timePointYears, addition = addition1 + addition2 + addition3)][!is.na(addition)]
                                  }
                                  if (sdgcode == "747"){
                                    dtab1 <- dw[regcode %in% sdgcode & geographicAreaM49 == '51', .(timePointYears, addition1 = eval(parse(text = adding[1])))]
                                    dtab2 <- dw[regcode %in% sdgcode & geographicAreaM49 == '31', .(timePointYears, addition2 = eval(parse(text = adding[1])))]
                                    dtab3 <- dw[regcode %in% sdgcode & geographicAreaM49 == '268', .(timePointYears, addition3 = eval(parse(text = adding[2])))]
                                    dtab4 <- dw[regcode %in% sdgcode & geographicAreaM49 == '729', .(timePointYears, addition4 = eval(parse(text = adding[2])))]
                                    setkey(dtab1, 'timePointYears'); setkey(dtab2, 'timePointYears'); setkey(dtab3, 'timePointYears'); setkey(dtab4, 'timePointYears')
                                    dt1 <- dtab1[dtab2,][dtab3,][dtab4,][.(timePointYears, addition = addition1 + addition2 + addition3 + addition4)][!is.na(addition)]
                                  }
                                }


                          # Get the second dataset at the regional level and aggregate elements
                              if (transboundary == '1') {
                                  dt2 <- dw[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                                                   regionalX4157 = sum(X4157, na.rm = TRUE),
                                                                   regionalX4549 = sum(X4549, na.rm = TRUE)),
                                                                   by = .(regcode, timePointYears)]

                                  dt_merge <- merge(dt2, dt1, by = 'timePointYears', all.x = TRUE)
                                  dt_merge <- dt_merge[complete.cases(dt_merge),]

                                  if (sdgcode != "202"){
                                    dt_agg_4550 <- dt_merge[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4157 + addition - regionalX4549))]
                                  } else {
                                    dt_agg_4550 <- dt_merge[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4157 - addition - regionalX4549))]
                                  }

                              } else {

                                   dt2 <- dw[regcode %in% sdgcode][,.(regionalX4263 = sum(X4263, na.rm = TRUE),
                                                                     regionalX4188 = sum(X4188, na.rm = TRUE),
                                                                     regionalX4549 = sum(X4549, na.rm = TRUE)),
                                                                      by = .(regcode, timePointYears)]
                                  dt_agg_4550 <- dt2[, .(regcode, timePointYears, Value = regionalX4263/(regionalX4188 - regionalX4549))]
                              }

                        dt_agg_4550[]
                  }
                  )
              dt_agg_4550 <- rbindlist(sdg_aggregations_list)

}




# POST PROCESSING -------------------------------------------------------------------------------------------------------------------

# setnames(dt_agg, 'regcode', 'geographicAreaM49')
# nameData('aquastat', 'aquastat_dan', dt_agg)
metadata <- unique(aqua_sdg_agg[, .(sdg_region, group_order_name)])
setnames(metadata, 'sdg_region', 'regcode')
dt_proc <- merge(dt_agg, metadata, by = 'regcode', allow.cartesian = TRUE)
setnames(dt_proc, c('regcode', 'group_order_name'), c('geographicAreaM49', 'geographicAreaM49_description'))
dt_proc$Unit <- '%'
dt_final <- dt_proc[, .(geographicAreaM49, geographicAreaM49_description, timePointYears, Value, Unit)]



# SEND EMAIL --------------------------------------------------------------------------------------------
filepattern <- paste0('SWS_AQUA_SDG_AGGREGATIONS')
dsdg_csv <- tempfile(pattern = filepattern,  fileext = c(".csv"))
write.csv(dt_final , file = dsdg_csv, row.names = FALSE)


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




# lowercasenames <- tolower(names(dt_final))
# names(dt_final) <- lowercasenames
#
# # SAVE DATA TABLE ----------------------------------------------------------------------------------------
# # Delete
# table <- 'aquastat_aggregation'
# changeset <- Changeset(table)
# newdat <- ReadDatatable(table,  readOnly = FALSE)
#
# AddDeletions(changeset, newdat)
# Finalise(changeset)
#
# ## Add
# AddInsertions(changeset, dt_final)
# Finalise(changeset)

