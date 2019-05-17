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


missing_dataset1 <- data_to_save[!data_saved, on = c('geographicAreaM49', 'aquastatElement', 'timePointYears')]

missing_dataset2 <- nameData('aquastat', 'aquastat_dan', data_to_save[!data_saved, on = c('geographicAreaM49', 'aquastatElement', 'timePointYears')])

data_saved <- readRDS(paste0(R_SWS_SHARE_PATH, '/AquastatValidation/output/aquastat_update.rds'))

sapply(data_saved, class)
