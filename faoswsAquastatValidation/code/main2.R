##' Validation module
##' Author: Francy Lisboa
##' Date: 05/12/2018
##' Purpose: this modules utilizes the pre-defined validation rules to output a dataset indicating country and year that
##' the logical test for a given rules was violated, ie. FALSE. The dataset has:
##' geographicAreaM49, timePointYears, expression, test_value, fauty_na, priority
##' expression is the tested logical expression in the country i and year t
##' test_value is the value of the logical test in the country i and year t
##' fauty_na is a vector of character strings pointing the missing elements in the expression in the country i and year t
##' priority is a binary variable indication the order of importance of the rules in the AQUASTAT context

# Loading libraries
suppressMessages({
  library(faosws)
  library(faoswsUtil)
  library(faoswsFlag)
  library(data.table)
  library(dplyr)
  library(tidyr)
  library(stringr)
  # library(flexdashboard)
  # library(shiny)
 })


R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){

  library(faoswsModules)
  SETTINGS = ReadSettings("~/github/faoswsAquastatValidation/sws.yml")

  ## If you're not on the system, your settings will overwrite any others
  R_SWS_SHARE_PATH = SETTINGS[["share"]]

  ## Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])

  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])

  Sys.setenv(R_ZIPCMD = "Rtools/bin/zip")

}


# get the data from the input dataset
data <- GetData(swsContext.datasets[[1]])
data <- setDT(data)
dc <- copy(data)
dc <- dc[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]


# Getting right rules
sanitize_rules <- function(data) {

        # read in the validation rules
        #rules <- ReadDatatable("validation_rules")
        rules <- ReadDatatable('validation_rules_clone')

        # make rules by joining columns
        rules_paste <- data.frame(vrules = str_trim(paste0(rules$lhs, rules$operator, rules$rhs)))
        rules_paste$vrules <- str_replace_all(rules_paste$vrules, '\"==\"', '==')
        rules_paste$priority <- rules$priority
        rules_paste <- rules_paste[!duplicated(rules_paste),]

        # getting elements in the validation rules
        val_elements <- unique(unlist(str_extract_all(rules_paste$vrules, "Value_[0-9]+")))

        # filter rules that cannot be evaluated and save them in the user's working directory
        data_elements <- paste0("Value_", unique(data$aquastatElement))
        intersection <- sort(intersect(val_elements, data_elements))

        # components to remove are not in the intersection between validation elements and the intersection of it and the dataset elements
        components_to_remove <- sort(setdiff(val_elements, intersection))

        # getting non-problematic rules
        if(length(components_to_remove) > 0) {
          rules_to_delete <- stringr::str_detect(rules_paste$vrules, paste(components_to_remove, collapse = "|"))
          rules_to_drop <- rules_paste$vrules[rules_to_delete]
          rules_to_keep <- rules_paste$vrules[!rules_to_delete]
          elements_to_drop <- unique(unlist(str_extract_all(rules_to_drop, "Value_[0-9]+")))
          elements_to_keep <- unique(unlist(str_extract_all(rules_to_keep, "Value_[0-9]+")))

        } else {
          rules_to_keep <- rules_paste$vrules
        }


        res_list <- list(components_to_remove,
                         rules_to_keep,    # rules without falty elements
                         rules_to_drop,    # rules with falty elements
                         elements_to_keep, # elements in the kept rules
                         elements_to_drop, # elements in the dropped rules
                         data_elements,    # all elements fro initial data
                         val_elements,
                         rules_paste)    # all elements in the validation rule

        names(res_list) <- c("components_to_remove",
                              "rules_to_keep",
                              "rules_to_drop",
                              "elements_to_keep",
                              "elements_to_drop",
                              "data_elements",
                              "val_elements",
                              "map_priority")

        return(res_list)
}

# call the function over rules
procrules <- sanitize_rules(data = dc)

# after sanitizing rules we need to:
# 1) keep non-falty elements
# 2) dcast data
reshape_data <- function(data) {
        d <- copy(data)
        d <- d[,.(geographicAreaM49, aquastatElement, timePointYears, Value)]
        d[, aquastatElement := paste0("Value_",aquastatElement)]
        d <- d[aquastatElement %in% procrules$elements_to_keep]
        d <- dcast(d, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")
        return(d)
}

data_wide <- reshape_data(dc)
dw <- copy(data_wide)


# convert sanitized rules to list object
sanitized_rules <- as.vector(procrules$rules_to_keep)

# Evaluation of rules
dfrules <- data.frame(ID = paste0("Exp", seq_along(sanitized_rules)),exp = sanitized_rules)
full_exp <- paste0(dfrules$ID, " = ", dfrules$exp)

eval_data <- copy(dw)
for(i in full_exp) eval_data <- within(eval_data, eval(parse(text = i)))

l1 <- str_extract_all(full_exp, "Exp[0-9]+|Value_[0-9]+")
names(l1) <- dfrules$ID


# for each expression, return the faulty elements
data_evaluated_proc <- lapply(l1, function(r) {

      to_select <- c("geographicAreaM49", "timePointYears", r)
      data_evaluated <- eval_data[, to_select, with = FALSE]
      data_evaluated$ID <- r[1]
      dfm <- merge(data_evaluated, dfrules, by = "ID", all.x = TRUE)
      elements <- colnames(dfm)[grepl("Value_", colnames(dfm))]
      sdata <- data_evaluated[, elements, with = FALSE]
      dfm$NAsum <- apply(sdata, 1, function(x) sum(is.na(x)))
      dfm$fauty_na <- ifelse(dfm$NAsum == 0, "none", paste(colnames(sdata)[apply(sdata, 2, anyNA)], collapse = ", "))

      setnames(dfm, r[1], "test_Value")
      setnames(dfm, "exp", "expression")

      dfm <- dfm[, mget(c("geographicAreaM49", "timePointYears", "expression", "test_Value", "fauty_na"))]

      return(dfm)

})

# Merging data
databound  <- rbindlist(data_evaluated_proc)
names(procrules$map_priority) <- c("expression", "priority")
databound_prty <- merge(databound, procrules$map_priority, by = "expression", all.x = TRUE)
data_eval_to_false <- databound_prty[test_Value == FALSE]
data_eval_to_false <- data_eval_to_false[, list(geographicAreaM49, timePointYears, expression, test_Value, fauty_na, priority)]
databound_final <- faoswsUtil::nameData("Aquastat", "aquastat_imputation", data_eval_to_false)
databound_final  <- merge(databound_final  , dw, by = c('geographicAreaM49', 'timePointYears'), all.x = TRUE)
val_elements <- unique(unlist(str_extract_all(databound_final$exp, "Value_[0-9]+")))
df <- data.frame(val_elements)
df$aquastatElement <- substring(df$val_elements, 7)
df <- faoswsUtil::nameData("Aquastat", "aquastat_imputation",  df)
databound_final$ElementName <- paste(df$aquastatElement_description, collapse = ", ")


# Lits of validation rule - based dataframes
data_evaluated_proc_value <- lapply(l1, function(r) {

  to_select <- c("geographicAreaM49", "timePointYears", r)
  data_evaluated <- eval_data[, to_select, with = FALSE]
  data_evaluated$ID <- r[1]
  dfm_merged <- merge(data_evaluated, dfrules, by = "ID", all.x = TRUE)
  dfm_merged <- merge(dfm_merged, procrules$map_priority, by.x = "exp", by.y = "expression", all.x = TRUE)
  setnames(dfm_merged, r[1], "test_Value")

  val_elements <- unique(unlist(str_extract_all(dfm_merged$exp, "Value_[0-9]+")))
  df <- data.frame(val_elements)
  df$aquastatElement <- substring(df$val_elements, 7)
  df <- faoswsUtil::nameData("Aquastat", "aquastat_imputation",  df)
  dfm_merged$aquastatElement_description <- paste(df$aquastatElement_description, collapse = ", ")
  dfm_merged <- faoswsUtil::nameData("Aquastat", "aquastat_imputation", dfm_merged)
  setnames(dfm_merged, "exp", "expression")
  return(dfm_merged)

})


# Share folder
share_drive_folder <- file.path(R_SWS_SHARE_PATH, "AquastatValidation/output")


## Save data
databound_final_FALSE <- databound_final[test_Value == FALSE]
databound_final_FALSE_PRIORITY <- databound_final[test_Value == FALSE & priority == 1]
write.csv(databound_final, paste0(share_drive_folder, "/aquaVal_output_ALL_",dataset, ".csv"), row.names = FALSE)
write.csv(databound_final_FALSE, paste0(share_drive_folder, "/aquaVal_output_FALSE_", dataset, ".csv"),row.names = FALSE)
write.csv(databound_final_FALSE_PRIORITY, paste0(share_drive_folder, "/aquaVal_output_FALSE_PRIORITY_",dataset, ".csv"),row.names = FALSE)
saveRDS(data_evaluated_proc_value, paste0(share_drive_folder, "/validation_by_rule.rds"))




for (i in data_evaluated_proc_value) {
  names(i)[names(i)]

}
data_all <- fread("//hqlprsws1.hq.un.fao.org/sws_r_share/AquastatValidation/output/aquaVal_output_ALL_aquastat_add_indicators.csv")

data_all$Test <- ifelse(is.na(data_all$Test), 'NA', data_all$Test)
data_all$Country <- ifelse(is.na(data_all$geographicAreaM49_description), 'NA', data_all$geographicAreaM49_description)
names(data_all)
