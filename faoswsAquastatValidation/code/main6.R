##' Validation module
##' Author: Francy Lisboa
##' Date: 15/02/2019
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

dataset_name <- swsContext.datasets[[1]]@dataset


# Read in data
data <- GetData(swsContext.datasets[[1]], flags = TRUE)
dc <- copy(data)
dc <- dc[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]

# Getting info on rules
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
        # missing elements are not in the intersection between validation elements and the intersection of it and the dataset elements
        missingelements <- sort(setdiff(val_elements, intersection))

        res_list <- list(missingelements,  # missing elements
                         data_elements,    # all elements from initial data
                         val_elements,     # all elements in the validation rule
                         rules_paste)      # validation rules
        names(res_list) <- c("missingelements",
                              "data_elements",
                              "val_elements",
                              "map_priority")
        return(res_list)
}

# call the function over rules
procrules <- sanitize_rules(data = dc)


# after sanitizing rules we need to get wide format
reshape_data <- function(data) {
        d <- copy(data)
        d <- d[,.(geographicAreaM49, aquastatElement, timePointYears, Value)]
        d[, aquastatElement := paste0("Value_",aquastatElement)]
        d <- dcast(d, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = "Value")
        return(d)
}
data_wide <- reshape_data(dc)
dw <- copy(data_wide)

# To avoid code crashing, we add the missing elements to the dataset
missing_elements <-  procrules$missingelements
for (i in missing_elements) {
  dw[, i] <- NA
}

# convert sanitized rules to list object
sanitized_rules <- procrules$map_priority$vrules

# Evaluating rules
dfrules <- data.frame(ID = paste0("Exp", seq_along(sanitized_rules)),exp = procrules$map_priority$vrules)
full_exp <- paste0(dfrules$ID, " = ", dfrules$exp)
eval_data <- copy(dw)
for(i in full_exp) eval_data <- within(eval_data, eval(parse(text = i)))

# getting l;ist of components
l1 <- str_extract_all(full_exp, "Exp[0-9]+|Value_[0-9]+")
names(l1) <- dfrules$ID


# Getting primary variables (non indicators)
# calc <- ReadDatatable('calculation_rule')
# proc_rules <- stringr::str_replace_all(calc$calculation_rule, "\\[([0-9]+)\\]", "Value_\\1")
# rule_elements <- paste0("Value_", sort(unique(unlist(str_extract_all(calc$calculation_rule, regex("(?<=\\[)[0-9]+(?=\\])"))))))
# lhs <- paste0("Value_",sort(substring(calc$calculation_rule, 2, 5)))
# rhs <- sort(setdiff(rule_elements, lhs))
#

# # for each expression, return the faulty elements
# data_evaluated_proc <- lapply(l1, function(r) {
#       to_select <- c("geographicAreaM49", "timePointYears", r)
#       data_evaluated <- eval_data[, to_select, with = FALSE]
#       data_evaluated$ID <- r[1]
#       dfm <- merge(data_evaluated, dfrules, by = "ID", all.x = TRUE)
#       elements <- colnames(dfm)[grepl("Value_", colnames(dfm))]
#       sdata <- data_evaluated[, elements, with = FALSE]
#       dfm$NAsum <- apply(sdata, 1, function(x) sum(is.na(x)))
#       dfm$fauty_na <- ifelse(dfm$NAsum == 0, "none", paste(colnames(sdata)[apply(sdata, 2, anyNA)], collapse = ", "))
#       setnames(dfm, r[1], "test_Value")
#       setnames(dfm, "exp", "expression")
#       dfm <- dfm[, mget(c("geographicAreaM49", "timePointYears", "expression", "test_Value", "fauty_na"))]
#       return(dfm)
# })
#
# # Merging data
# databound  <- rbindlist(data_evaluated_proc)
# names(procrules$map_priority) <- c("expression", "priority")
# databound_prty <- merge(databound, procrules$map_priority, by = "expression", all.x = TRUE)
# databound_prty$priority <- as.integer(databound_prty$priority)
# databound_prty_false <- databound_prty[test_Value == FALSE & priority == 1]
#
#
# elements <- unlist(unique(str_extract_all(databound_prty_false$expression, 'Value_[0-9]+')))
# intersect(elements, rhs)
#
#
# #
# #
# #
# # data_evaluated_proc_value$Exp8



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
  dfm_merged <- dfm_merged[, priority := as.integer(priority)]
  dfm_merged <- dfm_merged[test_Value == FALSE & priority == 1]
  return(dfm_merged)

})

# Collecting problematic rules
data_evaluated_proc_value_filtered <- Filter(function(l) nrow(l) > 0, data_evaluated_proc_value)
data_evaluated_proc_value_filtered






# CORRECTIONS ---------------------------------------------------------------------------------------------------------------
# Focus on rules testing percentage
lowerequal_to_100 <- Filter(function(l) any(str_detect(l$expression, '<=100|<= 100')), data_evaluated_proc_value_filtered)
lowerequal_to_100_bound  <- rbindlist(lowerequal_to_100)
lowerequal_to_100_bound[, expression := substring(expression, 1, 10)]
setnames(lowerequal_to_100_bound, 'expression', 'aquastatElement')
names(lowerequal_to_100_bound)[str_detect(names(lowerequal_to_100_bound), 'Value_[0-9]+')] <- "Value"
lowerequal_to_100_bound <- lowerequal_to_100_bound[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
lowerequal_to_100_bound[, aquastatElement := substring(aquastatElement, 7)]
# Merging testing data and validated data
setnames(lowerequal_to_100_bound, 'Value', 'Value_false')
df_join <- merge(data, lowerequal_to_100_bound, all.x = TRUE )
df_join <- df_join[, Value := ifelse(!is.na(Value_false), 99.99, Value)]
df_join1 <- df_join[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]



# Correcting rules with <= sign and one element on each side
lowerequal_to_value <- Filter(function(l) any(str_detect(l$expression, 'Value_[0-9]+<=Value_[0-9]+|Value_[0-9]+<= Value_[0-9]+')), data_evaluated_proc_value_filtered)
lowerequal_to_value_long <- lapply(lowerequal_to_value, function(l) {
        dt <- l
        names(dt)[str_detect(names(dt), 'Value_[0-9]+')][1] <- 'Value_lhs'
        names(dt)[str_detect(names(dt), 'Value_[0-9]+')] <- 'Value_rhs'
        dt <- dt[, Value_lhs_new := 0.99*Value_rhs]
        element1 <- unique(str_extract_all(dt$expression, 'Value_[0-9]+'))[[1]][1]
        element2 <- unique(str_extract_all(dt$expression, 'Value_[0-9]+'))[[1]][2]
        dt <- dt[, .(geographicAreaM49, timePointYears, Value_lhs_new, Value_rhs)]
        setnames(dt, 'Value_lhs_new', element1)
        setnames(dt, 'Value_rhs', element2)
        measure_vars <- names(dt)[str_detect(names(dt), 'Value_')]
        id_vars <- names(dt)[str_detect(names(dt), 'Value_') == FALSE]
        dtmelt <- melt(dt, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
        dtmelt
})
lowerequal_to_value_long  <- rbindlist(lowerequal_to_value_long)
lowerequal_to_value_long[, aquastatElement := substring(aquastatElement, 7)]
setnames(lowerequal_to_value_long, 'Value', 'Value_false')
df_join2 <- merge(df_join1, lowerequal_to_value_long, all.x = TRUE )
df_join2 <- df_join2[, Value := ifelse(!is.na(Value_false), Value_false, Value)]
df_join2 <- df_join2[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]


# Correcting rules with >= sign and one element on each side
higherequal_to_value <- Filter(function(l) any(str_detect(l$expression, 'Value_[0-9]+>=Value_[0-9]+|Value_[0-9]+>= Value_[0-9]+')), data_evaluated_proc_value_filtered)
higherequal_to_value_long <- lapply(higherequal_to_value, function(l) {
  dt <- l
  names(dt)[str_detect(names(dt), 'Value_[0-9]+')][1] <- 'Value_lhs'
  names(dt)[str_detect(names(dt), 'Value_[0-9]+')] <- 'Value_rhs'
  dt <- dt[, Value_lhs_new := 1.05*Value_rhs]
  element1 <- unique(str_extract_all(dt$expression, 'Value_[0-9]+'))[[1]][1]
  element2 <- unique(str_extract_all(dt$expression, 'Value_[0-9]+'))[[1]][2]
  dt <- dt[, .(geographicAreaM49, timePointYears, Value_lhs_new, Value_rhs)]
  setnames(dt, 'Value_lhs_new', element1)
  setnames(dt, 'Value_rhs', element2)
  measure_vars <- names(dt)[str_detect(names(dt), 'Value_')]
  id_vars <- names(dt)[str_detect(names(dt), 'Value_') == FALSE]
  dtmelt <- melt(dt, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
  dtmelt
})
higherequal_to_value_long  <- rbindlist(higherequal_to_value_long)
higherequal_to_value_long[, aquastatElement := substring(aquastatElement, 7)]
setnames(higherequal_to_value_long, 'Value', 'Value_false')
df_join3 <- merge(df_join2, higherequal_to_value_long, all.x = TRUE )
df_join3 <- df_join3[, Value := ifelse(!is.na(Value_false), Value_false, Value)]
df_join3 <- df_join3[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]

#
higherequal_division_to_value <- Filter(function(l) any(str_detect(l$expression, 'Value_[0-9]+/Value_[0-9]+>=|Value_[0-9]+ / Value_[0-9]+>=')), data_evaluated_proc_value_filtered)



# Correcting rules with <= sign and in a division
lowerequal_division_to_value <- Filter(function(l) any(str_detect(l$expression, 'Value_[0-9]+/Value_[0-9]+<=|Value_[0-9]+ / Value_[0-9]+<=')), data_evaluated_proc_value_filtered)
lowerequal_division_to_value_long <- lapply(lowerequal_division_to_value , function(l) {
  dt <- l
  factor <- as.numeric(unique(str_extract(dt$expression, '[0-9]\\.[0-9]+$')))
  names(dt)[str_detect(names(dt), 'Value_[0-9]+')][1] <- 'Value_lhs'
  names(dt)[str_detect(names(dt), 'Value_[0-9]+')] <- 'Value_rhs'
  dt <- dt[, Value_lhs_new := 0.99*(factor*Value_rhs)]
  element1 <- unique(str_extract_all(dt$expression, 'Value_[0-9]+'))[[1]][1]
  element2 <- unique(str_extract_all(dt$expression, 'Value_[0-9]+'))[[1]][2]
  dt <- dt[, .(geographicAreaM49, timePointYears, Value_lhs_new, Value_rhs)]
  setnames(dt, 'Value_lhs_new', element1)
  setnames(dt, 'Value_rhs', element2)
  measure_vars <- names(dt)[str_detect(names(dt), 'Value_')]
  id_vars <- names(dt)[str_detect(names(dt), 'Value_') == FALSE]
  dtmelt <- melt(dt, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
  dtmelt
})






calc <- ReadDatatable('calculation_rule')
proc_rules <- stringr::str_replace_all(calc$calculation_rule, "\\[([0-9]+)\\]", "Value_\\1")
rule_elements <- paste0("Value_", sort(unique(unlist(str_extract_all(calc$calculation_rule, regex("(?<=\\[)[0-9]+(?=\\])"))))))
lhs <- paste0("Value_",sort(substring(calc$calculation_rule, 2, 5)))
rhs <- sort(setdiff(rule_elements, lhs))

# Getting expression that evaluated to FALSE
data_eval_to_false <- databound_prty[test_Value == FALSE & priority == 1]
data_eval_to_false <- data_eval_to_false[, mget(c("geographicAreaM49", "timePointYears", "expression", "test_Value", "fauty_na", 'priority'))]
data_eval_to_false <- nameData('aquastat', 'aquastat_dan', data_eval_to_false)
data_eval_to_false <- data_eval_to_false[, mget(c("geographicAreaM49",
                                                  "geographicAreaM49_description",
                                                  "timePointYears",
                                                  "expression",
                                                  "test_Value",
                                                  'priority'))]

names(data_eval_to_false) <- c("country_code", 'country_name', "year", "expression", "test_value", 'priority')




# SAVE DATA ------------------------
if (nrow(data_eval_to_false) == 0) {
  df <- data_eval_to_false[1,]
  df[is.na(df)] <- 0
  df$date <- Sys.time()
  df$dataset <- dataset_name
  df <- df[, c("country_code", "country_name", "year", "expression", "test_value", 'date', 'dataset')]
} else {
  df <- data_eval_to_false
  df$date <- Sys.time()
  df$dataset <- dataset_name
  df <- df[, c("country_code", "country_name", "year", "expression", "test_value", 'date', 'dataset')]
}


# Get all columns as character of strings
#df <- dplyr::mutate_at(df %>% tbl_df(), c("country_code","country_name", "year", "expression", "test_value", "date", "dataset"), as.character)
df$test_value <- 'false'
df$date <- as.character(df$date)


df <- data.table(df)


# WIPE OUT AND SAVE NEW VALIDATION DATATABLE ---------------------------
table <- 'aquastatvalidation'
current_table <- ReadDatatable(table, readOnly = FALSE)
changeset <- Changeset(table)

# Delete
AddDeletions(changeset, current_table)
Finalise(changeset)

# Insert
AddInsertions(changeset, df)
Finalise(changeset)

paste0("Validation of the dataset ", dataset_name, " completed, Check out the ", table, " datatable")
