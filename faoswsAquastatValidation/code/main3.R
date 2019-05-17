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

# swsContext.computationParams$datapath
# root <-  '//hqlprsws1.hq.un.fao.org/sws_r_share/AquastatValidation/output/'
# datapath <- swsContext.computationParams
# filefull <- paste0(root, datapath)
#
# if (str_detect(datapath, 'csv$'))
#   data <- fread('//hqlprsws1.hq.un.fao.org/sws_r_share/AquastatValidation/output/Baseline/baseline.csv')
#
# if (str_detect(datapath, 'rds$'))

data <- readRDS('//hqlprsws1.hq.un.fao.org/sws_r_share/AquastatValidation/output/Baseline/baseline.rds')
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

# Getting expression that evaluated to FALSE
data_eval_to_false <- databound_prty[test_Value == FALSE & priority == 1]
data_eval_to_false <- data_eval_to_false[, mget(c("geographicAreaM49", "timePointYears", "expression", "test_Value", "fauty_na", 'priority'))]
names(data_eval_to_false) <- c("country", "year", "expression", "test_value", "fauty_na", 'priority')

# SAVE DATA
if (nrow(data_eval_to_false) == 0) {
  df <- data_eval_to_false[1,]
  df[is.na(df)] <- 0
  df$date <- Sys.time()
  df <- df[, c("country", "year", "expression", "test_value", "fauty_na", 'date')]
} else {
  df <- data_eval_to_false
  df$date <- Sys.time()
  df <- df[, c("country", "year", "expression", "test_value", "fauty_na", 'date')]
}


# Save DataTable
table = 'aquastat_validation'
## Erase all data in test table
# Define changeset object
changeset <- Changeset(table)
# Add rows to delete to changeset
AddDeletions(changeset, df)
# Send modifications to the server
Finalise(changeset)
