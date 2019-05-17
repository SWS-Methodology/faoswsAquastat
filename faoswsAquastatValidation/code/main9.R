print('starting module')
options(scipen = 999)
##' Validation module
##' Author: Francy Lisboa
##' Date: 15/02/2019
##' Purpose: this modules utilizes the pre-defined validation rules to output a dataset indicating country and year that
##' the logical test for a given rule was violated, ie. FALSE. The dataset has:
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
  library(sendmailR)
  # library(rmarkdown)
 })


R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
if(CheckDebug()){
      library(faoswsModules)
      SETTINGS = ReadSettings("~/github/faoswsAquastatValidation/sws.yml")
      Sys.setenv("R_SWS_SHARE_PATH" = SETTINGS[["share"]])
      R_SWS_SHARE_PATH = SETTINGS[["share"]]
      SetClientFiles(SETTINGS[["certdir"]])
      GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                         token = SETTINGS[["token"]])
}

# Global variables
dataset_name <- swsContext.datasets[[1]]@dataset
domain_name <- swsContext.datasets[[1]]@domain
dimensions <- names(swsContext.datasets[[1]]@dimensions)

# print('reading in data')
# # Read in data
data <- GetData(swsContext.datasets[[1]], flags = TRUE)
print('data ready')
dc <- copy(data)
dc <- dc[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]
dc[, Value := as.numeric(format(round(Value, 2), nsmall = 2))]


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


# Lits of validation rule - based dataframes
data_evaluated_proc_value <- lapply(l1, function(e) {
      to_select <- c("geographicAreaM49", "timePointYears", e)
      data_evaluated <- eval_data %>% tbl_df() %>% dplyr::select_(.dots = to_select) %>%  setDT()
      data_evaluated$ID <- e[1]
      dfm_merged <- merge(data_evaluated, dfrules, by = "ID", all.x = TRUE)
      dfm_merged <- merge(dfm_merged, procrules$map_priority, by.x = "exp", by.y = "vrules", all.x = TRUE)
      setnames(dfm_merged, e[1], "test_Value")
      setnames(dfm_merged, "exp", "expression")
      dfm_merged <- dfm_merged[, priority := as.integer(priority)]
      dfm_merged <- dfm_merged[test_Value == FALSE & priority == 1]
      return(dfm_merged)
})

# Collecting problematic rules
data_evaluated_proc_value_filtered <- Filter(function(l) nrow(l) > 0, data_evaluated_proc_value)





# Long format
faulty_rules_dfs <- copy(data_evaluated_proc_value_filtered)
faulty_rules_proc <- lapply(faulty_rules_dfs, function(l){
dt <- l
measure_vars <- names(dt)[str_detect(names(dt), 'Value_[0-9]+')]
id_vars <- c("geographicAreaM49", "timePointYears", 'expression', 'priority')
dtmelt <- melt(dt, id.vars = id_vars, measure.vars =  measure_vars, variable.name = 'aquastatElement', value.name = 'Value')
dtmelt[, aquastatElement := substring(aquastatElement, 7)]
setnames(dtmelt, 'Value', 'Value_false')
dtmelt
})

faulty_rules_proc0 <- rbindlist(faulty_rules_proc)
df_join <- merge(data, faulty_rules_proc0, by = c("geographicAreaM49", "timePointYears", 'aquastatElement'), all.y = TRUE)
data_to_save <- df_join[!is.na(Value_false)]


# SAVE  -----------------------------------------------------------------------------------------------
# Report
report_to_publish <- c("
#' ---
#' title: 'Aquastat Validation report'
#' author: 'fasoswsAquastatValidation module'
#' output:
#'     pdf_document
#' ---

#' ## Module background
#'  The module executes on different AQUASTAT living in the SWS either already saved in the database or saved in the session.
#'  Validation rules pre-defined by AQUASTAT are logical expressions representing domain knowledge expectations with regard to
#'  the values boundaries and relationship between aquastat elements/indicators. These validation rules are ran against  the data and,
#'  as result, countries, years, and figures in which the expressions have failed to pass are retuned as a single dataset.

#'  ### Module datasets and datatables
#'  The faoswsAquastatValidation module works with one SWS dataset and SWSCdatatable at the time. Currently, the datasets that can be used by the module are:
#'  __aquastat_baseline__, __aquastat_enr__, __aquastat_dan__, and __aquastat__. The only SWS datatable that is need for running the module is
#'  the __validation_rules_clone__ which contains the latest version of the expressions that need to be tested against the whole dataset.
#'
#'  ### Running the module
#'  The Methodological Innovation Team in ESS grants permissions to AQUASTAT - CBL user to run the faoswsAquastatValidation plugin.
#'  Permission granted users need to go though the following steps in order to obtain the results:
#'  1 - In the FAOSWS homepage[://hqlqasws1.hq.un.fao.org:8443/sws/], click on __New Query__';
#'  2 - Select  Aquastat domain and in the dataset tab, the dataset the validation should run for.
#'  3 - Double click on the __Run plugin__ button at the far top right on the screen.
#'  4 - In the dialogue box, select the faoswsAquastatValidation module and then click on the __Run plugin__ button at the bottom right.
#'  5 - Wait for the end of the process.
#'
#'  ### The Module results communication
#'  With the end of the processing, the faoswsAquastatValidation module communicates with the user by corporate email.
#'  The user is expeted to receive an email from __sws@fao.org__ with the following subject: __The AquastatValidation is finished__.
#'  In the email body the user will see two files with the same name: validation_'datasetname'.csv, and validation_'datasetname'.rds
#'  The .rds file is for users comfortable with R and is a list of datasets separated by validation rule. The .csv file is the main file and
#'  contains the:#'
#'  * __geographicAreaM49__: character vector with the country codes;
#'  * __geographicAreaM49_description__: character vector with the country names;
#'  * __timePointYears: character vector__ with the years;
#'  * __aquastatElement: aquastat element__ codes;
#'  * __aquastatElement_description__: aquastat element names;
#'  * __Value__: the value that has violated the logical test;
#'  * __flagObservationStatus__: character vector with the observation flag of the faulty value;
#'  * __flagMethod__: character vector indicating the method used to obtain the faulty value;
#'  * __expression__: character vector with the expressions that did not pass the logical test;
#'  * priority: integer vector indicating the priority of the rule with respect to validation; e.g. priority = 1 indicate user MUST check.
#'

#'  ## FIN
#'  The faoswsAquastatValidation module is an auxiliary tool for AQUASTAT to spot on potential errors in datasets. The user can now use
#'  the plugin to identify error and use domain knowledge to change figures accordingly.
")

res_meta_wide <- lapply(data_evaluated_proc_value_filtered, function(l) nameData('aquastat', 'aquastat_dan', l))
res_meta_long <- nameData('aquastat', 'aquastat_dan', data_to_save)
filepattern <- paste0('validation_', dataset_name)

dtemp_csv <- tempfile(pattern = filepattern,  fileext = c(".csv"))
write.csv(res_meta_long, file = dtemp_csv, row.names = FALSE)

dtemp_rds <- tempfile(pattern = filepattern,  fileext = c(".rds"))
saveRDS(res_meta_wide , dtemp_rds)

tempreport_pdf <- tempfile(pattern = filepattern,  fileext = c(".pdf"))
cat(report_to_publish, file = tempreport_pdf)
rmarkdown::render(tempreport_pdf, "pdf_document")

# SEND EMAIL --------------------------------------------------------------------------------------------
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
result <-  'The AquastatValidation is finished!'
body <- c(result, list(dtemp_csv, dtemp_rds, tempreport_pdf))

print('sending email to the user')
send_mail(from = from, to = to, subject = result , body = body)
print('email sent')
print('Waiting for the email...')
