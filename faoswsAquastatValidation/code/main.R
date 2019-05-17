##' AddIndicators module
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
  library(flexdashboard)
  library(shiny)
 })



if(CheckDebug()){

  library(faoswsModules)
  SETTINGS = ReadSettings("~/github/faoswsAddIndicators/sws.yml")

  ## If you're not on the system, your settings will overwrite any others
  R_SWS_SHARE_PATH = SETTINGS[["share"]]

  ## Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])

  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])

}

# The input data reside in the aquastat_clone dataset in SWS, which has been named aquastat_dan
imputKey <- DatasetKey(
  domain = "Aquastat",
  dataset = "aquastat_dan",
  dimensions = list(
    Dimension(name = "geographicAreaM49",
              keys = GetCodeList('Aquastat', 'aquastat_dan', 'geographicAreaM49')[type == 'country', code]),
    Dimension(name = "aquastatElement", keys = GetCodeList('Aquastat', 'aquastat_dan', 'aquastatElement')[, code]),
    Dimension(name = "timePointYears", keys = as.character(1961:2017))
  )
)


# read in data output from faoswsAquastatRecalculation
data <- readRDS("data/old/starting_data.rds")
rules <- read.csv("data/validation_rules.csv", header = TRUE)
data <- setDT(data)
dc <- copy(data)
dc <- dc[, .(geographicAreaM49, aquastatElement, timePointYears, Value)]



sanitize_rules <- function(data) {

        ## read in the validation rules
        #rules <- ReadDatatable("validation_rules")
        rules <- read.csv("data/validation_rules.csv", header = TRUE)

        # make rules by joining columns
        rules_paste <- data.frame(vrules = trimws(paste0(rules$lhs, rules$operator, rules$rhs)))
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

# Save results
# saveRDS(procrules[1], file = paste0(getwd(), "/", names(procrules[1]), ".rds"))
# saveRDS(procrules[2], file = paste0(getwd(), "/", names(procrules[2]), ".rds"))
# saveRDS(procrules[3], file = paste0(getwd(), "/", names(procrules[3]), ".rds"))


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

databound  <- rbindlist(data_evaluated_proc)
names(procrules$map_priority) <- c("expression", "priority")
databound_prty <- merge(databound, procrules$map_priority, by = "expression", all.x = TRUE)
databound_prty <- databound_prty[, list(geographicAreaM49, timePointYears, expression, test_Value, fauty_na, priority)]
databound_final <- faoswsUtil::nameData("Aquastat", "aquastat_imputation", databound_prty)
#
#
# databound_final  <- merge(databound_prty , dw, by = c('geographicAreaM49', 'timePointYears'), all.x = TRUE)
# val_elements <- unique(unlist(str_extract_all(databound_final$exp, "Value_[0-9]+")))
# df <- data.frame(val_elements)
# df$aquastatElement <- substring(df$val_elements, 7)
# df <- faoswsUtil::nameData("Aquastat", "aquastat_imputation",  df)
# databound_final$ElementName <- paste(df$aquastatElement_description, collapse = ", ")
# databound_final <- faoswsUtil::nameData("Aquastat", "aquastat_imputation", databound_final)
# databound_final <- unique(databound_final)



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

to_dash <- copy(databound_final)
to_dash[, geographicAreaM49 := NULL]
to_dash[, timePointYears_description := NULL]
setnames(to_dash, 'geographicAreaM49_description', "Country")
setnames(to_dash, 'timePointYears', "Years")
setnames(to_dash, 'test_Value', "Test")
setnames(to_dash, 'expression', "Expression")
setnames(to_dash, 'priority', "Priority")
filename_test <- tempfile(pattern = "validation_data", fileext = ".RData")
save(to_dash, file = filename_test)




saveRDS(to_dash, "output/to_dash.rds")
# # for each expression, return the dataset with the expression, the elements of the expression and the
# data_evaluated_proc_value <- lapply(l1, function(r) {
#
#   to_select <- c("geographicAreaM49", "timePointYears", r)
#   data_evaluated <- eval_data[, to_select, with = FALSE]
#   data_evaluated$ID <- r[1]
#   dfm_merged <- merge(data_evaluated, dfrules, by = "ID", all.x = TRUE)
#   dfm_merged <- merge(dfm_merged, procrules$map_priority, by.x = "exp", by.y = "expression", all.x = TRUE)
#   setnames(dfm_merged, r[1], "test_Value")
#
#   val_elements <- unique(unlist(str_extract_all(dfm_merged$exp, "Value_[0-9]+")))
#   df <- data.frame(val_elements)
#   df$aquastatElement <- substring(df$val_elements, 7)
#   df <- faoswsUtil::nameData("Aquastat", "aquastat_imputation",  df)
#   dfm_merged$aquastatElement_description <- paste(df$aquastatElement_description, collapse = ", ")
#   dfm_merged <- faoswsUtil::nameData("Aquastat", "aquastat_imputation", dfm_merged)
#   setnames(dfm_merged, "exp", "expression")
#   return(dfm_merged)
#
# })
#
#
# names <- lapply(data_evaluated_proc_value, function(l) { v_name <- paste(l[1, 1]) })
# names(data_evaluated_proc_value) <- names

#
# false_test <- databound_final[test_Value == FALSE]
# false_elements <- substring(unlist(unique(str_extract_all(false_test$expression, 'Value_[0-9]+'))), 7)
#
#
# false_ctr_year <- unique(false_test[, list(geographicAreaM49, timePointYears)])
# getdata <- merge(false_ctr_year, data, by = c('geographicAreaM49', 'timePointYears'), all.x = TRUE)
# false_elements_dt <- getdata[aquastatElement %in% false_elements]
# false_elements_dt  <- faoswsUtil::nameData("Aquastat", "aquastat_imputation", false_elements_dt)


# saving results in tempfiles of the user
# filename_test <- paste0(getwd(), "/", "validation_test.RData")
filename_test <- tempfile(pattern = "validation_test", fileext = ".RData")
save(databound_final, file = filename_test)


filename_value <- tempfile(pattern = "validation_value", fileext = ".RData")
save(data_evaluated_proc_value, file = filename_value)
# #dataaaaa <- get(load(temp_value))


validation_report <- c("
---
title: 'AQUASTAT VALIDATION CHECK'
output:
  flexdashboard::flex_dashboard:
  orientation: rows
  vertical_layout: fill
  social: menu
runtime: shiny
---

```{r global, include=FALSE}
library(flexdashboard)
library(shiny)
library(DT)
library(dplyr)

# load and processing data
data_vtest <- get(load(filename_test)) %>%
  tbl_df() %>%
  select(geographicAreaM49_description,
         timePointYears,
         expression,
         test_Value,
         fauty_na,
         priority)

names(data_vtest) <- c('Country',
                       'Year',
                       'Expression',
                       'Test',
                       'Faulty_na',
                       'Priority')

data_vtest$Test <- ifelse(is.na(data_vtest$Test), 'NA', data_vtest$Test)

```

Sidebar {.sidebar}
============================================================================================================


```{r}
# inputs
uiOutput('country')
uiOutput('exp')
uiOutput('test')
uiOutput('prior')

```

***faoswsAquastatValidation overview***
==========================================================================================================

### <font size='8'> ***AQUASTAT validation rules output*** </font>

<font size='6', family='Helvetica'> This interactive dashboard allows the user to give a
close look at the dataset produced by the faoswsAquastatValidation
module. This module logically tests a pre-defined set of
validation rules at the country and year levels. Each validation
rule, here called Expression, can evaluates to **TRUE** case it passes the test;
**FALSE** case it fails; or **NA** in case of one or more variables composing the
evaluated rule are missing.

Description of columns in the ouptut:

- **Expression** is the logically tested validation rule in the country *i* and year *t*;
- **Test** is the value of the logical test in the country *i* and year *t*;
- **Fauty_na** is a vector of character strings pointing the missing elements in the Expression in the country *i* and year *t*;
- **Priority** is a binary variable indicating the order of importance of the rules in the AQUASTAT context: 1 (MUST), 0 (SHOULD).
</font>

Data Dashboard
============================================================================================================

Row {data-width = 200}
-------------------------------------------------------------------------------------------------------------------------

### <font size=4>**Share of output where validation rules were evaluated to NA** </font> {.value-box}

```{r}
# Emit the nonevaluate rules rate
na_test <- reactive({data_vtest %>% filter(Test == 'NA')})

renderValueBox({
  na_share <- (nrow(na_test())/nrow(data_vtest)) * 100
  valueBox(
    value = na_share,
    icon = 'fa-area-chart'
  )
})
```

### <font size=4>**Share of output where validation rules were evaluated to TRUE** </font> {.value-box}
```{r}
# Emit the nonevaluate rules rate
true_test <- reactive({data_vtest %>% filter(Test == TRUE)})
renderValueBox({
  true_share <- (nrow(true_test())/nrow(data_vtest)) * 100
  valueBox(
    value = true_share,
    icon = 'fa-area-chart'
  )
})
```

### <font size=4> **Share of output where validation rules were evaluated to FALSE**</font> {.value-box}
```{r}
# Emit the nonevaluate rules rate
false_test <- reactive({data_vtest %>% filter(Test == FALSE)})
renderValueBox({
  false_share <- (nrow(false_test())/nrow(data_vtest)) * 100
  valueBox(
    value = false_share,
    icon = 'fa-area-chart'
  )
})
```

Row
----------------------------------------------------------------------------------------------------------------------------------------

### <font size=1> ***Filtered data** </font>
```{r}

output$country <- renderUI({

                       countrylist <- sort(unique(as.vector(data_vtest$Country)), decreasing = FALSE)
                       countrylist <- append('All', countrylist)
                       selectizeInput('countrychoose', 'Country:', countrylist)

                       })

# Expression UI
output$exp <- renderUI({

  expressionlist <- sort(unique(as.vector(data_vtest$Expression)), decreasing = FALSE)
  expressionlist <- append('All', expressionlist)
  selectizeInput('expressionchoose', 'Expression:', expressionlist)

})

# Test selection
output$test <- renderUI({

  radioButtons('testchoose', 'Test:', c('All', TRUE, FALSE))

})

# Priority selection
output$prior <- renderUI({

  radioButtons('priorchoose', 'Priority:', c('All', 1, 0))

})



df <- reactive({

  req(input$countrychoose)
  req(input$expressionchoose)
  req(input$testchoose)
  req(input$priorchoose)

  # country filter
  if (input$countrychoose == 'All') {

    ctr_f <- quote(Country != '@@@@@')

  } else {
    ctr_f <- quote(Country == input$countrychoose)
  }


  # expression filter

  if (input$expressionchoose == 'All') {

    exp_f <- quote(Expression != '@@@@@')

  } else {
    exp_f <- quote(Expression == input$expressionchoose)
  }

  # test filter
  if (input$testchoose == 'All') {

    test_f <- quote(Test != '@@@@@')

  } else {
    test_f <- quote(Test == input$testchoose)
  }

  # priority filter
  if (input$priorchoose == 'All') {

    prior_f <- quote(Priority != '@@@@@')

  } else {

    prior_f <- quote(Priority == input$priorchoose)
  }


  data_vtest %>%
    filter_(ctr_f) %>%
    filter_(exp_f) %>%
    filter_(test_f) %>%
    filter_(prior_f)

})


DT::renderDT({



  datatable(df() ,
            extensions = 'Buttons',
            options = list(
              dom = 'Blfrtip',
              buttons = c('copy', 'csv', 'excel', 'pdf'),
              lengthMenu = list(c(15, 50, 100, -1), c(15, 50, 100, 'All'))))

})

```
")




tmp <- tempfile(fileext = "validation_report.Rmd")
cat(validation_report, file = tmp)
rmarkdown::render(tmp, "flex_dashboard")

