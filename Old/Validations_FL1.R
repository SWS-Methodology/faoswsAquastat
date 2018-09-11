library(validate)
library(errorlocate)
library(dcmodify)
library(tictoc)
library(tidyverse)


# read in data output from faoswsAquastatImputation
df_imputed <- read_csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aquastat_test_imputed_virginie.csv")
df_imputed <- mutate(df_imputed, aquastatElement = paste0("Value_", aquastatElement))

# wide format 
df_imputed_wide <- dcast(df_imputed, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))
#d_imputed_wide <- dcast(d, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))

# VALIDATOR OBJECTS --------
# validator to check imputations
# Here the left hand side is checked whther it is eaqual to the right hand side
# the number of rows passing and failing the for each logical test is returned 
vimp <- validator(Value_4103 == Value_4101+Value_4102
               ,Value_4105 == Value_4104-Value_4106
               ,Value_4107 == Value_4104/(Value_4100/100)
               ,Value_4108 == Value_4109+Value_4110
               ,Value_4150 == Value_4100*Value_4155/100000
               ,Value_4157 == Value_4154+Value_4155-Value_4156
               ,Value_4158 == Value_4157*1000000/Value_4104
               ,Value_4164 == Value_4160+Value_4162+Value_4168
               ,Value_4176 == Value_4160+Value_4162+Value_4168-Value_4174
               ,Value_4182 == Value_4176+Value_4452
               ,Value_4185 == Value_4176+Value_4155
               ,Value_4187 == Value_4154+Value_4452
               ,Value_4188 == Value_4185+Value_4187-Value_4156
               ,Value_4190 == Value_4188*1000000/Value_4104
               ,Value_4192 == 100*(Value_4164+Value_4452)/(Value_4164+Value_4452+Value_4157)
               ,Value_4509 == Value_4193+Value_4194
               ,Value_4196 == Value_4509+Value_4195
               ,Value_4253 == Value_4251+Value_4252+Value_4250
               ,Value_4254 == Value_4250/Value_4253*100
               ,Value_4255 == Value_4251/Value_4253*100
               ,Value_4256 == Value_4252/Value_4253*100
               ,Value_4257 == Value_4253*1000000/Value_4104
               ,Value_4263 == Value_4253-Value_4264-Value_4265-Value_4451
               ,Value_4271 == 100*Value_4260/Value_4250
               ,Value_4273 == 100*Value_4250/Value_4188
               ,Value_4275 == 100*Value_4263/Value_4188
               ,Value_4300 == Value_4303+Value_4304
               ,Value_4305 == 100*Value_4300/Value_4103
               ,Value_4311 == Value_4308+Value_4309+Value_4310
               ,Value_4313 == Value_4311+Value_4312+Value_4316
               ,Value_4317 == Value_4313+Value_4314+Value_4315
               ,Value_4319 == 100*Value_4313/Value_4317
               ,Value_4323 == 100*Value_4320/Value_4313
               ,Value_4324 == 100*Value_4321/Value_4313
               ,Value_4325 == 100*Value_4322/Value_4313
               ,Value_4327 == 100*Value_4326/Value_4313
               ,Value_4328 == 100*Value_4318/Value_4313
               ,Value_4330 == 100*Value_4313/Value_4307
               ,Value_4331 == 100*Value_4313/Value_4103
               ,Value_4445 == 100*Value_4400/Value_4313
               ,Value_4446 == 100*Value_4303/Value_4313
               ,Value_4448 == Value_4314+Value_4315
               ,Value_4450 == 100*Value_4263/Value_4157
               ,Value_4455 == 100*Value_4454/Value_4101
               ,Value_4456 == Value_4160+Value_4162+Value_4168+Value_4170
               ,Value_4457 == Value_4251*1000000/Value_4104
               ,Value_4458 == Value_4112/Value_4104/1000
               ,Value_4459 == Value_4309+Value_4310
               ,Value_4462 == 100*Value_4379/Value_4461
               ,Value_4463 == 100*Value_4461/Value_4311 
               ,Value_4464 == 100*Value_4379/Value_4461 
               ,Value_4466 == 100*Value_4465/Value_4313 
               ,Value_4467 == 100*Value_4263/Value_4253 
               ,Value_4468 == Value_4251*1000000/Value_4106 
               ,Value_4470 == 100*Value_4103/Value_4100 
               ,Value_4471 == 1000000*Value_4197/Value_4104 
               ,Value_4514 == 100*Value_4513/Value_4313 
               ,Value_4527 == 100*Value_4526/Value_4313 
               ,Value_4531 == Value_4252*1000000/Value_4104 
               ,Value_4532 == Value_4250*1000000/Value_4104 
               ,Value_4538 == 100*Value_4108/Value_4449 
               ,Value_4540 == 100*Value_4539/Value_4313 
               ,Value_4550 == 100*Value_4263/(Value_4188-Value_4549) 
               ,Value_4551 == (Value_4552*Value_4254)+(Value_4553*Value_4256)+(Value_4554*Value_4255) 
               ,Value_4552 == ((Value_4548*Value_4555/100)/Value_4250)/1000000000 
               ,Value_4553 == (Value_4546/Value_4252)/1000000000 
               ,Value_4554 == (Value_4547/Value_4251)/1000000000 
               ,Value_4555 == 1/(1+((1-(Value_4556/100))/((Value_4556/100)*Value_4557))) 
               ,Value_4556 == 100*Value_4379/Value_4101)



# Creating an ID column to help error location
df_imputed_wide$ID <- paste(row.names(df_imputed_wide), df_imputed_wide$geographicAreaM49, df_imputed_wide$timePointYears, sep = "_")
# df_imputed_wide[is.na(df_imputed_wide)] <- 0

# Confront data and validation rules
# cf <- confront(df_imputed_wide, vimp)

# locating errors
le <- locate_errors(df_imputed_wide, vimp)
# print(le)
# summary(le)
class(le$errors)
le_df <- cbind(df_imputed_wide[,1:2], as.data.frame(le)[, 3:122]) %>%  tbl_df()




# data with no fauty elements
rle <- df_imputed_wide %>% replace_errors(le)
le <- locate_errors(rle, vimp)
summary(le)




# prepare a list of dataframes based on geographicAreaM49
list_df <- split(df_wide_imputed, df_wide_imputed$geographicAreaM49)

View(rle)
# Within each country confront the rules against the dataframe
confrontation <- function(df)  validate::confront(df, v, key = "ID") 
val <- lapply(list_df, confrontation)

# summarise confronation by country
val_summary <- function(val)  summary(val)
val_sumarising <- lapply(val, val_summary)
 
#  Detect violations by country
detect_violations <- function(cf) {
  df <- cf %>%  as.data.frame() %>%  tbl_df() %>% 
  filter(value == FALSE) %>%
  separate(ID, c("row", "gegraphicAreaM49", "timePointYears"), sep = "_") %>% 
  mutate(row = as.integer(row), timePointYears = as.integer(timePointYears))
  
  # return(unique(df$row))
}

# get the variables of the expressions
function(expression) variables(v)

violations["expression"]
# Get rows violating the rules
violations <- lapply(val, detect_violations)

# Use the violations to filter the original list of imputed datasets.
where_violations <- lapply(list_df, function(x) x %>% tbl_df() %>% filter(detect_violations(.)))











# # Within each country calculate the indicators
# calculation <- function(df)  validate::confront(df, i, key = "ID") 
# ind <- lapply(list_df, calculation)
# 
# 

# 
# ind_summary <- function(ind)  summary(ind)
# ind_sumarising <- lapply(ind, ind_summary )
# 
# 

# 
# # plot functions by set
# plot_set1 <- function(val) barplot(cf[1:23],main="Checks on the imputed data set")
# plot_set2 <- function(cf) barplot(cf[24:46],main="Checks on the imputed data set")
# plot_set3 <- function(cf) barplot(cf[47:69],main="Checks on the imputed data set")
# 
# 
# 
# 
# 
# 
# # Confrontation
# cf1 <- confront(df_wide_imputed, v, key = "ID")
# summary(cf1)
# 
# if1 <- confront(df_wide_imputed, i, key = "ID")
# summary(if1)
# 

# Check the barplot
barplot(cf1[1:23],main="Checks on the imputed data set")
barplot(cf1[24:46],main="Checks on the imputed data set")
barplot(cf1[47:69],main="Checks on the imputed data set")


# Locate errors
out <- cf1 %>%  as.data.frame() %>%  tbl_df() %>%  filter(value == FALSE)
out_i <- if1 %>%  as.data.frame() %>%  tbl_df() %>%  filter(name == Value_4263)









data("women")
women

validator(inch := 1/2/54
          , us_mean := us_mean(height))