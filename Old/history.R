library(validate)
library(dcmodify)
library(errorlocate)
library(tidyverse)

# read in data output from faoswsAquastatImputation
df_imputed <- read.csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aquastat_test_imputed.csv")

df_definitions <- read.csv("~./github/Aquastat/Aquastat_Definitions.csv")

# wide format
df_wide_imputed <- dcast(df_imputed, geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))

aquastat_indicators <- c("4103", "4105", "4107", "4456", "4108", "4150", "4157", "4158", 
  "4164", "4176", "4182", "4185", "4187", "4188", "4190", "4192", 
  "4509", "4196", "4253", "4254", "4255", "4256", "4257", "4263", 
  "4271", "4273", "4275", "4300", "4305", "4311", "4313", "4317", 
  "4319", "4323", "4324", "4325", "4327", "4328", "4330", "4331", 
  "4445", "4446", "4448", "4450", "4455", "4457", "4458", "4459", 
  "4462", "4463", "4464", "4466", "4467", "4468", "4470", "4471", 
  "4514", "4527", "4531", "4532", "4538", "4540", "4550", "4551", 
  "4552", "4553", "4554", "4555", "4556")

# ind_definitions <- filter(df_definitions, Element %in% aquastat_indicators)
# dput(setdiff(aquastat_indicators, ind_definitions$Element))


proc_rules <- c("Value_4103=Value_4101 + Value_4102", "Value_4105=Value_4104 - Value_4106", 
  "Value_4107=Value_4104/(Value_4100/100)", "Value_4456=Value_4160+Value_4162+Value_4168+Value_4170", 
  "Value_4108=Value_4109+Value_4110", "Value_4150=Value_4100 * Value_4155 / 100000", 
  "Value_4157=Value_4154+Value_4155-Value_4156", "Value_4158=Value_4157*1000000/Value_4104", 
  "Value_4164=Value_4160+Value_4162+Value_4168", "Value_4176=Value_4160+Value_4162+Value_4168-Value_4174", 
  "Value_4182=Value_4176+Value_4452", "Value_4185=Value_4176+Value_4155", 
  "Value_4187=Value_4154+Value_4452", "Value_4188=Value_4185+Value_4187-Value_4156", 
  "Value_4190=Value_4188*1000000/Value_4104", "Value_4192=100*(Value_4164+Value_4452)/(Value_4164+Value_4452+Value_4157)", 
  "Value_4509=Value_4193+Value_4194", "Value_4196=Value_4509+Value_4195", 
  "Value_4253=Value_4251+Value_4252+Value_4250", "Value_4254=Value_4250/Value_4253*100", 
  "Value_4255=Value_4251/Value_4253*100", "Value_4256=Value_4252/Value_4253*100", 
  "Value_4257=Value_4253*1000000/Value_4104", "Value_4263=Value_4253-Value_4264-Value_4265-Value_4451", 
  "Value_4271=100*Value_4260/Value_4250", "Value_4273=100*Value_4250/Value_4188", 
  "Value_4275=100*Value_4263/Value_4188", "Value_4300=Value_4303+Value_4304", 
  "Value_4305=100*Value_4300/Value_4103", "Value_4311=Value_4308+Value_4309+Value_4310", 
  "Value_4313=\tValue_4311+Value_4312+Value_4316", "Value_4317=\tValue_4313+Value_4314+Value_4315", 
  "Value_4319=100*Value_4313/Value_4317", "Value_4323=100*Value_4320/Value_4313", 
  "Value_4324=100*Value_4321/Value_4313", "Value_4325=100*Value_4322/Value_4313", 
  "Value_4327=100*Value_4326/Value_4313", "Value_4328=100*Value_4318/Value_4313", 
  "Value_4330= 100*Value_4313/Value_4307", "Value_4331=100*Value_4313/Value_4103", 
  "Value_4445=100*Value_4400/Value_4313", "Value_4446=100*Value_4303/Value_4313", 
  "Value_4448=Value_4314+Value_4315", "Value_4450=100*Value_4263/Value_4157", 
  "Value_4455=100*Value_4454/Value_4101", "Value_4457=Value_4251*1000000/Value_4104", 
  "Value_4458=Value_4112/Value_4104/1000", "Value_4459=Value_4309+Value_4310", 
  "Value_4462=\t100*Value_4379/Value_4461", "Value_4463=100*Value_4461/Value_4311", 
  "Value_4464=100*Value_4379/Value_4461", "Value_4466=100*Value_4465/Value_4313", 
  "Value_4467=100*Value_4263/Value_4253", "Value_4468=Value_4251*1000000/Value_4106", 
  "Value_4470=100*Value_4103/Value_4100", "Value_4471=1000000*Value_4197/Value_4104", 
  "Value_4514=100*Value_4513/Value_4313", "Value_4527=100*Value_4526/Value_4313", 
  "Value_4531=\tValue_4252*1000000/Value_4104", "Value_4532=Value_4250*1000000/Value_4104", 
  "Value_4538=100*Value_4108/Value_4449", "Value_4540=100*Value_4539/Value_4313", 
  "Value_4550=100*Value_4263/(Value_4188-Value_4549)", "Value_4551=(Value_4552*Value_4254)+(Value_4553*Value_4256)+(Value_4554*Value_4255)", 
  "Value_4552=\t((Value_4548*Value_4555/100)/Value_4250)/1000000000", 
  "Value_4553=(Value_4546/Value_4252)/1000000000", "Value_4554=\t(Value_4547/Value_4251)/1000000000", 
  "Value_4555=1/(1+((1-(Value_4556/100))/((Value_4556/100)*Value_4557)))", 
  "Value_4556=100*Value_4379/Value_4101")



df_rules <- data.frame(rule = str_replace_all(proc_rules,"=", "=="),label = aquastat_indicators)
v <- validator(.data=df_rules)
names(v) <- calc_names


df_ind <- data.frame(rule = substring(proc_rules, 12), label = paste0("ind_", aquastat_indicators))
i <- indicator(.data=df_ind)



rule01 <- paste0(unique(df_imputed$aquastatElement), " == ", "ref$", unique(df_imputed$aquastatElement))
df_rule_cross <- data.frame(rule = rule01, label = as.character(unique(df_imputed$aquastatElement)))
v_cross <- validator(.data = df_rule_cross)


cross_ref <- confront(dat = df_wide_imputed, x = v_cross, ref =  dt_wide_value) 
summary(cross_ref )


Path = "~./github/Aquastat/modules/faoswsAquastatImputation/output/Figs/cross_validation_plots.pdf"
pdf(file=Path, height = 16, width = 11) 
plot_set <- function(cf) barplot(cross_ref, main = "Imputed versus NonImputed") 
system.time(ggy <- lapply(1:length(cross_ref ), plot_set)) 
dev.off()

# crossdata_lowlim <- validator(
#   uplim_Value_4103 >  0.95 * ref$Value_4103
#   , uplim_Value_4105 >  0.95 * ref$Value_4105
#   , uplim_Value_4107 >  0.95 * ref$Value_4107
#   , uplim_Value_4456 >  0.95 * ref$Value_4456
#   , uplim_Value_4108 >  0.95 * ref$Value_4108
#   , uplim_Value_4150 >  0.95 * ref$Value_4150
#   , uplim_Value_4157 >  0.95 * ref$Value_4157
#   , uplim_Value_4158 >  0.95 * ref$Value_4158
#   , uplim_Value_4164 >  0.95 * ref$Value_4164
#   , uplim_Value_4176 >  0.95 * ref$Value_4176
#   , uplim_Value_4182 >  0.95 * ref$Value_4182
#   , uplim_Value_4185 >  0.95 * ref$Value_4185
#   , uplim_Value_4187 >  0.95 * ref$Value_4187
#   , uplim_Value_4188 >  0.95 * ref$Value_4188
#   , uplim_Value_4190 >  0.95 * ref$Value_4190
#   , uplim_Value_4192 >  0.95 * ref$Value_4192
#   , uplim_Value_4509 >  0.95 * ref$Value_4509
#   , uplim_Value_4196 >  0.95 * ref$Value_4196
#   , uplim_Value_4253 >  0.95 * ref$Value_4253
#   , uplim_Value_4254 >  0.95 * ref$Value_4254
#   , uplim_Value_4255 >  0.95 * ref$Value_4255
#   , uplim_Value_4256 >  0.95 * ref$Value_4256
#   , uplim_Value_4257 >  0.95 * ref$Value_4257
#   , uplim_Value_4263 >  0.95 * ref$Value_4263
#   , uplim_Value_4271 >  0.95 * ref$Value_4271
#   , uplim_Value_4273 >  0.95 * ref$Value_4273
#   , uplim_Value_4275 >  0.95 * ref$Value_4275
#   , uplim_Value_4300 >  0.95 * ref$Value_4300
#   , uplim_Value_4305 >  0.95 * ref$Value_4305
#   , uplim_Value_4311 >  0.95 * ref$Value_4311
#   , uplim_Value_4313 >  0.95 * ref$Value_4313
#   , uplim_Value_4317 >  0.95 * ref$Value_4317
#   , uplim_Value_4319 >  0.95 * ref$Value_4319
#   , uplim_Value_4323 >  0.95 * ref$Value_4323
#   , uplim_Value_4324 >  0.95 * ref$Value_4324
#   , uplim_Value_4325 >  0.95 * ref$Value_4325
#   , uplim_Value_4327 >  0.95 * ref$Value_4327
#   , uplim_Value_4328 >  0.95 * ref$Value_4328
#   , uplim_Value_4330 >  0.95 * ref$Value_4330
#   , uplim_Value_4331 >  0.95 * ref$Value_4331
#   , uplim_Value_4445 >  0.95 * ref$Value_4445
#   , uplim_Value_4446 >  0.95 * ref$Value_4446
#   , uplim_Value_4448 >  0.95 * ref$Value_4448
#   , uplim_Value_4450 >  0.95 * ref$Value_4450
#   , uplim_Value_4455 >  0.95 * ref$Value_4455
#   , uplim_Value_4457 >  0.95 * ref$Value_4457
#   , uplim_Value_4458 >  0.95 * ref$Value_4458
#   , uplim_Value_4459 >  0.95 * ref$Value_4459
#   , uplim_Value_4462 >  0.95 * ref$Value_4462
#   , uplim_Value_4463 >  0.95 * ref$Value_4463
#   , uplim_Value_4464 >  0.95 * ref$Value_4464
#   , uplim_Value_4466 >  0.95 * ref$Value_4466
#   , uplim_Value_4467 >  0.95 * ref$Value_4467
#   , uplim_Value_4468 >  0.95 * ref$Value_4468
#   , uplim_Value_4470 >  0.95 * ref$Value_4470
#   , uplim_Value_4471 >  0.95 * ref$Value_4471
#   , uplim_Value_4514 >  0.95 * ref$Value_4514
#   , uplim_Value_4527 >  0.95 * ref$Value_4527
#   , uplim_Value_4531 >  0.95 * ref$Value_4531
#   , uplim_Value_4532 >  0.95 * ref$Value_4532
#   , uplim_Value_4538 >  0.95 * ref$Value_4538
#   , uplim_Value_4540 >  0.95 * ref$Value_4540
#   , uplim_Value_4550 >  0.95 * ref$Value_4550
#   , uplim_Value_4551 >  0.95 * ref$Value_4551
#   , uplim_Value_4552 >  0.95 * ref$Value_4552
#   , uplim_Value_4553 >  0.95 * ref$Value_4553
#   , uplim_Value_4554 >  0.95 * ref$Value_4554
#   , uplim_Value_4555 >  0.95 * ref$Value_4555
#   , uplim_Value_4556 >  0.95 * ref$Value_4556)
# 
# 
# crossdata_uplim <- validator(
#   uplim_Value_4103 <  1.05 * ref$Value_4103
#   , uplim_Value_4105 <  1.05 * ref$Value_4105
#   , uplim_Value_4107 <  1.05 * ref$Value_4107
#   , uplim_Value_4456 <  1.05 * ref$Value_4456
#   , uplim_Value_4108 <  1.05 * ref$Value_4108
#   , uplim_Value_4150 <  1.05 * ref$Value_4150
#   , uplim_Value_4157 <  1.05 * ref$Value_4157
#   , uplim_Value_4158 <  1.05 * ref$Value_4158
#   , uplim_Value_4164 <  1.05 * ref$Value_4164
#   , uplim_Value_4176 <  1.05 * ref$Value_4176
#   , uplim_Value_4182 <  1.05 * ref$Value_4182
#   , uplim_Value_4185 <  1.05 * ref$Value_4185
#   , uplim_Value_4187 <  1.05 * ref$Value_4187
#   , uplim_Value_4188 <  1.05 * ref$Value_4188
#   , uplim_Value_4190 <  1.05 * ref$Value_4190
#   , uplim_Value_4192 <  1.05 * ref$Value_4192
#   , uplim_Value_4509 <  1.05 * ref$Value_4509
#   , uplim_Value_4196 <  1.05 * ref$Value_4196
#   , uplim_Value_4253 <  1.05 * ref$Value_4253
#   , uplim_Value_4254 <  1.05 * ref$Value_4254
#   , uplim_Value_4255 <  1.05 * ref$Value_4255
#   , uplim_Value_4256 <  1.05 * ref$Value_4256
#   , uplim_Value_4257 <  1.05 * ref$Value_4257
#   , uplim_Value_4263 <  1.05 * ref$Value_4263
#   , uplim_Value_4271 <  1.05 * ref$Value_4271
#   , uplim_Value_4273 <  1.05 * ref$Value_4273
#   , uplim_Value_4275 <  1.05 * ref$Value_4275
#   , uplim_Value_4300 <  1.05 * ref$Value_4300
#   , uplim_Value_4305 <  1.05 * ref$Value_4305
#   , uplim_Value_4311 <  1.05 * ref$Value_4311
#   , uplim_Value_4313 <  1.05 * ref$Value_4313
#   , uplim_Value_4317 <  1.05 * ref$Value_4317
#   , uplim_Value_4319 <  1.05 * ref$Value_4319
#   , uplim_Value_4323 <  1.05 * ref$Value_4323
#   , uplim_Value_4324 <  1.05 * ref$Value_4324
#   , uplim_Value_4325 <  1.05 * ref$Value_4325
#   , uplim_Value_4327 <  1.05 * ref$Value_4327
#   , uplim_Value_4328 <  1.05 * ref$Value_4328
#   , uplim_Value_4330 <  1.05 * ref$Value_4330
#   , uplim_Value_4331 <  1.05 * ref$Value_4331
#   , uplim_Value_4445 <  1.05 * ref$Value_4445
#   , uplim_Value_4446 <  1.05 * ref$Value_4446
#   , uplim_Value_4448 <  1.05 * ref$Value_4448
#   , uplim_Value_4450 <  1.05 * ref$Value_4450
#   , uplim_Value_4455 <  1.05 * ref$Value_4455
#   , uplim_Value_4457 <  1.05 * ref$Value_4457
#   , uplim_Value_4458 <  1.05 * ref$Value_4458
#   , uplim_Value_4459 <  1.05 * ref$Value_4459
#   , uplim_Value_4462 <  1.05 * ref$Value_4462
#   , uplim_Value_4463 <  1.05 * ref$Value_4463
#   , uplim_Value_4464 <  1.05 * ref$Value_4464
#   , uplim_Value_4466 <  1.05 * ref$Value_4466
#   , uplim_Value_4467 <  1.05 * ref$Value_4467
#   , uplim_Value_4468 <  1.05 * ref$Value_4468
#   , uplim_Value_4470 <  1.05 * ref$Value_4470
#   , uplim_Value_4471 <  1.05 * ref$Value_4471
#   , uplim_Value_4514 <  1.05 * ref$Value_4514
#   , uplim_Value_4527 <  1.05 * ref$Value_4527
#   , uplim_Value_4531 <  1.05 * ref$Value_4531
#   , uplim_Value_4532 <  1.05 * ref$Value_4532
#   , uplim_Value_4538 <  1.05 * ref$Value_4538
#   , uplim_Value_4540 <  1.05 * ref$Value_4540
#   , uplim_Value_4550 <  1.05 * ref$Value_4550
#   , uplim_Value_4551 <  1.05 * ref$Value_4551
#   , uplim_Value_4552 <  1.05 * ref$Value_4552
#   , uplim_Value_4553 <  1.05 * ref$Value_4553
#   , uplim_Value_4554 <  1.05 * ref$Value_4554
#   , uplim_Value_4555 <  1.05 * ref$Value_4555
#   , uplim_Value_4556 <  1.05 * ref$Value_4556)

# Creating an ID column to help error location
df_wide_imputed$ID <- paste(row.names(df_wide_imputed), df_wide_imputed$geographicAreaM49, df_wide_imputed$timePointYears, sep = "_")


# prepare a list of dataframes based on geographicAreaM49
list_df <- split(df_wide_imputed, df_wide_imputed$geographicAreaM49)
confrontation <- function(df)  validate::confront(df, v, key = "ID")
val <- lapply(list_df, confrontation)


# Plotting summaries by country
Path = "~./github/Aquastat/modules/faoswsAquastatImputation/output/Figs/validation_plots.pdf"
pdf(file=Path, height = 16, width = 11) 
plot_set <- function(cf) barplot(val[[cf]], main = names(val)[[cf]])
system.time(ggy <- lapply(1:length(val), plot_set)) 
dev.off() 


# Detecting violations by country
detect_violations <- function(cf) {
d <- cf %>%  as.data.frame() %>%  tbl_df() %>%
filter(value == FALSE) %>%
separate(ID, c("row", "gegraphicAreaM49", "timePointYears"), sep = "_") %>%
mutate(row = as.integer(row), timePointYears = as.integer(timePointYears))
as.vector(unique(d$row))
}

violations <- lapply(val, detect_violations)
violations

aquastat_indicators
# indicators
# original <- dt_wide_value %>% select("geographicAreaM49", "timePointYears", paste0("Value_", aquastat_indicators))
imputated <- df_wide_imputed                                                      

# Indicators BEFORE imputation
dt_wide_value$ID <- paste(row.names(dt_wide_value), dt_wide_value$geographicAreaM49, dt_wide_value$timePointYears, sep = "_")
calc1 <- as.data.frame(confront(dt_wide_value, i, key = "ID"))
measures1 <- as.data.frame(i)
final1 <- merge(calc1, measures1) %>%  tbl_df() %>% rename(Original_Value = value) %>%  select(ID:label)

# Indicators AFTER imputation
calc0 <- as.data.frame(confront(df_wide_imputed, i, key = "ID"))
measures0 <- as.data.frame(i)
final0 <- merge(calc0, measures0) %>%  tbl_df() %>% rename(Imputed_Value = value) %>%  select(ID:label)

# Merging datas
merged_df <- 
  left_join(final1, final0, by = c("ID", "expression", "label")) %>% 
  separate(ID, c("row", "geographicAreaM49", "timePointYears"), sep = "_") %>%
  select(geographicAreaM49, timePointYears, Original_Value, Imputed_Value, label, expression)
