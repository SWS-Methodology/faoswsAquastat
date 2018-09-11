library(validate)
library(dcmodify)
library(errorlocate)
library(tidyverse)

# read in data output from faoswsAquastatImputation
df_imputed <- read.csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aquastat_test_imputed.csv")
df_baseline <- read.csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aqua_basedataset.csv")

d <- readr::read_csv("~./github/Aquastat/test_old_system.csv") 
df_definitions <- read.csv("~./github/Aquastat/Aquastat_Definitions.csv")
df_annex1 <- read_csv("~./github/Aquastat/Annex1.csv")


# left_join(df_definitions, as.data.frame(aquastat_indicators), by = c("Element" = "aquastat_indicators"))

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

rules_guidelines <- c("[4103] = [4101]+[4102]" 
,"[4105] = [4104]-[4106]" 
,"[4107] = [4104]/([4100]/100) "
,"[4108] = [4109]+[4110]"
,"[4150] = [4100]*[4151]/100000"
,"[4157] = [4154]+[4155]-[4156]"
,"[4158] = [4157]*1000000/[4104]"
, "[4157 [4164] = [4160]+[4162]+[4168]"
,"[4176] = [4160]+[4162]+[4168]-[4174]"
,"[4182] = [4176]+[4452]"
,"[4185] = [4176]+[4155]"
,"[4187] = [4154]+[4452]"
,"[4188] = [4185]+[4187]-[4156]"
,"[4190] = [4188]*1000000/[4104]"
,"[4192] = 100*([4164]+[4452])/([4164]+[4452]+[4157])"
,"[4196] = [4509]+[4195]"
,"[4253] = [4251]+[4252]+[4250]"
,"[4254] = [4250]/[4253]*100"
,"[4255] = [4251]/[4253]*100"
,"[4256] = [4252]/[4253]*100"
,"[4257] = [4253]*1000000/[4104]"
,"[4263] = [4253]-[4264]-[4265]-[4451]"
,"[4271] = 100*[4260]/[4250]"
,"[4273] = 100*[4250]/[4188]"
,"[4275] = 100*[4263]/[4188]"
,"[4300] = [4303]+[4304]"
,"[4305] = 100*[4300]/[4103] "
,"[4311] = [4308]+[4309]+[4310]"
,"[4313] = [4311]+[4312]+[4316]"
,"[4317] = [4313]+[4314]+[4315]"
,"[4319] = 100*[4313]/[4317]"
,"[4323] = 100*[4320]/[4313]"
,"[4324] = 100*[4321]/[4313]"
,"[4325] = 100*[4322]/[4313]"
,"[4327] = 100*[4326]/[4313]"
,"[4328] = 100*[4318]/[4313]"
,"[4330] = 100*[4313]/[4307]"
,"[4331] = 100*[4313]/[4103]"
,"[4445] = 100*[4400]/[4313]"
,"[4446] = 100*[4303]/[4313]"
,"[4448] = [4314]+[4315]"
,"[4450] = 100*[4263]/[4157]"
,"[4455] = 100*[4454]/[4101]"
,"[4456] = [4160]+[4162]+[4168]+[4170]"
,"[4457] = [4251]*1000000/[4104]"
,"[4458] = [4112]/[4104]/1000"
,"[4459] = [4309]+[4310]"
,"[4462] = 100*[4379]/[4461]"
,"[4463] = 100*[4461]/[4311]"
,"[4464] = 100*[4379]/[4461]"
,"[4466] = 100*[4465]/[4313]"
,"[4467] = 100*[4263]/[4253]"
,"[4468] = [4251]*1000000/[4106]"
,"[4470] = 100*[4103]/[4100]"
,"[4471] = 1000000*[4197]/[4104]"
, "[4509] = [4193]+[4194]"
,"[4514] = 100*[4513]/[4313]"
,"[4527] = 100*[4526]/[4313]"
,"[4531] = [4252]*1000000/[4104]"
,"[4532] = [4250]*1000000/[4104]"
,"[4533] = [4465]"
,"[4534] = [4513]"
,"[4535] = [4265]"
,"[4536] = [4156]"
,"[4538] = 100*[4108]/[4449]"
,"[4540] = 100*[4539]/[4313]")

# Replace brackets in rules to make them valid R code
proc_rules_guidelines <- str_replace_all(rules_guidelines, "\\[([0-9]+)\\]", "Value_\\1")

# Extract all the elements that are used in rules
rule_elements_ <- unique(unlist(str_extract_all(rules, regex("(?<=\\[)[0-9]+(?=\\])"))))

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


# Calculation rules dataframe
df_ind <- data.frame(rule = substring(proc_rules, 12), label = paste0("i", aquastat_indicators))
i <- indicator(.data=df_ind)



# Indicators BEFORE imputation
df_baseline$ID <- paste(row.names(df_baseline), df_baseline$geographicAreaM49, df_baseline$timePointYears, sep = "_")
before_ind <- confront(df_baseline, i, key = "ID")

# merging calculation and metadata
before_df <- as.data.frame(before_ind) %>% separate(ID, c("Row", "geographicAreaM49", "timePointYears"), sep = "_")
origin(i) <- "raw data"
measures1 <- as.data.frame(i)

before_final <- merge(before_df, measures1) %>%  tbl_df() %>% rename(Original_Value = value) %>%  select(geographicAreaM49, timePointYears, Original_Value,  label,  rule)


# Indicators AFTER imputation
df_wide_imputed$ID <- paste(row.names(df_wide_imputed), df_wide_imputed$geographicAreaM49, df_wide_imputed$timePointYears, sep = "_")
df_wide_imputed[is.na(df_wide_imputed)] <- 0
after_ind <- confront(df_wide_imputed, i, key = "ID")

# merging calculation and metadata
after_df <- as.data.frame(after_ind) %>% separate(ID, c("Row", "geographicAreaM49", "timePointYears"), sep = "_")
origin(i) <- "imputed data"
measures1 <- as.data.frame(i)

after_final <- merge(after_df, measures1) %>%  tbl_df() %>% rename(Imputed_Value = value) %>%  select(geographicAreaM49, timePointYears, Imputed_Value, label,  rule)


# Merging datasest
merged_df <- 
  left_join(before_final, after_final) %>% 
  select(geographicAreaM49, timePointYears, Original_Value, Imputed_Value, label, rule)

# save dataset for report

unique(merged_df$label)
