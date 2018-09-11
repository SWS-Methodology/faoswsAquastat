library(validate)
library(errorlocate)
library(dcmodify)
library(tictoc)
library(tidyverse)
library(data.table)

# Load imputed data
imputed_data <- read_csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/Aquastat_Imputed_directly.csv")

# load raw data
d <- readr::read_csv("~./github/Aquastat/test_old_system.csv") 

# # all elements from calculated rules
allelements <- c("4103", "4101", "4102", "4105", "4104", "4106", "4107", "4100",
                 "4456", "4160", "4162", "4168", "4170", "4108", "4109", "4110",
                 "4150", "4155", "4157", "4154", "4156", "4158", "4164", "4176",
                 "4174", "4182", "4452", "4185", "4187", "4188", "4190", "4192",
                 "4509", "4193", "4194", "4196", "4195", "4253", "4251", "4252",
                 "4250", "4254", "4255", "4256", "4257", "4263", "4264", "4265",
                 "4451", "4271", "4260", "4273", "4275", "4300", "4303", "4304",
                 "4305", "4311", "4308", "4309", "4310", "4313", "4312", "4316",
                 "4317", "4314", "4315", "4319", "4323", "4320", "4324", "4321",
                 "4325", "4322", "4327", "4326", "4328", "4318", "4330", "4307",
                 "4331", "4445", "4400", "4446", "4448", "4450", "4455", "4454",
                 "4457", "4458", "4112", "4459", "4462", "4379", "4461", "4463",
                 "4464", "4466", "4465", "4467", "4468", "4470", "4471", "4197",
                 "4514", "4513", "4527", "4526", "4531", "4532", "4538", "4449",
                 "4540", "4539", "4550", "4549", "4551", "4552", "4553", "4554",
                 "4548", "4555", "4546", "4547", "4556", "4557")

# getting indicator codes
lhs <- c("4103", "4105", "4107", "4456", "4108", "4150", "4157", "4158",
         "4164", "4176", "4182", "4185", "4187", "4188", "4190", "4192",
         "4509", "4196", "4253", "4254", "4255", "4256", "4257", "4263",
         "4271", "4273", "4275", "4300", "4305", "4311", "4313", "4317",
         "4319", "4323", "4324", "4325", "4327", "4328", "4330", "4331",
         "4445", "4446", "4448", "4450", "4455", "4457", "4458", "4459",
         "4462", "4463", "4464", "4466", "4467", "4468", "4470", "4471",
         "4514", "4527", "4531", "4532", "4538", "4540", "4550", "4551",
         "4552", "4553", "4554", "4555", "4556")


# getting components
rhs <- setdiff(allelements, lhs)

aquastatElement <- c(4100L, 4101L, 4102L, 4103L, 4104L, 4105L, 4106L, 4107L, 4108L, 
                     4109L, 4110L, 4111L, 4112L, 4113L, 4114L, 4115L, 4116L, 4150L, 
                     4151L, 4152L, 4153L, 4154L, 4155L, 4156L, 4157L, 4158L, 4159L, 
                     4160L, 4161L, 4162L, 4164L, 4165L, 4166L, 4167L, 4168L, 4169L, 
                     4170L, 4171L, 4172L, 4173L, 4174L, 4175L, 4176L, 4177L, 4178L, 
                     4182L, 4183L, 4184L, 4185L, 4186L, 4187L, 4188L, 4189L, 4190L, 
                     4191L, 4192L, 4193L, 4194L, 4195L, 4196L, 4197L, 4250L, 4251L, 
                     4252L, 4253L, 4254L, 4255L, 4256L, 4257L, 4260L, 4261L, 4262L, 
                     4263L, 4264L, 4265L, 4266L, 4267L, 4268L, 4269L, 4270L, 4271L, 
                     4273L, 4275L, 4300L, 4303L, 4304L, 4305L, 4306L, 4307L, 4308L, 
                     4309L, 4310L, 4311L, 4312L, 4313L, 4314L, 4315L, 4316L, 4317L, 
                     4318L, 4319L, 4320L, 4321L, 4322L, 4323L, 4324L, 4325L, 4326L, 
                     4327L, 4328L, 4329L, 4330L, 4331L, 4332L, 4333L, 4334L, 4335L, 
                     4336L, 4337L, 4338L, 4339L, 4340L, 4341L, 4342L, 4343L, 4344L, 
                     4345L, 4379L, 4400L, 4401L, 4402L, 4403L, 4410L, 4445L, 4446L, 
                     4448L, 4449L, 4450L, 4451L, 4452L, 4454L, 4455L, 4456L, 4457L, 
                     4458L, 4459L, 4461L, 4462L, 4463L, 4464L, 4465L, 4466L, 4467L, 
                     4468L, 4470L, 4471L, 4509L, 4513L, 4514L, 4527L, 4531L, 4532L, 
                     4533L, 4534L, 4535L, 4536L, 4538L, 4539L, 4540L, 4546L, 4547L, 
                     4548L, 4549L, 4550L, 4551L, 4552L, 4553L, 4554L, 4555L, 4556L, 
                     4557L)


definitions <- c("Total area of the country", "Arable land area", "Permanent crops area", 
                 "Cultivated area (arable land + permanent crops)", "Total population", 
                 "Rural population", "Urban population", "Population density", 
                 "Population economically active in agriculture", "Male population economically active in agriculture", 
                 "Female population economically active in agriculture", "Human Development Index (HDI) [highest = 1]", 
                 "Gross Domestic Product (GDP)", "Agriculture, value added (% GDP)", 
                 "Total population with access to improved drinking-water source (JMP)", 
                 "Rural population with access to improved drinking-water source (JMP)", 
                 "Urban population with access to improved drinking-water source (JMP)", 
                 "Long-term average annual precipitation in volume", "Long-term average annual precipitation in depth", 
                 "Evaporation from artificial lakes and reservoirs", "Water resources produced internally in a 10th dry year frequency", 
                 "Groundwater produced internally", "Surface water produced internally", 
                 "Overlap between surface water and groundwater", "Total internal renewable water resources (IRWR)", 
                 "Total internal renewable water resources per capita", "Surface water: entering the country (total)", 
                 "Surface water: inflow not submitted to treaties", "Surface water: inflow submitted to treaties", 
                 "Surface water: inflow secured through treaties", "Surface water: accounted inflow", 
                 "Surface water: total flow of border rivers", "Surface water: total flow of border rivers (actual)", 
                 "Surface water: accounted flow of border rivers (natural)", "Surface water: accounted flow of border rivers", 
                 "Surface water: accounted part of border lakes (natural)", "Surface water: accounted part of border lakes (actual)", 
                 "Surface water: leaving the country to other countries (total)", 
                 "Surface water: outflow to other countries not submitted to treaties", 
                 "Surface water: outflow to other countries submitted to treaties", 
                 "Surface water: outflow to other countries secured through treaties", 
                 "Surface water: total entering and bordering the country (natural)", 
                 "Surface water: total external renewable", "Groundwater: entering the country (total)", 
                 "Groundwater: leaving the country to other countries (total)", 
                 "Water resources: total external renewable", "Water resources: total external renewable (natural)", 
                 "Total renewable surface water (natural)", "Total renewable surface water", 
                 "Total renewable groundwater (natural)", "Total renewable groundwater", 
                 "Total renewable water resources", "Total renewable water resources (natural)", 
                 "Total renewable water resources per capita", "Total renewable water resources per capita (natural)", 
                 "Dependency ratio", "Exploitable: regular renewable surface water", 
                 "Exploitable: irregular renewable surface water", "Exploitable: regular renewable groundwater", 
                 "Total exploitable water resources", "Total dam capacity", "Agricultural water withdrawal", 
                 "Municipal water withdrawal", "Industrial water withdrawal", 
                 "Total water withdrawal", "Agricultural water withdrawal as % of total water withdrawal", 
                 "Municipal water withdrawal as % of total withdrawal", "Industrial water withdrawal as % of total water withdrawal", 
                 "Total water withdrawal per capita", "Irrigation water requirement", 
                 "Fresh surface water withdrawal (primary and secondary)", "Fresh groundwater withdrawal (primary and secondary)", 
                 "Total freshwater withdrawal (primary and secondary)", "Desalinated water produced", 
                 "Direct use of treated municipal wastewater", "Depletion rate of renewable groundwater resources", 
                 "Abstraction of fossil groundwater", "Expected time that fossil groundwater will last", 
                 "Produced municipal wastewater", "Treated municipal wastewater", 
                 "Agricultural water requirement as % of agricultural water withdrawal", 
                 "Agricultural water withdrawal as % of total renewable water resources", 
                 "MDG 7.5. Freshwater withdrawal as % of total renewable water resources", 
                 "Total cultivated area drained", "Area equipped for irrigation drained", 
                 "Non-irrigated cultivated area drained", "% of total cultivated area drained", 
                 "Water harvesting area", "Irrigation potential", "Area equipped for full control irrigation: surface irrigation", 
                 "Area equipped for full control irrigation: sprinkler irrigation", 
                 "Area equipped for full control irrigation: localized irrigation", 
                 "Area equipped for full control irrigation: total", "Area equipped for irrigation: equipped lowland areas", 
                 "Area equipped for irrigation: total", "Flood recession cropping area non-equipped", 
                 "Cultivated wetlands and inland valley bottoms non-equipped", 
                 "Area equipped for irrigation: spate irrigation", "Total agricultural water managed area", 
                 "Area equipped for irrigation: actually irrigated", "% of agricultural water managed area equipped for irrigation", 
                 "Area equipped for irrigation by groundwater", "Area equipped for irrigation by surface water", 
                 "Area equipped for irrigation by mixed surface water and groundwater", 
                 "% of area equipped for irrigation by groundwater", "% of area equipped for irrigation by surface water", 
                 "% of area equipped for irrigation by mixed surface water and groundwater", 
                 "Area equipped for power irrigation (surface water or groundwater)", 
                 "% of area equipped for irrigation power irrigated", "% of the area equipped for irrigation actually irrigated", 
                 "% of total grain production irrigated", "% of irrigation potential equipped for irrigation", 
                 "% of the cultivated area equipped for irrigation", "Total area of small irrigation schemes", 
                 "Total area of medium irrigation schemes", "Total area of large irrigation schemes", 
                 "Average cost of irrigation development in public schemes", "Average cost of operation and maintenance in public schemes", 
                 "Average cost of drainage development in public schemes", "Average cost of irrigation rehabilitation in public schemes", 
                 "Average cost of irrigation development in private schemes", 
                 "Average cost of operation and maintenance cost in private schemes", 
                 "Average cost of drainage development in private schemes", "Average cost of irrigation rehabilitation in private schemes", 
                 "Average cost of installation of sprinkler irrigation: on farm", 
                 "Average cost of installation of localized irrigation: on farm", 
                 "Harvested irrigated temporary crop area: Wheat", "Total harvested irrigated crop area (full control irrigation)", 
                 "Area salinized by irrigation", "Area waterlogged by irrigation", 
                 "Area waterlogged not irrigated", "Population affected by water related disease", 
                 "Irrigated crop yield: Wheat", "% of area equipped for irrigation salinized", 
                 "% of area equipped for irrigation drained", "Other agricultural water managed area", 
                 "Population economically active", "Freshwater withdrawal as % of internal renewable water resources", 
                 "Direct use of agricultural drainage water", "Groundwater: accounted inflow", 
                 "Conservation agriculture area", "Conservation agriculture area as % of arable land area", 
                 "Not defined1", "Municipal water withdrawal per capita (total population)", 
                 "GDP per Capita", "Area equipped for full control irrigation: pressurized (sprinkler + localized)", 
                 "Area equipped for full control irrigation: actually irrigated", 
                 "Harvested irrigated crop area as % of the full control irrigation area actually irrigated", 
                 "% of area equipped for full control irrigation actually irrigated", 
                 "Irrigated cropping intensity", "Area equipped by direct use of treated municipal wastewater", 
                 "% of area equipped by direct use of treated municipal wastewater", 
                 "Freshwater withdrawal as % of total water withdrawal", "Municipal water withdrawal per capita (urban population)", 
                 "% of total country area cultivated", "Dam capacity per capita", 
                 "Exploitable: total renewable surface water", "Area equipped for irrigation by direct use of non-treated municipal wastewater", 
                 "% of area equipped for irrigation by direct use of non-treated municipal wastewater", 
                 "% of area equipped for irrigation by direct use of agricultural drainage water", 
                 "Industrial water withdrawal per capita", "Agricultural water withdrawal per capita", 
                 "Area equipped by direct use of treated municipal wastewater", 
                 "Area equipped by direct use of not treated municipal wastewater", 
                 "Direct use of treated municipal wastewater", "Overlap: between surface water and groundwater", 
                 "% of economically active population active in agriculture", 
                 "Area equipped for irrigation by desalinated water", "% of area equipped for irrigation by desalinated water", 
                 "Not defined2", "Not defined3", "Not defined4", "Not defined5", 
                 "Not defined6", "Not defined7", "Not defined8", "Not defined9", 
                 "Not defined10", "Not defined11", "Not defined12", "Not defined13"
)

# set a indicator definition dataframe to be use later
df_definitions <- data.frame(aquastatElement = aquastatElement, definitions = definitions)

# Data pre-processing
imputed_data_proc <- 
  imputed_data %>%
  # getting all the element codes that are not indicator codes
  filter(!(aquastatElement %in% lhs)) %>% 
  select(geographicAreaM49, timePointYears, aquastatElement, Value) %>% 
  mutate(aquastatElement = paste0("Value_", aquastatElement)) 

    
# wide format 
imputed_wide <- dcast(imputed_data_proc , geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value"))


# Calculation using mutate
calc <-
  tbl_df(imputed_wide) %>%
   mutate(Value_4103 = Value_4101+Value_4102
          ,Value_4105 = Value_4104-Value_4106
          ,Value_4107 = Value_4104/(Value_4100/100)
          ,Value_4108 = Value_4109+Value_4110
          ,Value_4150 = Value_4100*Value_4155/100000
          ,Value_4157 = Value_4154+Value_4155-Value_4156
          ,Value_4158 = Value_4157*1000000/Value_4104
          ,Value_4164 = Value_4160+Value_4162+Value_4168
          ,Value_4176 = Value_4160+Value_4162+Value_4168-Value_4174
          ,Value_4182 = Value_4176+Value_4452
          ,Value_4185 = Value_4176+Value_4155
          ,Value_4187 = Value_4154+Value_4452
          ,Value_4188 = Value_4185+Value_4187-Value_4156
          ,Value_4190 = Value_4188*1000000/Value_4104
          ,Value_4192 = 100*(Value_4164+Value_4452)/(Value_4164+Value_4452+Value_4157)
          ,Value_4509 = Value_4193+Value_4194
          ,Value_4196 = Value_4509+Value_4195
          ,Value_4253 = Value_4251+Value_4252+Value_4250
          ,Value_4254 = Value_4250/Value_4253*100
          ,Value_4255 = Value_4251/Value_4253*100
          ,Value_4256 = Value_4252/Value_4253*100
          ,Value_4257 = Value_4253*1000000/Value_4104
          ,Value_4263 = Value_4253-Value_4264-Value_4265-Value_4451
          ,Value_4271 = 100*Value_4260/Value_4250
          ,Value_4273 = 100*Value_4250/Value_4188
          ,Value_4275 = 100*Value_4263/Value_4188
          ,Value_4300 = Value_4303+Value_4304
          ,Value_4305 = 100*Value_4300/Value_4103
          ,Value_4311 = Value_4308+Value_4309+Value_4310
          ,Value_4313 = Value_4311+Value_4312+Value_4316
          ,Value_4317 = Value_4313+Value_4314+Value_4315
          ,Value_4319 = 100*Value_4313/Value_4317
          ,Value_4323 = 100*Value_4320/Value_4313
          ,Value_4324 = 100*Value_4321/Value_4313
          ,Value_4325 = 100*Value_4322/Value_4313
          ,Value_4327 = 100*Value_4326/Value_4313
          ,Value_4328 = 100*Value_4318/Value_4313
          ,Value_4330 = 100*Value_4313/Value_4307
          ,Value_4331 = 100*Value_4313/Value_4103
          ,Value_4445 = 100*Value_4400/Value_4313
          ,Value_4446 = 100*Value_4303/Value_4313
          ,Value_4448 = Value_4314+Value_4315
          ,Value_4450 = 100*Value_4263/Value_4157
          ,Value_4455 = 100*Value_4454/Value_4101
          # ,Value_4456 = Value_4160+Value_4162+Value_4168+Value_4170
          ,Value_4457 = Value_4251*1000000/Value_4104
          ,Value_4458 = Value_4112/Value_4104/1000
          ,Value_4459 = Value_4309+Value_4310
          ,Value_4462 = 100*Value_4379/Value_4461
          ,Value_4463 = 100*Value_4461/Value_4311
          ,Value_4464 = 100*Value_4379/Value_4461
          ,Value_4466 = 100*Value_4465/Value_4313
          ,Value_4467 = 100*Value_4263/Value_4253
          ,Value_4468 = Value_4251*1000000/Value_4106
          ,Value_4470 = 100*Value_4103/Value_4100
          ,Value_4471 = 1000000*Value_4197/Value_4104
          ,Value_4514 = 100*Value_4513/Value_4313
          # ,Value_4527 = 100*Value_4526/Value_4313
          ,Value_4531 = Value_4252*1000000/Value_4104
          ,Value_4532 = Value_4250*1000000/Value_4104
          ,Value_4538 = 100*Value_4108/Value_4449
          # ,Value_4540 = 100*Value_4539/Value_4313
          ,Value_4556 = 100*Value_4379/Value_4101
          ,Value_4555 = 1/(1+((1-(Value_4556/100))/((Value_4556/100)*Value_4557)))
          ,Value_4554 = (Value_4547/Value_4251)/1000000000
          ,Value_4553 = (Value_4546/Value_4252)/1000000000
          ,Value_4552 = ((Value_4548*Value_4555/100)/Value_4250)/1000000000
          ,Value_4551 = (Value_4552*Value_4254)+(Value_4553*Value_4256)+(Value_4554*Value_4255)
          ,Value_4550 = 100*Value_4263/(Value_4188-Value_4549))
          
          
# getting the long format        
# Now we have indicators calculated from imputed components
long_imputed <- gather(calc, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")) %>% 
  mutate(aquastatElement = as.integer(substring(aquastatElement, 7))) %>% 
  left_join(df_definitions, by = c("aquastatElement"))
  
dtt <-
  tbl_df(d) %>%
  select(geographicAreaM49, timePointYears, aquastatElement, Value)
  group_by(geographicAreaM49, aquastatElement) %>%
  complete(timePointYears = as.integer(seq(min(timePointYears), max(timePointYears)))) %>%
  ungroup()
long_values <- left_join(select(tbl_df(dtt), -Value), long_values, by = c("geographicAreaM49", "aquastatElement", "timePointYears"))
       

# Save this data to GDS
write.csv(long_imputed, "~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/CALC_FROM_COMPONENTS_AQUASTAT.csv", row.names = FALSE)


indirect <- read_csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/CALC_FROM_COMPONENTS_AQUASTAT.csv")
direct <- read_csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/aquastat_test_imputed_all_TSEXP.csv")


df_merged <- left_join(direct %>% rename(DirImpValue = Value),
          indirect %>% rename(IndImpValue = Value))

write.csv(df_merged , "~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/IND_AND_DIR_COMPUTATION.csv", row.names = FALSE)
