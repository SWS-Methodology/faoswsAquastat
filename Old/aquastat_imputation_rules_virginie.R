##' faoswsAquastatImputation exercise
##' Author: Francy Lisboa
##' Date: 01/08/2018
##' Purpose: this module perfoms the imputation of Aquastat elements using a split-apply-combine approach
##' the elements in the calculation rules are selected from the the aquastat raw data queried from SWS.
##' At the country level, the filtered dataset is first divided into two subdatasets: the one
##' with variables  with all missing values - totally empity time-series -; and one with variables 
##' with at least one observed values. The datasets with elements with at least one observation are drilled down according to
##' the type of imputation.
##'  For example: dataset with elements with no missing values - No need of imputation;
##'               dataset with elements with zero variance and at least one missing value - Least Observation Carried Forward;
##'               dataset with elements with nonzero variance and at least one missing value - Linear Interpolation
##' This approach is believed to be exaustive in terms of element coverage and at the end four country-level datasets are combined
##' in order to have a consolidated imputed dataset with four columns - geographicAreaM49, timePointYears, aquastatElement, Value.
##' Flags will be processed at the end.
##' 
##' Observations:
##' 1 - The dataset used for this exercise is from the current Aquastat working System because at the moment - 01/08/2018 -
##' the dataset migrated into SWS contains only two countries Algeria and Australia. A total of 43 countries is used to test the imputation procedure.
##' 2 - It has been noticed that some of elements present in the calculated rules are NOT in the elemnts of the dataset used for the exercise
##' Here I am assuming that the data team wants  to focus on the elements in the calculated rules.
##' 3 - There are few elements in the calculation rules that are undefined - 4456L, 4550L, 4551L, 4552L, 4553L, 4554L, 4555L, 4556L, 4557L.
##' For the sake of the exercise I have called them as "not defenied". but it needs to be clarified with the data team.
##' 4 - At this exercise, I did not expand the element time-series before the data processing, thus the time-series lenght of the output is the same as
##' in the imput; however it can be easily changed according to the data team demand.
##' 

# Loading libraries
supressMessages({ 
library(faosws)
library(faoswsUtil)
library(faoswsModules)
library(faoswsFlag)
library(tidyverse)
library(data.table)
library(imputeTS)
})

# Get the shared path and read the session info (working locally)
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/animal_stockFRANCESCA/sws.yml")
  
  ## If you're not on the system, your settings will overwrite any others
  R_SWS_SHARE_PATH = SETTINGS[["share"]]
  
  ## Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])
  
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
  
}


# Getting rules 
rules_guidelines <- c("[4103]=[4101] + [4102]",
           "[4105]=[4104] - [4106]",
           "[4107]=[4104]/([4100]/100)",
           "[4456]=[4160]+[4162]+[4168]+[4170]",
           "[4108]=[4109]+[4110]",
           "[4150]=[4100] * [4155] / 100000",
           "[4157]=[4154]+[4155]-[4156]", 
           "[4158]=[4157]*1000000/[4104]", 
           "[4164]=[4160]+[4162]+[4168]",
           "[4176]=[4160]+[4162]+[4168]-[4174]", 
           "[4182]=[4176]+[4452]", "[4185]=[4176]+[4155]",
           "[4187]=[4154]+[4452]", "[4188]=[4185]+[4187]-[4156]",
           "[4190]=[4188]*1000000/[4104]", 
           "[4192]=100*([4164]+[4452])/([4164]+[4452]+[4157])",
           "[4509]=[4193]+[4194]" , "[4196]=[4509]+[4195]",
           "[4253]=[4251]+[4252]+[4250]", "[4254]=[4250]/[4253]*100", 
           "[4255]=[4251]/[4253]*100", "[4256]=[4252]/[4253]*100", 
           "[4257]=[4253]*1000000/[4104]", 
           "[4263]=[4253]-[4264]-[4265]-[4451]", 
           "[4271]=100*[4260]/[4250]", "[4273]=100*[4250]/[4188]", 
           "[4275]=100*[4263]/[4188]", "[4300]=[4303]+[4304]", 
           "[4305]=100*[4300]/[4103]", "[4311]=[4308]+[4309]+[4310]", 
           "[4313]=	[4311]+[4312]+[4316]", 
           "[4317]=	[4313]+[4314]+[4315]", 
           "[4319]=100*[4313]/[4317]",
           "[4323]=100*[4320]/[4313]", 
           "[4324]=100*[4321]/[4313]", 
           "[4325]=100*[4322]/[4313]", 
           "[4327]=100*[4326]/[4313]", 
           "[4328]=100*[4318]/[4313]", 
           "[4330]=100*[4313]/[4307]", 
           "[4331]=100*[4313]/[4103]", 
           "[4445]=100*[4400]/[4313]", 
           "[4446]=100*[4303]/[4313]", 
           "[4448]=[4314]+[4315]",
           "[4450]=100*[4263]/[4157]",
           "[4455]=100*[4454]/[4101]", 
           "[4457]=[4251]*1000000/[4104]",
           "[4458]=[4112]/[4104]/1000",
           "[4459]=[4309]+[4310]", 
           "[4462]=	100*[4379]/[4461]", 
           "[4463]=100*[4461]/[4311]",
           "[4464]=100*[4379]/[4461]", 
           "[4466]=100*[4465]/[4313]", 
           "[4467]=100*[4263]/[4253]" , 
           "[4468]=[4251]*1000000/[4106]",
           "[4470]=100*[4103]/[4100]", 
           "[4471]=1000000*[4197]/[4104]", 
           "[4514]=100*[4513]/[4313]", 
           "[4527]=100*[4526]/[4313]", 
           "[4531]=	[4252]*1000000/[4104]", 
           "[4532]=[4250]*1000000/[4104]",
           "[4538]=100*[4108]/[4449]", 
           "[4540]=100*[4539]/[4313]", 
           "[4550]=100*[4263]/([4188]-[4549])", 
           "[4551]=([4552]*[4254])+([4553]*[4256])+([4554]*[4255])", 
           "[4552]=	(([4548]*[4555]/100)/[4250])/1000000000", 
           "[4553]=([4546]/[4252])/1000000000", 
           "[4554]=	([4547]/[4251])/1000000000", 
           "[4555]=1/(1+((1-([4556]/100))/(([4556]/100)*[4557])))",
           "[4556]=100*[4379]/[4101]")


# Definitions of indicators (left-hand side)
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

# set a indicator definition dataframe so that it can be used later
df_definitions <- data.frame(aquastatElement = aquastatElement, definitions = definitions)

# Replace brackets in rules to make them valid R code
proc_rules_guide <- str_replace_all(rules_guidelines, "\\[([0-9]+)\\]", "Value_\\1")

# Extract all the elements that are used in rules
rule_elements_guide <- unique(unlist(str_extract_all(rules_guidelines, regex("(?<=\\[)[0-9]+(?=\\])"))))


# pulling raw data from SWS
d <- GetData(swsContext.datasets[[1]])


# select relevant columns
dt <- dplyr::select(d, geographicAreaM49, timePointYears, aquastatElement, Value, flag_aquastat)


# expand time-series of elements so that first and last years are the boundaries
dtt <-
  tbl_df(dt) %>%
  group_by(geographicAreaM49, aquastatElement) %>%
  complete(timePointYears = as.integer(seq(min(timePointYears), max(timePointYears)))) %>%
  ungroup()

# Get wide format dataset (elements as columns)
dt_wide <- dcast(setDT(dtt), geographicAreaM49 + timePointYears ~ aquastatElement, value.var = c("Value", "flag_aquastat"))
dt_wide_value <- dt_wide %>% select(geographicAreaM49, timePointYears, starts_with("Value_"))


# dim(dt_wide)
# Create a  list of dataframes which is the starting point for the subsettings
list_value <- split(as.data.frame(dt_wide_value), dt_wide_value$geographicAreaM49)
list_flag <- split(as.data.frame(dt_wide_flag), dt_wide_flag$geographicAreaM49)

# Breaking the raw data down
# Dataset: all missing value columns ----
AllMissingColumns <- function(df) as.vector(which(colSums(is.na(df)) == nrow(df))) 
dt_wide_allNAs_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, AllMissingColumns(.)) %>%  setDT())


# Dataset: One observation only ----
OneObservationColumns <- function(df) as.vector(which(colSums(!is.na(df)) == 1)) 
dt_wide_OneObs_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, OneObservationColumns(.)) %>%  setDT())


# Dataset: zero variance with no missing values -----
ZeroVarianceCols_noNA <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & colSums(is.na(df)) == 0)) 
dt_wide_zerovar0_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols_noNA(.)) %>%  setDT())


# Dataset: zero variance and at least one NA --------
ZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) == 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar1_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, ZeroVarianceCols(.)) %>%  setDT())

# Dataset: nonzero variance and at least one NA ----
NonZeroVarianceCols <- function(df) as.vector(which(apply(df, 2, var, na.rm = TRUE) != 0 & apply(df, 2, function(x) any(is.na(x))) == TRUE))
dt_wide_zerovar2_values <- lapply(list_value, function(x) tbl_df(x) %>% select(geographicAreaM49, timePointYears, NonZeroVarianceCols(.)) %>%  setDT())
 


# IMPUTATION ACTION ---------
l1_values <- dt_wide_allNAs_values    # NO IMPUTATION
l2_values <- dt_wide_OneObs_values    # REPLACED BY THE UNIQUE OBSERVED VALUE
l3_values <- dt_wide_zerovar0_values  # NO IMPUTATION
l4_values <- dt_wide_zerovar1_values  # REPLACE THE NAS BY THE ONLY OBSERVED VALUE IN THE TIME-SERIES
l5_values <- dt_wide_zerovar2_values  # INTERPOLATION

# locf ----
l2_imputed_values <- lapply(l2_values, function(x) imputeTS::na.locf(x))
l4_imputed_values <- lapply(l4_values, function(x) imputeTS::na.locf(x))

# linear interpolation ----
l5_imputed_values <- lapply(l5_values, function(x) imputeTS::na.interpolation(x))


# RECOMBINATION OF DATASETS -----
l01 <- Map(merge, l1_values, l3_values)
l02 <-  Map(merge, l2_imputed_values, l4_imputed_values) 
l03 <- Map(merge, l01, l02)
l_final <- Map(merge, l03, l5_imputed_values)


# GET LONG FORMAT (Four variables dataset) ----
l_long_all_values <- lapply(l_final, function(x) gather(x, aquastatElement, Value, -c("geographicAreaM49", "timePointYears")))
long_all_values <- do.call("rbind", l_long_all_values) %>% tbl_df() 
row.names(long_all_values) <- NULL
long_all_values <- long_all_values %>% mutate(aquastatElement = as.integer(substring(aquastatElement, 7))) %>% arrange(geographicAreaM49, aquastatElement)


# getting the elements in rules_elements
long_values <- filter(long_all_values, aquastatElement %in% df_definitions$aquastatElement) %>% left_join(df_definitions, by = c("aquastatElement"))
long_values <- left_join(select(tbl_df(dtt), -Value), long_values, by = c("geographicAreaM49", "aquastatElement", "timePointYears")) %>%  select(-flag_aquastat)

data_to_save <- as.data.table(long_values)
data_to_save2 <- data_to_save[!is.na(Value)]


# save data 
SaveData("Aquastat", "aquastat_imputed", long_values, waitTimeout = 5000)
paste0("faoswsAquastatImputation module completed successfully!!!",
       
       stats$inserted,"observations written,",
       
       stats$ignored,"weren't updated,",
       
       stats$discarded,"had problems.")