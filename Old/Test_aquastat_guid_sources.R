library(devtools)
install_github("ropenscilabs/tabulizerjars")
install_github("ropenscilabs/tabulizer")

library(tabulizer)
tabs <- extract_tables("~./github/Aquastat/AQUASTAT-Guidelines.pdf")


aqua_sources0 <- list(tabs[[8]],tabs[[9]],tabs[[10]],tabs[[11]],tabs[[12]])
aqua_sources1 <- do.call("rbind", aqua_sources0)[-c(1,2), c(2, 4, 5, 6, 7)]
colnames(aqua_sources1) <- c("Category", "aquastatElement.ElementName", "Diss", "Calculated", "Source")
aqua_sources1 <- as.data.frame(aqua_sources1)
aqua_sources1$aquastatElement.ElementName <- as.character(aqua_sources1$aquastatElement.ElementName)

df_aqua_guide <- 
  aqua_sources1 %>%
  separate(aquastatElement.ElementName , c("aquastatElement", "ElementName"), ". ") %>% 
  select("aquastatElement", "Diss", "Calculated", "Source")


# write.csv(df_aqua_guide, "~./github/Aquastat/aquastat_ref_table.csv", row.names = FALSE)
indicators <- filter(df_aqua_guide, Source == "C" )
internal_s_variables <- filter(df_aqua_guide, Source == "A")
external_s_variables <- filter(df_aqua_guide, Source == "O")


indicators %>% group_by(Diss) %>% summarise(D = n())
internal_s_variables %>% group_by(Diss) %>% summarise(D = n())
external_s_variables %>% group_by(Diss) %>% summarise(D = n())


df_aqua_guide %>% group_by(Diss) %>% summarise(D = n())
definitions <- read_csv("~./github/Aquastat/Aquastat_Definitions.csv")


setdiff(df_aqua_guide$aquastatElement, definitions$Element)
intersect(df_aqua_guide$aquastatElement, definitions$Element)

setdiff(df_aqua_guide$aquastatElement, definitions$Element)
intersect(df_aqua_guide$aquastatElement, definitions$Element)
