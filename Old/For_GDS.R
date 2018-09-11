library(tidyverse)

exercise02 <- read_csv("~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/Comparing_approaches_TSEXPAN_ex2_FL.csv")
metrics02 <- 
  exercise02 %>% 
  group_by(geographicAreaM49, aquastatElement) %>% 
  summarise(ts_length = as.numeric(n()),
            count_NA_raw = as.numeric(sum(is.na(Value_raw))),
            count_NA_imp = as.numeric(sum(is.na(Value_imp))),
            num_imp_values =  ifelse(count_NA_imp == 0, count_NA_raw, count_NA_raw),
            share_NA_imp = as.numeric((num_imp_values/ts_length)*100)) %>% 
  ungroup() %>% 
  left_join(exercise02 %>%  select(aquastatElement, definitions) %>%  distinct()) %>% 
  arrange(geographicAreaM49,  aquastatElement, definitions)
  

write.csv(metrics02, "~./github/Aquastat/modules/faoswsAquastatImputation/output/Datasets/GDS/metric02FL20082118.csv", row.names = FALSE)

  
  
  