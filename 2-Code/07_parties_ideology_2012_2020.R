# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: MARCH 2025

# Cleaning my environment  
rm(list = ls())

# Memory management  
gc()

# Required packages ------------------------------------------------------------
library(ggplot2)
library(sf)
library(dplyr)
library(readxl)

# ------------------------------------------------------------------------------
# LOADING THE DATA -------------------------------------------------------------
# ------------------------------------------------------------------------------

# Loading the data: Power and Zucco (2021)
zucco_data <- read.csv("C:/Users/cedric.antunes/Desktop/Partisan_Ideology/BLS9_full.csv") |>
  select(uniqueid,
         caseid,
         yearcase,
         wave,
         party_survey,
         lrpmdb,
         lrdem,
         lrpcb,
         lrpcdob,
         lrpdc,
         lrpds,
         lrpdt,
         lrpfl,
         lrpl,
         lrpp,
         lrpp_ppb,
         lrppr,
         lrpps,
         lrpr,
         lrprn,
         lrpsb,
         lrpsd,
         lrpsdb,
         lrpsol,
         lrpstu,
         lrpt,
         lrptb,
         lrprb,
         lrsd,
         lrpsc,
         lrpros,
         lrrede,
         lrptn,
         lrpv,
         lrmdb,
         lrpsl,
         lrcid,
         lrrep,
         lrpode,
         lrnovo)

# ------------------------------------------------------------------------------
# PREPARING THE DATA -----------------------------------------------------------
# ------------------------------------------------------------------------------

zucco_data <- zucco_data |> 
  # Settin a variable for survey year
  mutate(ANO = substr(as.character(yearcase), 1, 4)) |>
  # Filtering only relevant electoral years: 2012, 2016, 2020
  filter(ANO %in% c("2013",
                    "2017",
                    "2021"))
  
# Replacing -999 with NA for columns 6 to 40
zucco_data[, 6:40] <- lapply(zucco_data[, 6:40], function(x) ifelse(x == -999, NA, x))

# Getting average party ideology for each year
averages_per_year <- zucco_data |> 
  group_by(ANO) |> 
  summarise(across(6:40, ~ mean(.x, na.rm = TRUE)))

# Reshaping the data from wide to long format
averages_long <- averages_per_year |> 
  pivot_longer(cols = -ANO, 
               names_to = "SIGLA_PARTIDO", 
               values_to = "Ideology_Score") |> 
  # Cleanning and setting party acronyms
  mutate(SIGLA_PARTIDO = toupper(gsub("^lr", "", SIGLA_PARTIDO))) |>
  # Dropping NAs
  drop_na(Ideology_Score)  

# Scaling ideology scores from 0 (Left) to 1 (Right)
averages_long <- averages_long |> 
  mutate(Ideology_Score = scales::rescale(Ideology_Score, to = c(0, 1)))

# ------------------------------------------------------------------------------
# PLOT -------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Plotting parties' average ideology per legislative term
ggplot(averages_long, aes(x = Ideology_Score, y = reorder(SIGLA_PARTIDO, Ideology_Score), color = as.factor(ANO))) +
  geom_point(size = 3, alpha = 0.8) + 
  scale_x_continuous(
    limits = c(0, 1), 
    breaks = seq(0, 1, by = 0.2), 
    labels = c("Left", "", "", "", "", "Right")  # Now matches the number of breaks
  ) +
  labs(title = "Brazilian Parties' Ideology Scores Across Years",
       subtitle = "Positioning of political parties from Left (0) to Right (1)",
       x = "Ideological Spectrum",
       y = "Party",
       color = "Year",
       caption = "Data Source: Power and Zucco (2021)") +
  facet_wrap(~ANO, scales = "free_y") +
  theme_minimal() +
  theme(legend.position = "bottom",
        axis.text.y = element_text(size = 10),
        axis.text.x = element_text(size = 12),
        strip.text = element_text(face = "bold"))

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving as .csv file 
write.csv(averages_long,
          "parties_ideology_2012_2020.csv")
