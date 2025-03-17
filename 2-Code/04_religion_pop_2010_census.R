# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: MARCH 2025

# Cleaning my environment  
rm(list = ls())

# Memory management  
gc()

# Useful packages 
library(tidyverse)
library(dplyr)
library(censobr)

# Checking vars  
data_dictionary(year = 2010,
                dataset = "population",
                showProgress = TRUE,
                cache = TRUE)


# ------------------------------------------------------------------------------
# EXTRACTING THE DATA ----------------------------------------------------------
# ------------------------------------------------------------------------------

# Downloading 2010 Census data: population level
data_2010 <- read_population(
  year = 2010,
  columns = NULL,
  add_labels = NULL,
  as_data_frame = TRUE,
  showProgress = TRUE,
  cache = TRUE
)

# ------------------------------------------------------------------------------
# ORGANIZING THE DATA ----------------------------------------------------------
# ------------------------------------------------------------------------------

# Population data  
data_selected <- data_2010 |>
  # Renaming relevant vars
  rename(
   RELIGIAO = V6121) |>
  # Selecting only necessary vars 
  select(code_muni,
         abbrev_state,
         name_state,
         RELIGIAO)

# ------------------------------------------------------------------------------
# RELIGIOUS DATA ---------------------------------------------------------------
# ------------------------------------------------------------------------------

# Defining religious groups based on Rocha et al. (2018)
data_aggregated <- data_selected |>
  # Renaming vars 
  mutate(
    Catholic = ifelse(RELIGIAO %in% c(110:199), 1, 0),
    Protestant = ifelse(RELIGIAO %in% c(210:300), 1, 0),
    Pentecostal = ifelse(RELIGIAO %in% c(310:499), 1, 0),
    Other_Christian = ifelse(RELIGIAO %in% c(510:599), 1, 0),
    Spiritist = ifelse(RELIGIAO %in% c(610:619), 1, 0),
    Afro_Brazilian = ifelse(RELIGIAO %in% c(620:649), 1, 0),
    Judaism = ifelse(RELIGIAO %in% c(710:719), 1, 0),
    Oriental_Religions = ifelse(RELIGIAO %in% c(740:799), 1, 0),
    Islam = ifelse(RELIGIAO %in% c(810:819), 1, 0),
    Esoteric = ifelse(RELIGIAO %in% c(820:829), 1, 0),
    Indigenous_Beliefs = ifelse(RELIGIAO %in% c(830:839), 1, 0),
    Other_Religions = ifelse(RELIGIAO %in% c(850:899), 1, 0),
    Ignored = ifelse(RELIGIAO == 999, 1, 0)
  ) |>
  # Groupping by municipality
  group_by(code_muni, 
           abbrev_state, 
           name_state) |>
  # Summing individuals at the municipal level 
  summarise(
    Total_Religious_Population = n(),
    Catholic = sum(Catholic),
    Protestant = sum(Protestant),
    Pentecostal = sum(Pentecostal),
    Other_Christian = sum(Other_Christian),
    Spiritist = sum(Spiritist),
    Afro_Brazilian = sum(Afro_Brazilian),
    Judaism = sum(Judaism),
    Oriental_Religions = sum(Oriental_Religions),
    Islam = sum(Islam),
    Esoteric = sum(Esoteric),
    Indigenous_Beliefs = sum(Indigenous_Beliefs),
    Other_Religions = sum(Other_Religions),
    Ignored = sum(Ignored)
  ) |>
  ungroup()

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving the data as .csv file 
write.csv(data_aggregated,
          "religiao_pop_censo_2010.csv",
          fileEncoding = "latin1")
