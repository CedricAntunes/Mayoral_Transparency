# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: FEBRUARY 2025

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

# Baixando dados de indivíduos no nível do setor censitário
data_2010 <- read_households(
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

# Data per household: 2010
data_selected <- data_2010 |>
  # Renaming key vars 
  rename(
    electricity_access = V0211,
    radio = V0213,
    television = V0214,
    cellphone = V0217,
    phone = V0218,
    computer = V0219,
    internet_access = V0220) |>
  # Selecting only key vars 
  select(code_muni,
         abbrev_state,
         name_state,
         electricity_access,
         radio,
         television,
         cellphone,
         phone,
         computer,
         internet_access) |>
  # Standardizing data for merge
  mutate(electricity_access = as.numeric(electricity_access),
         radio = as.numeric(radio),
         cellphone = as.numeric(cellphone),
         television = as.numeric(television),
         phone = as.numeric(phone),
         computer = as.numeric(computer),
         internet_access = as.numeric(internet_access))


# Quantifying "Yes" or "No" answers
data_selected <- data_selected |>
  mutate(across(c(electricity_access, 
                  radio, 
                  television, 
                  cellphone, 
                  phone, 
                  computer, 
                  internet_access),
                ~ if_else(. == 2, 0, .)))

# Getting final dataset 
data_muni <- data_selected |>
  # Groupping households per municipality
  group_by(code_muni, 
           abbrev_state, 
           name_state) |>
  # Summing households at the municipality level
  summarise(across(c(electricity_access, 
                     radio, 
                     television, 
                     cellphone, 
                     phone, 
                     computer, 
                     internet_access),
                   sum, na.rm = TRUE))

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving the data as .csv
write.csv(data_muni,
          "household_municipality_2010_census.csv",
          fileEncoding = "latin1")
