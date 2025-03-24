# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: MARCH 2025

# Cleaning my environment 
rm(list = ls())

# Managing memory
gc()

# Attaching required packages 
library(readxl)
library(dplyr)
library(arrow)
library(dplyr)
library(tidyverse)
library(arrow)
library(parquetize)
library(sf)

# ------------------------------------------------------------------------------
# LOADING DATA -----------------------------------------------------------------
# ------------------------------------------------------------------------------

# Loading final data 
merged_data <- readRDS("merged_data.rds")

# Loading MUNIC 2019 data 
munic <- read_excel("C:/Users/cedric.antunes/Downloads/Base_MUNIC_2019_20210817.xlsx", 
                   sheet = "Governança") |>
  rename(NOME_MUNIC = `NOME MUNIC`)

# New collected data 
new_lai_data <- read_excel("mun_LAI_2020_2024.xlsx")

# ------------------------------------------------------------------------------
# CLEANING THE DATA ------------------------------------------------------------
# ------------------------------------------------------------------------------

# Preparing new lai data
new_lai_data <- new_lai_data |> 
  filter(!is.na(ANO)) |>
  # Renaming for join 
  rename(NOME_MUNIC = Municipio)

# ------------------------------------------------------------------------------
# JOIN -------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Updating MUNIC 2019 with 2020-2024 data 
munic_updated <- munic |>
  left_join(new_lai_data |>
              select(NOME_MUNIC, 
                     ANO), 
            by = c("NOME_MUNIC"))

# Cleaning MUNIC 1990-2024
munic_updated <- munic_updated |>
  mutate(
    MGOV01 = ifelse(!is.na(ANO), "Sim", MGOV01),
    MGOV011B = ifelse(!is.na(ANO), ANO, MGOV011B)
  )

# Dropping year variable
munic_updated <- munic_updated |> 
  select(-ANO)

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving .rds file
saveRDS(munic_updated,
        "final_munic_data.rds")

# Saving as .csv
write.csv(munic_updated,
          "final_munic_data.csv",
          fileEncoding = "latin1")
