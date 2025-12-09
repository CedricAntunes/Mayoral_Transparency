# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: DECEMBER 2025

# Cleaning my environment 
rm(list = ls())

# Managing memory
gc()

# Required packages ------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(sf)
})

# ------------------------------------------------------------------------------
# LOADING THE DATA -------------------------------------------------------------
# ------------------------------------------------------------------------------

# District magnitude data ------------------------------------------------------
vagas <- readRDS("F:/Public/Documents/tabelasAuxiliares/Data/Output/vagas_vereadores_2000_2020.rds")

# Analysis data ----------------------------------------------------------------
data <- readRDS("C:/Users/cedric.antunes/Downloads/revised_neighbors_data.rds")  |>
  st_drop_geometry()

# ------------------------------------------------------------------------------
# DATA PREPARATION -------------------------------------------------------------
# ------------------------------------------------------------------------------
vagas_clean <- vagas |>
  select(COD_MUN_IBGE,
         `2012`,
         `2016`,
         `2020`)

# Setting data as long
vagas_long <- vagas_clean |>
  pivot_longer(
    cols      = -COD_MUN_IBGE,      
    names_to  = "year",             
    # New column for district magnitude
    values_to = "N_LEGISLATIVE_SEATS"           
  ) |>
  mutate(year = as.integer(year)) |>
  # Preparing for joing
  rename(ANO_ELEICAO = year)

# ------------------------------------------------------------------------------
# JOIN -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
data_final <- data |>
  left_join(vagas_long,
            by = c("COD_MUN_IBGE",
                   "ANO_ELEICAO"))

# Saving final data ------------------------------------------------------------
saveRDS(data_final,
        "C:/Users/cedric.antunes/Desktop/Paper_Transparency/data_final.rds")
