# Title: Candidtes-Pledges Classified 2012-2020 --------------------------------
# Author: Cedric Antunes (FGV-CEPESP) ------------------------------------------
# Date: April, 2026-------------------------------------------------------------

# Cleaning my environment
rm(list = ls())

# Managing memory
gc()

# Required packages ------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(janitor)
  library(data.table)
  library(arrow)
})

# ------------------------------------------------------------------------------
# Data -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
raw <- fread("D:/Users/cedric/Downloads/candidate_transparency_econometric_ready_2012_2016_2020.csv",
             header = TRUE,
             # Keep encoding for latin names
             encoding = "UTF-8")

# Cleaning ---------------------------------------------------------------------
df_clean <- raw |>
  # Dropping duplicate columns 
  select(-ID_CEPESP_y,
         -ANO_ELEICAO_y,
         -UF_y,
         -STATUS_INCUMBENT_y,
         -PDF_y) |>
  # Renaming for join
  rename(ID_CEPESP = ID_CEPESP_x,
         ANO_ELEICAO = ANO_ELEICAO_x,
         UF = UF_x,
         STATUS_INCUMBENT = STATUS_INCUMBENT_x,
         PDF = PDF_x) |>
  # Making all column names upper case for join
  rename_with(toupper) |>
  # Standardizing for join
  mutate(across(c(ID_CEPESP, 
                  ANO_ELEICAO,
                  NUM_TURNO,
                  COD_MUN_IBGE,
                  COD_MUN_TSE), as.character))

# ------------------------------------------------------------------------------
# Coding outcomes --------------------------------------------------------------
# ------------------------------------------------------------------------------
df_clean <- df_clean |>
  mutate(ELECTED = ifelse(DESC_SIT_TOT_TURNO == "ELEITO", 1, 0),
         CHALLENGER = ifelse(STATUS_INCUMBENT == "CHALLENGER", 1, 0),
         INCUMBENT = ifelse(STATUS_INCUMBENT == "INCUMBENT", 1, 0))

# ------------------------------------------------------------------------------
# Saving -----------------------------------------------------------------------
# ------------------------------------------------------------------------------
write_parquet(df_clean,
              "D:/Users/cedric/Downloads/df_classified_clean.parquet")
