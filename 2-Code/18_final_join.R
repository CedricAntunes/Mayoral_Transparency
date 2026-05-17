# Author: Cedric Antunes (FGV-CEPESP) ------------------------------------------
# Date: May, 2026 --------------------------------------------------------------
# Final join -------------------------------------------------------------------

# Cleaning my environment 
rm(list = ls())

# Managing memory
gc()

# Required packages ------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(sf)
})

# ------------------------------------------------------------------------------
# Mother-dataset cleaning ------------------------------------------------------
# ------------------------------------------------------------------------------
DATA_PATH <- "C:/Users/cedric.antunes/Desktop/Paper_Transparency/supervised_data_clean.rds"

# Loading the data
d <- read_rds(DATA_PATH) |>
  st_drop_geometry()

# Dropping irrelevant vars 
d <- d |>
  select(-`UNNAMED: 0`,
         -EXTRACT_METHOD,
         -PDF,
         -POP_2010,
         -DENS_DEMO,
         -TAXA_0010,
         -OBSERVACAO,
         -PIB_PER_C,
         -POPULACAO)

# ------------------------------------------------------------------------------
# Municipality time-varying controls -------------------------------------------
# ------------------------------------------------------------------------------
mun <- read_rds("C:/Users/cedric.antunes/Desktop/Paper_Transparency/municipality_year_covariates/municipality_year_FINAL_ibge_siconfi_ideb_2012_2016_2020.rds")

mun_clean <- mun |>
  rename(ANO_ELEICAO = ano_eleicao,
         COD_MUN_IBGE = id_municipio) |>
  select(-ano_fonte,
         -municipio) |>
  mutate(ANO_ELEICAO = as.character(ANO_ELEICAO))

# ------------------------------------------------------------------------------
# Join -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
final_df <- d |>
  left_join(mun_clean,
            by = c("ANO_ELEICAO",
                   "COD_MUN_IBGE"))
