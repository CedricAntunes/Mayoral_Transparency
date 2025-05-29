# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: MAY 2025

rm(list = ls())

library(tidyverse)
library(dplyr)
library(sf)

data <- readRDS("C:/Users/cedric.antunes/Downloads/final_data_neighbors.rds") |>
  st_drop_geometry()
  

data |> glimpse()

filtered_candidates <- data |>
  mutate(ANO_ELEICAO = as.numeric(ANO_ELEICAO)) |>
  filter(
    LEI.DE.ACESSO.LAI > 0,
    YEAR_LEGISLATION > ANO_ELEICAO,
    YEAR_LEGISLATION <= ANO_ELEICAO + 4,
    DESC_SIT_TOT_TURNO == "ELEITO",
  ) |>
  select(ANO_ELEICAO,
         NOME_CANDIDATO,
         NOME_PARTIDO,
         NOME_MUNICIPIO,
         DESC_SIT_TOT_TURNO,
         YEAR_LEGISLATION)

write.csv(filtered_candidates,
          "C:/Users/cedric.antunes/Desktop/Mayoral_Transparency/lai_on_pledges.csv",
          fileEncoding = "latin1")
