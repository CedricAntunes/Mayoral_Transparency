# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP) ------------------------------------------
# DATE: DECEMBER 2025 ----------------------------------------------------------

# Cleaning my environment 
rm(list = ls())

# Managing memory 
gc()

# Required packages ------------------------------------------------------------
suppressPackageStartupMessages({
  library(sf)
})

# ------------------------------------------------------------------------------
# LOADING THE DATA -------------------------------------------------------------
# ------------------------------------------------------------------------------
final_data <- readRDS("C:/Users/cedric.antunes/Desktop/Paper_Transparency/data_final.rds") |>
  mutate(SEQUENCIAL_CANDIDATO = as.character(SEQUENCIAL_CANDIDATO))

incumbency_data <- readRDS("C:/Users/cedric.antunes/Desktop/Paper_Transparency/mayors_incumbency_status.rds") |>
  st_drop_geometry() 

# ------------------------------------------------------------------------------
# JOIN -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
final_data_incumbents <- final_data |>
  left_join(incumbency_data,
            by = c("SEQUENCIAL_CANDIDATO",
                   "DESCRICAO_UE",
                   "ANO_ELEICAO"))

# ------------------------------------------------------------------------------
# FINAL CLEAN ------------------------------------------------------------------
# ------------------------------------------------------------------------------
final_data_clean <- final_data_incumbents |>
  select(-DESC_SIT_TOT_TURNO.y,
         -SIGLA_UF.y,
         -NOME_CANDIDATO.y,
         -ID_CEPESP.y,
         -ANO.Y,
         -ANO.X,
         -NOME_UF.X,
         -DATA_GERACAO_CEPESP,
         -HORA_GERACAO_CEPESP,
         -NUM_FEDERACAO,
         -NOME_FEDERACAO,
         -SIGLA_FEDERACAO,
         -NOME_MUNICIPIO,
         -NOME_UF,
         -NOME_UF.X) |>
  rename(ID_CEPESP = ID_CEPESP.x,
         NOME_CANDIDATO = NOME_CANDIDATO.x) |>
  mutate(IS_INCUMBENT = if_else(STATUS_INCUMBENT == "INCUMBENT", 1, 0))

# Saving the data --------------------------------------------------------------
saveRDS(final_data_clean,
        "C:/Users/cedric.antunes/Desktop/Paper_Transparency/final_clean_data.rds")
