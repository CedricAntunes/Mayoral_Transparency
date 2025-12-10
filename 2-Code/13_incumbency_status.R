# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP) ------------------------------------------
# DATE: DECEMBER 2025 ----------------------------------------------------------

# Cleaning my environment
rm(list = ls())

# Managing memory
gc()

# Seed for replication of sanity check 
set.seed(127)

# Required packages ------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(parquetize)
})

# ------------------------------------------------------------------------------
# LOADING AND PREPARING THE DATA -----------------------------------------------
# ------------------------------------------------------------------------------
cand <- read_parquet("F:/Public/Documents/repositorioTSE/data/output/Candidatos/candidatos_gr_mun_final.parquet") |>
  mutate(ANO_ELEICAO = as.numeric(ANO_ELEICAO))

# Municipal electoral cycles 
years <- seq(2008, 2020, 4)

# Filtering mayors and cleaning special elections
mayors <- cand |>
  filter(
    ANO_ELEICAO %in% years,
    DESCRICAO_CARGO == "PREFEITO"
  ) |>
  group_by(ANO_ELEICAO, 
           SIGLA_UE, 
           ID_CEPESP) |>
  slice_min(DATA_ELEICAO, with_ties = FALSE) |>
  ungroup()

# Elected mayors
elected_prev <- mayors |>
  filter(DESC_SIT_TOT_TURNO == "ELEITO") |>
  mutate(ANO_ELEICAO = ANO_ELEICAO + 4L) |>
  transmute(
    ANO_ELEICAO,          
    SIGLA_UE,
    ID_CEPESP,
    incumbent_flag = 1L
  ) |>
  distinct(ANO_ELEICAO, 
           SIGLA_UE, 
           ID_CEPESP, .keep_all = TRUE)

# ------------------------------------------------------------------------------
# JOIN -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
cand_inc <- mayors |>
  left_join(
    elected_prev,
    by = c("ANO_ELEICAO", 
           "SIGLA_UE", 
           "ID_CEPESP")
  ) |>
  mutate(
    INCUMBENT  = if_else(!is.na(incumbent_flag), 1L, 0L),
    CHALLEGER = if_else(ANO_ELEICAO >= 2012 & INCUMBENT == 0L, 1L, 0L),
    STATUS_INCUMBENT = case_when(
      ANO_ELEICAO >= 2012 & INCUMBENT == 1L ~ "INCUMBENT",
      ANO_ELEICAO >= 2012 & INCUMBENT == 0L ~ "CHALLENGER",
      TRUE                                  ~ NA_character_
    )
  )

# Final data: dropping 2008 ----------------------------------------------------
mayors_final <- cand_inc |>
  filter(ANO_ELEICAO != 2008)

# ------------------------------------------------------------------------------
# Sanity check -----------------------------------------------------------------
# ------------------------------------------------------------------------------
mayors_check <- mayors_final |>
  select(ANO_ELEICAO,
         ID_CEPESP,
         NOME_CANDIDATO,
         SIGLA_UF,
         DESCRICAO_UE,
         DESC_SIT_TOT_TURNO,
         STATUS_INCUMBENT)

# Checking things manually with the electoral results: all good!
random_sample_df <- mayors_check[sample(nrow(mayors_check), 5, replace = FALSE), ]

# Saving the data --------------------------------------------------------------
saveRDS(mayors_check, 
        "mayors_incumbency_status.rds")
