# Author: Cedric Antunes (FGV-CEPESP) ------------------------------------------
# Date: May, 2026 --------------------------------------------------------------
# SICONFI / FINBRA fiscal data ----------------------------------------

# Cleaning my environment 
rm(list = ls())

# Managing memory
gc()

# Required packages ------------------------------------------------------------
suppressPackageStartupMessages({
  library(tesouror)
  library(dplyr)
  library(purrr)
  library(readr)
  library(janitor)
  library(stringr)
})

# ------------------------------------------------------------------------------
# Settings ---------------------------------------------------------------------
# ------------------------------------------------------------------------------
OUT_DIR <- "C:/Users/cedric.antunes/Desktop/Paper_Transparency/municipality_year_covariates_no_google"

# Electoral cycles
ELECTION_YEARS <- c(2012, 2016, 2020)

# Lagged measurement
SOURCE_YEARS <- ELECTION_YEARS - 1

# UFs
UF_LIST <- c(
  "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
  "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
  "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
)

year_map <- tibble(
  ano_eleicao = ELECTION_YEARS,
  ano_fonte = SOURCE_YEARS
)

# ------------------------------------------------------------------------------
# Extracting raw DCA data by state-year ----------------------------------------
# ------------------------------------------------------------------------------
# DCA = Declaração de Contas Anuais.
# This is better than calling municipality by municipality.
safe_get_dca_state <- possibly(
  function(uf, year) {
    message("Downloading DCA: ", uf, " - ", year)
    
    tesouror::get_dca_for_state(
      state_uf = uf,
      an_exercicio = year,
      include_capital = TRUE,
      on_error = "warn",
      use_cache = TRUE
    ) |>
      janitor::clean_names() |>
      mutate(
        uf = uf,
        ano_fonte = year
      )
  },
  otherwise = tibble()
)

dca_raw <- map_dfr(SOURCE_YEARS, function(y) {
  map_dfr(UF_LIST, function(uf) {
    safe_get_dca_state(uf, y)
  })
})

#write_rds(
#  dca_raw,
#  file.path(OUT_DIR, "siconfi_dca_raw_2011_2015_2019.rds")
#)

#write_csv(
#  dca_raw,
#  file.path(OUT_DIR, "siconfi_dca_raw_2011_2015_2019.csv")
#)

# ------------------------------------------------------------------------------
# Inspecting column names and account labels -----------------------------------
# ------------------------------------------------------------------------------
dca_columns <- tibble(column = names(dca_raw))

#write_csv(
#  dca_columns,
#  file.path(OUT_DIR, "siconfi_dca_column_names.csv")
#)

# Trying to identify account/name/value columns
possible_account_cols <- names(dca_raw)[
  str_detect(names(dca_raw), "conta|rotulo|descricao|item|coluna|cod")
]

possible_value_cols <- names(dca_raw)[
  str_detect(names(dca_raw), "valor|saldo|vl")
]

account_preview <- dca_raw |>
  select(any_of(c("ano_fonte", "uf", possible_account_cols, possible_value_cols))) |>
  head(500)

#write_csv(
#  account_preview,
#  file.path(OUT_DIR, "siconfi_dca_account_preview.csv")
#)

# Chekcs
print(names(dca_raw))
print(possible_account_cols)
print(possible_value_cols)

# ------------------------------------------------------------------------------
# Aggregating SICONFI / DCA fiscal variables -----------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Helper functions -------------------------------------------------------------
# ------------------------------------------------------------------------------
# Standardizing
zscore <- function(x) {
  x <- as.numeric(x)
  s <- sd(x, na.rm = TRUE)
  
  if (!is.finite(s) || is.na(s) || s == 0) {
    return(rep(NA_real_, length(x)))
  }
  
  as.numeric((x - mean(x, na.rm = TRUE)) / s)
}

# Safe log transformations
safe_log1p <- function(x) {
  x <- as.numeric(x)
  ifelse(x < 0, NA_real_, log1p(x))
}

# Clean municipality name/code
clean_id_municipio <- function(x) {
  x <- as.character(x)
  x <- str_replace_all(x, "[^0-9]", "")
  str_pad(x, width = 7, side = "left", pad = "0")
}

# ------------------------------------------------------------------------------
# Cleaning DCA raw data --------------------------------------------------------
# ------------------------------------------------------------------------------
dca_clean <- dca_raw |>
  janitor::clean_names() |>
  mutate(
    id_municipio = clean_id_municipio(cod_ibge),
    ano_fonte = as.integer(ano_fonte),
    valor = as.numeric(valor),
    coluna = as.character(coluna),
    cod_conta = as.character(cod_conta),
    conta = as.character(conta),
    conta_lower = str_to_lower(conta)
  )

# ------------------------------------------------------------------------------
# Coverage check ---------------------------------------------------------------
# ------------------------------------------------------------------------------
dca_coverage <- dca_clean |>
  distinct(ano_fonte, uf, id_municipio) |>
  count(ano_fonte, name = "n_municipalities")

print(dca_coverage)

# ------------------------------------------------------------------------------
# Revenue aggregates -----------------------------------------------------------
# ------------------------------------------------------------------------------
# We calculate both gross and net revenue.
#
# Gross total revenue:
#   TotalReceitas / Total Receitas, column "Receitas Brutas Realizadas"
#
# FUNDEB deductions:
#   TotalReceitas / Total Receitas, column "Deduções - FUNDEB"
#
# Net revenue:
#   gross revenue - FUNDEB deductions
#
# Own revenue:
#   "Receita Tributária" under gross realized revenues.
# ------------------------------------------------------------------------------
receita_total <- dca_clean |>
  filter(
    coluna == "Receitas Brutas Realizadas",
    cod_conta == "TotalReceitas"
  ) |>
  group_by(ano_fonte, id_municipio) |>
  summarise(
    receita_total_bruta = sum(valor, na.rm = TRUE),
    .groups = "drop"
  )

deducoes_fundeb <- dca_clean |>
  filter(
    coluna == "Deduções - FUNDEB",
    cod_conta == "TotalReceitas"
  ) |>
  group_by(ano_fonte, id_municipio) |>
  summarise(
    deducoes_fundeb = sum(valor, na.rm = TRUE),
    .groups = "drop"
  )

receita_propria <- dca_clean |>
  filter(
    coluna == "Receitas Brutas Realizadas",
    str_detect(conta_lower, "receita tribut")
  ) |>
  group_by(ano_fonte, id_municipio) |>
  summarise(
    receita_propria = sum(valor, na.rm = TRUE),
    .groups = "drop"
  )

# ------------------------------------------------------------------------------
# Expenditure aggregates -------------------------------------------------------
# ------------------------------------------------------------------------------
# We use committed expenditure ("Despesas Empenhadas") as the main measure.
# ------------------------------------------------------------------------------
despesa_total <- dca_clean |>
  filter(
    coluna == "Despesas Empenhadas",
    cod_conta == "TotalDespesas"
  ) |>
  group_by(ano_fonte, id_municipio) |>
  summarise(
    despesa_total_empenhada = sum(valor, na.rm = TRUE),
    .groups = "drop"
  )

despesa_pessoal <- dca_clean |>
  filter(
    coluna == "Despesas Empenhadas",
    str_detect(conta_lower, "pessoal e encargos sociais")
  ) |>
  group_by(ano_fonte, id_municipio) |>
  summarise(
    despesa_pessoal_empenhada = sum(valor, na.rm = TRUE),
    .groups = "drop"
  )

investimento <- dca_clean |>
  filter(
    coluna == "Despesas Empenhadas",
    str_detect(conta_lower, "investimentos")
  ) |>
  group_by(ano_fonte, id_municipio) |>
  summarise(
    investimento_empenhado = sum(valor, na.rm = TRUE),
    .groups = "drop"
  )

# ------------------------------------------------------------------------------
# Balance-sheet liability proxy ------------------------------------------------
# ------------------------------------------------------------------------------
passivo_circulante <- dca_clean |>
  filter(
    str_detect(coluna, "^31/12"),
    str_detect(conta_lower, "passivo circulante"),
    !str_detect(conta_lower, "não|nao")
  ) |>
  group_by(ano_fonte, id_municipio) |>
  summarise(
    passivo_circulante = sum(valor, na.rm = TRUE),
    .groups = "drop"
  )

passivo_nao_circulante <- dca_clean |>
  filter(
    str_detect(coluna, "^31/12"),
    str_detect(conta_lower, "passivo n[aã]o[- ]?circulante|passivo não-circulante|passivo nao-circulante")
  ) |>
  group_by(ano_fonte, id_municipio) |>
  summarise(
    passivo_nao_circulante = sum(valor, na.rm = TRUE),
    .groups = "drop"
  )

# ------------------------------------------------------------------------------
# Building fiscal panel --------------------------------------------------------
# ------------------------------------------------------------------------------
fiscal_panel <- receita_total |>
  full_join(deducoes_fundeb, by = c("ano_fonte", "id_municipio")) |>
  full_join(receita_propria, by = c("ano_fonte", "id_municipio")) |>
  full_join(despesa_total, by = c("ano_fonte", "id_municipio")) |>
  full_join(despesa_pessoal, by = c("ano_fonte", "id_municipio")) |>
  full_join(investimento, by = c("ano_fonte", "id_municipio")) |>
  full_join(passivo_circulante, by = c("ano_fonte", "id_municipio")) |>
  full_join(passivo_nao_circulante, by = c("ano_fonte", "id_municipio")) |>
  left_join(year_map, by = "ano_fonte") |>
  mutate(
    deducoes_fundeb = replace_na(deducoes_fundeb, 0),
    
    receita_total_liquida = receita_total_bruta - deducoes_fundeb,
    
    receita_propria_share = case_when(
      !is.na(receita_total_liquida) & receita_total_liquida > 0 ~
        receita_propria / receita_total_liquida,
      TRUE ~ NA_real_
    ),
    
    despesa_pessoal_share = case_when(
      !is.na(despesa_total_empenhada) & despesa_total_empenhada > 0 ~
        despesa_pessoal_empenhada / despesa_total_empenhada,
      TRUE ~ NA_real_
    ),
    
    investimento_share = case_when(
      !is.na(despesa_total_empenhada) & despesa_total_empenhada > 0 ~
        investimento_empenhado / despesa_total_empenhada,
      TRUE ~ NA_real_
    ),
    
    passivo_total_proxy = coalesce(passivo_circulante, 0) +
      coalesce(passivo_nao_circulante, 0)
  ) |>
  select(
    ano_eleicao,
    ano_fonte,
    id_municipio,
    receita_total_bruta,
    deducoes_fundeb,
    receita_total_liquida,
    receita_propria,
    receita_propria_share,
    despesa_total_empenhada,
    despesa_pessoal_empenhada,
    despesa_pessoal_share,
    investimento_empenhado,
    investimento_share,
    passivo_circulante,
    passivo_nao_circulante,
    passivo_total_proxy
  )

# ------------------------------------------------------------------------------
# Join -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
muni_year_panel_fiscal <- muni_year_panel |>
  mutate(
    id_municipio = clean_id_municipio(id_municipio)
  ) |>
  left_join(
    fiscal_panel,
    by = c("ano_eleicao", "ano_fonte", "id_municipio")
  ) |>
  mutate(
    receita_total_liquida_pc = receita_total_liquida / populacao,
    receita_propria_pc = receita_propria / populacao,
    despesa_total_empenhada_pc = despesa_total_empenhada / populacao,
    despesa_pessoal_empenhada_pc = despesa_pessoal_empenhada / populacao,
    investimento_empenhado_pc = investimento_empenhado / populacao,
    passivo_total_proxy_pc = passivo_total_proxy / populacao,
    
    log_receita_total_liquida_pc = safe_log1p(receita_total_liquida_pc),
    log_receita_propria_pc = safe_log1p(receita_propria_pc),
    log_despesa_total_empenhada_pc = safe_log1p(despesa_total_empenhada_pc),
    log_despesa_pessoal_empenhada_pc = safe_log1p(despesa_pessoal_empenhada_pc),
    log_investimento_empenhado_pc = safe_log1p(investimento_empenhado_pc),
    log_passivo_total_proxy_pc = safe_log1p(passivo_total_proxy_pc),
    
    z_log_receita_total_liquida_pc = zscore(log_receita_total_liquida_pc),
    z_log_receita_propria_pc = zscore(log_receita_propria_pc),
    z_receita_propria_share = zscore(receita_propria_share),
    z_log_despesa_total_empenhada_pc = zscore(log_despesa_total_empenhada_pc),
    z_log_despesa_pessoal_empenhada_pc = zscore(log_despesa_pessoal_empenhada_pc),
    z_despesa_pessoal_share = zscore(despesa_pessoal_share),
    z_log_investimento_empenhado_pc = zscore(log_investimento_empenhado_pc),
    z_investimento_share = zscore(investimento_share),
    z_log_passivo_total_proxy_pc = zscore(log_passivo_total_proxy_pc)
  )

# ------------------------------------------------------------------------------
# Diagnostics ------------------------------------------------------------------
# ------------------------------------------------------------------------------
fiscal_diagnostics_by_year <- muni_year_panel_fiscal |>
  group_by(ano_eleicao, ano_fonte) |>
  summarise(
    n_rows = n(),
    n_municipalities = n_distinct(id_municipio),
    missing_receita = sum(is.na(receita_total_liquida)),
    missing_receita_pc = sum(is.na(receita_total_liquida_pc)),
    missing_receita_propria_share = sum(is.na(receita_propria_share)),
    missing_despesa = sum(is.na(despesa_total_empenhada)),
    missing_despesa_pessoal_share = sum(is.na(despesa_pessoal_share)),
    missing_investimento = sum(is.na(investimento_empenhado)),
    missing_passivo_proxy = sum(is.na(passivo_total_proxy)),
    mean_receita_pc = mean(receita_total_liquida_pc, na.rm = TRUE),
    median_receita_pc = median(receita_total_liquida_pc, na.rm = TRUE),
    mean_despesa_pc = mean(despesa_total_empenhada_pc, na.rm = TRUE),
    median_despesa_pc = median(despesa_total_empenhada_pc, na.rm = TRUE),
    mean_passivo_proxy_pc = mean(passivo_total_proxy_pc, na.rm = TRUE),
    median_passivo_proxy_pc = median(passivo_total_proxy_pc, na.rm = TRUE),
    .groups = "drop"
  )

fiscal_diagnostics_overall <- muni_year_panel_fiscal |>
  summarise(
    n_rows = n(),
    n_municipalities = n_distinct(id_municipio),
    years = n_distinct(ano_eleicao),
    missing_receita = sum(is.na(receita_total_liquida)),
    missing_receita_pc = sum(is.na(receita_total_liquida_pc)),
    missing_receita_propria_share = sum(is.na(receita_propria_share)),
    missing_despesa = sum(is.na(despesa_total_empenhada)),
    missing_despesa_pessoal_share = sum(is.na(despesa_pessoal_share)),
    missing_investimento = sum(is.na(investimento_empenhado)),
    missing_passivo_proxy = sum(is.na(passivo_total_proxy)),
    mean_receita_pc = mean(receita_total_liquida_pc, na.rm = TRUE),
    median_receita_pc = median(receita_total_liquida_pc, na.rm = TRUE),
    mean_despesa_pc = mean(despesa_total_empenhada_pc, na.rm = TRUE),
    median_despesa_pc = median(despesa_total_empenhada_pc, na.rm = TRUE),
    mean_passivo_proxy_pc = mean(passivo_total_proxy_pc, na.rm = TRUE),
    median_passivo_proxy_pc = median(passivo_total_proxy_pc, na.rm = TRUE)
  )

print(fiscal_diagnostics_by_year)
print(fiscal_diagnostics_overall)

# ------------------------------------------------------------------------------
# Saving the data --------------------------------------------------------------
# ------------------------------------------------------------------------------
write_rds(
  fiscal_panel,
  file.path(OUT_DIR, "siconfi_dca_fiscal_panel_2011_2015_2019.rds")
)

write_csv(
  fiscal_panel,
  file.path(OUT_DIR, "siconfi_dca_fiscal_panel_2011_2015_2019.csv")
)

write_rds(
  muni_year_panel_fiscal,
  file.path(OUT_DIR, "municipality_year_ibge_siconfi_2012_2016_2020.rds")
)

write_csv(
  muni_year_panel_fiscal,
  file.path(OUT_DIR, "municipality_year_ibge_siconfi_2012_2016_2020.csv")
)
