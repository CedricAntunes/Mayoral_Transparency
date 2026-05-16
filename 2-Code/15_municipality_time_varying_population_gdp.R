# Author: Cedric Antunes (FGV-CEPESP) ------------------------------------------
# Date: May, 2026 --------------------------------------------------------------
# Objectives:
# - Extracts and clean population and GDP per capita per municiaplity for
# for 2012, 2016, and 2020

# Cleaning my environment
rm(list = ls())

# Managing memory
gc()

# Packages ---------------------------------------------------------------------
suppressPackageStartupMessages({
  library(sidrar)
  library(dplyr)
  library(tidyr)
  library(readr)
  library(stringr)
  library(janitor)
})

# ------------------------------------------------------------------------------
# Settings ---------------------------------------------------------------------
# ------------------------------------------------------------------------------
ELECTION_YEARS <- c(2012, 2016, 2020)

USE_LAGGED_COVARS <- TRUE

# Measuring municipality covariates with one-year lags
SOURCE_YEARS <- if (USE_LAGGED_COVARS) {
  ELECTION_YEARS - 1
} else {
  ELECTION_YEARS
}

# Reference sheet
year_map <- tibble(
  ano_eleicao = ELECTION_YEARS,
  ano_fonte = SOURCE_YEARS
)

# Output directory
OUT_DIR <- "C:/Users/cedric.antunes/Desktop/Paper_Transparency/time_varying_mun_covariates"

dir.create(OUT_DIR, 
           recursive = TRUE, 
           showWarnings = FALSE)

# ------------------------------------------------------------------------------
# Helpers ----------------------------------------------------------------------
# ------------------------------------------------------------------------------
to_numeric_br <- function(x) {
  x <- as.character(x)
  x <- str_replace_all(x, "\\.", "")
  x <- str_replace_all(x, ",", ".")
  suppressWarnings(as.numeric(x))
}

# Standardization
zscore <- function(x) {
  x <- as.numeric(x)
  s <- sd(x, na.rm = TRUE)
  
  if (!is.finite(s) || is.na(s) || s == 0) {
    return(rep(NA_real_, length(x)))
  }
  
  as.numeric((x - mean(x, na.rm = TRUE)) / s)
}

# Log transformation
safe_log1p <- function(x) {
  x <- as.numeric(x)
  ifelse(x < 0, NA_real_, log1p(x))
}

# Columns haromization
standardize_sidra_cols <- function(df) {
  df |>
    janitor::clean_names() |>
    rename_with(~ str_replace_all(.x, "_codigo$", "_cod"))
}

# ------------------------------------------------------------------------------
# Population: SIDRA table 6579 -------------------------------------------------
# ------------------------------------------------------------------------------
# Table 6579 = estimated resident population.
# Variable 9324 = População residente estimada. 
pop_raw <- sidrar::get_sidra(
  x = 6579,
  variable = 9324,
  period = as.character(SOURCE_YEARS),
  geo = "City",
  format = 4
) |>
  standardize_sidra_cols()

# Checks
names(pop_raw)
glimpse(pop_raw)

# Cleaning
pop <- pop_raw |>
  transmute(
    ano_fonte = as.integer(ano_cod),
    id_municipio = as.character(municipio_cod),
    municipio = municipio,
    populacao = to_numeric_br(valor)
  ) |>
  left_join(year_map, by = "ano_fonte") |>
  select(
    ano_eleicao,
    ano_fonte,
    id_municipio,
    municipio,
    populacao
  )

# ------------------------------------------------------------------------------
# 4. GDP: SIDRA table 5938 -----------------------------------------------------
# ------------------------------------------------------------------------------
# We use variable 37:
#   Produto Interno Bruto a preços correntes
#
# GDP per capita will be calculated manually:
#   pib_per_capita = pib_total / populacao
#
# Note: if SIDRA reports PIB in R$ 1,000, then pib_total_reais multiplies by 1000.
# -------------------------------------------------------------------------
PIB_TOTAL_VAR <- "37"

# Extraction
pib_raw <- sidrar::get_sidra(
  x = 5938,
  variable = PIB_TOTAL_VAR,
  period = as.character(SOURCE_YEARS),
  geo = "City",
  format = 4
) |>
  standardize_sidra_cols()

# Cleaning
pib <- pib_raw |>
  mutate(
    ano_fonte = as.integer(ano_cod),
    id_municipio = as.character(municipio_cod),
    pib_total_raw = to_numeric_br(valor)
  ) |>
  filter(
    ano_fonte %in% SOURCE_YEARS
  ) |>
  transmute(
    ano_fonte,
    id_municipio,
    pib_total_raw
  ) |>
  left_join(year_map, by = "ano_fonte") |>
  select(
    ano_eleicao,
    ano_fonte,
    id_municipio,
    pib_total_raw
  )

# ------------------------------------------------------------------------------
# Join -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
muni_year_panel <- pop |>
  left_join(
    pib,
    by = c("ano_eleicao", "ano_fonte", "id_municipio")
  ) |>
  mutate(
    # SIDRA table 5938 normally reports monetary values in R$ 1,000.
    # If your diagnostics show implausibly high/low GDP per capita,
    # check the 'unidade_de_medida' column in pib_raw.
    pib_total_reais = pib_total_raw * 1000,
    
    pib_per_capita = case_when(
      !is.na(pib_total_reais) & !is.na(populacao) & populacao > 0 ~
        pib_total_reais / populacao,
      TRUE ~ NA_real_
    ),
    
    log_populacao = safe_log1p(populacao),
    log_pib_total = safe_log1p(pib_total_reais),
    log_pib_per_capita = safe_log1p(pib_per_capita),
    
    z_log_populacao = zscore(log_populacao),
    z_log_pib_total = zscore(log_pib_total),
    z_log_pib_per_capita = zscore(log_pib_per_capita)
  )

# ------------------------------------------------------------------------------
# Diagnostics ------------------------------------------------------------------
# ------------------------------------------------------------------------------
diagnostics_by_year <- muni_year_panel |>
  group_by(ano_eleicao, ano_fonte) |>
  summarise(
    n_rows = n(),
    n_municipalities = n_distinct(id_municipio),
    missing_population = sum(is.na(populacao)),
    missing_pib_total = sum(is.na(pib_total_reais)),
    missing_pib_per_capita = sum(is.na(pib_per_capita)),
    mean_pib_per_capita = mean(pib_per_capita, na.rm = TRUE),
    median_pib_per_capita = median(pib_per_capita, na.rm = TRUE),
    min_pib_per_capita = min(pib_per_capita, na.rm = TRUE),
    max_pib_per_capita = max(pib_per_capita, na.rm = TRUE),
    .groups = "drop"
  )

diagnostics_overall <- muni_year_panel |>
  summarise(
    n_rows = n(),
    n_municipalities = n_distinct(id_municipio),
    years = n_distinct(ano_eleicao),
    missing_population = sum(is.na(populacao)),
    missing_pib_total = sum(is.na(pib_total_reais)),
    missing_pib_per_capita = sum(is.na(pib_per_capita)),
    mean_pib_per_capita = mean(pib_per_capita, na.rm = TRUE),
    median_pib_per_capita = median(pib_per_capita, na.rm = TRUE),
    min_pib_per_capita = min(pib_per_capita, na.rm = TRUE),
    max_pib_per_capita = max(pib_per_capita, na.rm = TRUE)
  )

print(diagnostics_by_year)
print(diagnostics_overall)

# ------------------------------------------------------------------------------
# Saving the data --------------------------------------------------------------
# ------------------------------------------------------------------------------
write_rds(
  muni_year_panel,
  file.path(OUT_DIR, "municipality_year_ibge_population_gdp_2012_2016_2020.rds")
)

write_csv(
  muni_year_panel,
  file.path(OUT_DIR, "municipality_year_ibge_population_gdp_2012_2016_2020.csv")
)

write_csv(
  diagnostics_by_year,
  file.path(OUT_DIR, "diagnostics_by_year.csv")
)

write_csv(
  diagnostics_overall,
  file.path(OUT_DIR, "diagnostics_overall.csv")
)
