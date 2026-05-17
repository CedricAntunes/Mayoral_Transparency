# Author: Cedric Antunes (FGV-CEPESP) ------------------------------------------
# Date: May, 2026 --------------------------------------------------------------
# IDEB municipality-year education covariates ----------------------------------

# Cleaning my environment 
rm(list = ls())

# Managing memory
gc()

# Required packages ------------------------------------------------------------
suppressPackageStartupMessages({
  library(educabR)
  library(dplyr)
  library(tidyr)
  library(readr)
  library(stringr)
  library(janitor)
})

# ------------------------------------------------------------------------------
# Settings ---------------------------------------------------------------------
# ------------------------------------------------------------------------------
OUT_DIR <- "C:/Users/cedric.antunes/Desktop/Paper_Transparency/municipality_year_covariates_no_google"

# Electoral cycles
ELECTION_YEARS <- c(2012, 2016, 2020)

# Measurement with one year lag
SOURCE_YEARS <- ELECTION_YEARS - 1

year_map <- tibble(
  ano_eleicao = ELECTION_YEARS,
  ano_fonte = SOURCE_YEARS
)

# ------------------------------------------------------------------------------
# Helpers ----------------------------------------------------------------------
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

# Clean municipality name
clean_id_municipio <- function(x) {
  x <- as.character(x)
  x <- str_replace_all(x, "[^0-9]", "")
  str_pad(x, width = 7, side = "left", pad = "0")
}

# ------------------------------------------------------------------------------
# Extracting raw data ----------------------------------------------------------
# ------------------------------------------------------------------------------
# We pull municipality-level IDEB indicators for:
#   - anos_iniciais: early elementary
#   - anos_finais: late elementary
# ------------------------------------------------------------------------------
ideb_iniciais_raw <- educabR::get_ideb(
  level  = "municipio",
  stage  = "anos_iniciais",
  metric = "indicador",
  year   = SOURCE_YEARS,
  quiet  = FALSE
) |>
  janitor::clean_names() |>
  mutate(stage = "anos_iniciais")

ideb_finais_raw <- educabR::get_ideb(
  level  = "municipio",
  stage  = "anos_finais",
  metric = "indicador",
  year   = SOURCE_YEARS,
  quiet  = FALSE
) |>
  janitor::clean_names() |>
  mutate(stage = "anos_finais")

# Empilhando
ideb_raw <- bind_rows(
  ideb_iniciais_raw,
  ideb_finais_raw
)

# Raw data
write_rds(
  ideb_raw,
  file.path(OUT_DIR, "ideb_raw_municipio_2011_2015_2019.rds")
)

# ------------------------------------------------------------------------------
# IDEB Data Cleaning -----------------------------------------------------------
# ------------------------------------------------------------------------------
ideb_clean <- ideb_raw |>
  mutate(
    id_municipio = clean_id_municipio(municipio_codigo),
    ano_fonte = as.integer(ano),
    rede = str_to_lower(as.character(rede)),
    indicador = str_to_lower(as.character(indicador)),
    valor = as.numeric(valor)
  ) |>
  filter(
    ano_fonte %in% SOURCE_YEARS,
    indicador == "ideb"
  )

# ------------------------------------------------------------------------------
# Building municipality-year education panel -----------------------------------
# ------------------------------------------------------------------------------
# We keep:
#   - municipal network IDEB where available
#   - public network IDEB where available
#   - all-network / total IDEB where available
# ------------------------------------------------------------------------------
ideb_panel <- ideb_clean |>
  mutate(
    rede_clean = case_when(
      str_detect(rede, "municipal") ~ "municipal",
      str_detect(rede, "publica|pública") ~ "publica",
      str_detect(rede, "estadual") ~ "estadual",
      str_detect(rede, "total") ~ "total",
      TRUE ~ rede
    ),
    var_name = paste0("ideb_", stage, "_", rede_clean)
  ) |>
  group_by(ano_fonte, id_municipio, var_name) |>
  summarise(
    value = mean(valor, na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_wider(
    names_from = var_name,
    values_from = value
  ) |>
  left_join(year_map, by = "ano_fonte") |>
  select(
    ano_eleicao,
    ano_fonte,
    id_municipio,
    everything()
  )

# ------------------------------------------------------------------------------
# Education controls -----------------------------------------------------------
# ------------------------------------------------------------------------------
required_ideb_cols <- c(
  "ideb_anos_iniciais_municipal",
  "ideb_anos_iniciais_publica",
  "ideb_anos_iniciais_total",
  "ideb_anos_finais_municipal",
  "ideb_anos_finais_publica",
  "ideb_anos_finais_total"
)

for (v in required_ideb_cols) {
  if (!v %in% names(ideb_panel)) {
    ideb_panel[[v]] <- NA_real_
  }
}

ideb_panel <- ideb_panel |>
  mutate(
    # Main preferred IDEB variable:
    # 1. municipal network, early elementary
    # 2. public network, early elementary
    # 3. total network, early elementary
    ideb_main = coalesce(
      ideb_anos_iniciais_municipal,
      ideb_anos_iniciais_publica,
      ideb_anos_iniciais_total
    ),
    
    # Secondary / robustness IDEB variable:
    # late elementary, same fallback logic.
    ideb_finais_main = coalesce(
      ideb_anos_finais_municipal,
      ideb_anos_finais_publica,
      ideb_anos_finais_total
    ),
    
    z_ideb_main = zscore(ideb_main),
    z_ideb_finais_main = zscore(ideb_finais_main),
    z_ideb_anos_iniciais_municipal = zscore(ideb_anos_iniciais_municipal),
    z_ideb_anos_finais_municipal = zscore(ideb_anos_finais_municipal)
  )

# ------------------------------------------------------------------------------
# Join -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
muni_year_panel_final <- muni_year_panel_fiscal |>
  mutate(
    id_municipio = clean_id_municipio(id_municipio)
  ) |>
  left_join(
    ideb_panel,
    by = c("ano_eleicao", 
           "ano_fonte", 
           "id_municipio")
  )

# ------------------------------------------------------------------------------
# Diagnostics/Checks -----------------------------------------------------------
# ------------------------------------------------------------------------------
ideb_diagnostics_by_year <- muni_year_panel_final |>
  group_by(ano_eleicao, ano_fonte) |>
  summarise(
    n_rows = n(),
    n_municipalities = n_distinct(id_municipio),
    missing_ideb_main = sum(is.na(ideb_main)),
    missing_ideb_finais_main = sum(is.na(ideb_finais_main)),
    mean_ideb_main = mean(ideb_main, na.rm = TRUE),
    median_ideb_main = median(ideb_main, na.rm = TRUE),
    min_ideb_main = min(ideb_main, na.rm = TRUE),
    max_ideb_main = max(ideb_main, na.rm = TRUE),
    mean_ideb_finais_main = mean(ideb_finais_main, na.rm = TRUE),
    median_ideb_finais_main = median(ideb_finais_main, na.rm = TRUE),
    .groups = "drop"
  )

ideb_diagnostics_overall <- muni_year_panel_final |>
  summarise(
    n_rows = n(),
    n_municipalities = n_distinct(id_municipio),
    years = n_distinct(ano_eleicao),
    missing_ideb_main = sum(is.na(ideb_main)),
    missing_ideb_finais_main = sum(is.na(ideb_finais_main)),
    mean_ideb_main = mean(ideb_main, na.rm = TRUE),
    median_ideb_main = median(ideb_main, na.rm = TRUE),
    min_ideb_main = min(ideb_main, na.rm = TRUE),
    max_ideb_main = max(ideb_main, na.rm = TRUE),
    mean_ideb_finais_main = mean(ideb_finais_main, na.rm = TRUE),
    median_ideb_finais_main = median(ideb_finais_main, na.rm = TRUE)
  )

print(ideb_diagnostics_by_year)
print(ideb_diagnostics_overall)

# ------------------------------------------------------------------------------
# Saving cleaned data ----------------------------------------------------------
# ------------------------------------------------------------------------------
write_rds(
  ideb_panel,
  file.path(OUT_DIR, "ideb_panel_2011_2015_2019.rds")
)

write_csv(
  ideb_panel,
  file.path(OUT_DIR, "ideb_panel_2011_2015_2019.csv")
)

write_rds(
  muni_year_panel_final,
  file.path(OUT_DIR, "municipality_year_FINAL_ibge_siconfi_ideb_2012_2016_2020.rds")
)

write_csv(
  muni_year_panel_final,
  file.path(OUT_DIR, "municipality_year_FINAL_ibge_siconfi_ideb_2012_2016_2020.csv")
