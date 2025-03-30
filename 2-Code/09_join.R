# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: JANUARY 2025

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

# Loading CEPESP-DATA data 
mayors_votes <- read.csv("final_mayor_votes.csv",
                         fileEncoding = "latin1")

# Loading municipalities geometry data 
municipalities_centroids <- readRDS(
  gzcon(
    url("https://raw.githubusercontent.com/CedricAntunes/Mayoral_Transparency/main/1-Data/municipalities_centroids.rds")))

# Droping geometry
st_drop_geometry(municipalities_centroids)

# Loading municipalities demographics data 
municipalities_demographics <- read.csv("municipality_demographics_2012_2022.csv",
                                        fileEncoding = "latin1")

# Loading deprecated municipal fiscal capacity data 
municipal_debt_fiscal_capacity <- readRDS("municipal_debt_fiscal_capacity.rds")

# Loading parties' ideology data 
parties_ideology <- read.csv("parties_ideology_2012_2020.csv")

# Loading new municipal fiscal capacity data (IPEA-DATA)
mun_revenue_investment_expenditure_2012_2020 <- read.csv("mun_revenue_investment_expenditure_2012_2020.csv")

# Loading MUNIC data 2019-2020
munic_data <- readRDS("final_munic_data.rds") |>
  # Selecting only variables of interest
  select(CodMun,
         MGOV01,
         MGOV011B) |>
  # Renaming vars
  rename(COD_MUN_IBGE = CodMun,
         HAS_LAI_LEGISLATION = MGOV01,
         YEAR_LEGISLATION = MGOV011B) |>
  # Standardizing var structure for join
  mutate(COD_MUN_IBGE = as.character(COD_MUN_IBGE),
         # Creating dummy var for LAI legislation
         DUMMY_LAI_LEGISLATION = ifelse(HAS_LAI_LEGISLATION == "Sim", 1, 0))

# ------------------------------------------------------------------------------
# PREPARING DATA FOR JOIN ------------------------------------------------------
# ------------------------------------------------------------------------------

mayors_votes <- mayors_votes |>
  mutate(ANO_ELEICAO = as.character(ANO_ELEICAO),
         COD_MUN_IBGE = as.character(COD_MUN_IBGE))

municipalities_centroids <- municipalities_centroids |>
  as.data.frame() |>
  rename(COD_MUN_IBGE = GEOCOD_CH) |>
  select(-ID,
         -DATA,
         -GEOCOD,
         -UF_SIGLA,
         -neighbors_names)

municipalities_demographics <- municipalities_demographics |>
  rename(ANO_ELEICAO = ANO,
         COD_MUN_IBGE = CODIGO_MUNICIPIO) |>
  mutate(ANO_ELEICAO = as.character(ANO_ELEICAO),
         COD_MUN_IBGE = as.character(COD_MUN_IBGE))


# Parties' ideology data -------------------------------------------------------
# Creating ANO_ELEICAO variable based on ANO
parties_ideology <- parties_ideology |> 
  mutate(ANO_ELEICAO = case_when(
    ANO == "2013" ~ "2012",
    ANO == "2017" ~ "2016",
    ANO == "2021" ~ "2020",
    TRUE ~ as.character(ANO)  # Keep other values unchanged if needed
  )) |>
  select(-1) |>
  rename(PARTY_AVG_IDEOLOGY_SCORE = Ideology_Score)

# Create a mapping of incorrect to correct acronyms
party_corrections <- c(
  "PMDB" = "MDB",
  "PC DO B" = "PCDOB",
  "PP" = "PP_PPB",
  "REPUBLICANOS" = "REP",
  "CIDADANIA" = "CID"
)

# Standardize SIGLA_PARTIDO in parties_ideology (if necessary)
parties_ideology <- parties_ideology |> 
  mutate(SIGLA_PARTIDO = ifelse(SIGLA_PARTIDO %in% party_corrections, 
                                names(party_corrections)[match(SIGLA_PARTIDO, party_corrections)], 
                                SIGLA_PARTIDO))

# Municipal investment, expenditure, and revenue -------------------------------
mun_revenue_investment_expenditure_2012_2020 <- mun_revenue_investment_expenditure_2012_2020 |>
  # Droppin Excel index
  select(-1) |>
  rename_with(toupper) |>
  mutate(ANO_ELEICAO = as.character(ANO_ELEICAO),
         COD_MUN_IBGE = as.character(COD_MUN_IBGE))

# ------------------------------------------------------------------------------
# PERFORMING THE JOIN ----------------------------------------------------------
# ------------------------------------------------------------------------------

# Merging electoral data: votes
merged_data <- mayors_votes |>
  left_join(municipalities_centroids, 
            by = "COD_MUN_IBGE")

# Merging municipality demographics 
merged_data <- merged_data |>
  left_join(municipalities_demographics,
            by = c("ANO_ELEICAO",
                   "COD_MUN_IBGE"))

# Merging deprecate municipal fiscal capacity data 
merged_data <- merged_data |>
  left_join(municipal_debt_fiscal_capacity,
            by = c("ANO_ELEICAO",
                   "COD_MUN_IBGE")) |>
  # Dropping columns duplicated during joing
  select(-X.x,
         -X.y,
         -NOME_MUNICIPIO.x) |>
  # Renaming vars 
  rename(NOME_CANDIDATO = NOME_CANDIDATO.x,
         NUMERO_CANDIDATO = NUMERO_CANDIDATO.x,
         CPF_CANDIDATO = CPF_CANDIDATO.x) |>
  # Setting as data.frame
  as.data.frame()

# Merging party ideology score per electoral cycle 
merged_data <- merged_data |>
  left_join(parties_ideology,
            by = c("ANO_ELEICAO",
                   "SIGLA_PARTIDO"))

# Merging new fiscal capacity data
merged_data <- merged_data |>
  left_join(mun_revenue_investment_expenditure_2012_2020,
            by = c("ANO_ELEICAO",
                   "SIGLA_UF",
                   "COD_MUN_IBGE"))

# Cleaning final data
merged_data <- merged_data |>
  rename_with(toupper) |>
  rename(NOME_PARTIDO = `NOME_PARTIDO.X`,
         SILGA_UE = SIGLA_UE.X,
         NOME_UF = NOME_UF.Y) |>
  select(-MUNICIPAL_FISCAL_CAPACITY)

# Merging MUNIC data 
merged_data <- merged_data |>
  left_join(munic_data,
            by = "COD_MUN_IBGE")

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------
write.csv(merged_data,
          "merged_data.csv")

saveRDS(merged_data,
        "merged_data.rds")
