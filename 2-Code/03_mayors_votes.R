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

# ------------------------------------------------------------------------------
# Loading CEPESPDATA data ------------------------------------------------------
# ------------------------------------------------------------------------------

# Defining electoral years of interest 
years <- c(2012, 2016, 2020)

file_paths <- paste0("F:/Public/Documents/repositorioTSE/data/output/JoinFinal/votacao_secao_coli_cand_", 
                     years, 
                     "_PREFEITO_CEPESPv202410.parquet")

# Empty list for data 
data_list <- list()

# Looping through each data file 
for (file in file_paths) {
  data <- read_parquet(file) |>
    # Converting total quantity of votes to numeric
    mutate(QTDE_VOTOS = as.numeric(QTDE_VOTOS)) 
  
  # Storing the processed data
  data_list[[file]] <- data
}

# Combine all years into a single dataframe
combined_data <- bind_rows(data_list)

# Summing total votes for each candidate in each municipality
# in each electoral year 
final_data <- combined_data |>
  group_by(ANO_ELEICAO,
           UF,
           NUM_TURNO,
           NOME_MUNICIPIO,
           NOME_CANDIDATO) |>
  mutate(QTDE_VOTOS_SUM = sum(QTDE_VOTOS, na.rm = TRUE)) |> 
  slice(1) |> 
  ungroup()  

# Dropping residual ballots: 2012
mayoral_votes_2012 <- final_data |>
  filter(ANO_ELEICAO == "2012") |>
  filter(!NOME_CANDIDATO %in% c("VOTO BRANCO", 
                                "VOTO NULO", 
                                "VOTO ANULADO", 
                                "APURADO EM SEPARADO"))

# Dropping residual ballots: 2016
mayoral_votes_2016 <- final_data |>
  filter(ANO_ELEICAO == "2016") |>
  filter(!NOME_CANDIDATO %in% c("VOTO BRANCO", 
                                "VOTO NULO", 
                                "VOTO ANULADO", 
                                "APURADO EM SEPARADO"))

# Dropping residual ballots: 2020
mayoral_votes_2020 <- final_data |>
  filter(ANO_ELEICAO == "2020") |>
  filter(!NOME_CANDIDATO %in% c("VOTO BRANCO", 
                                "VOTO NULO", 
                                "VOTO ANULADO", 
                                "APURADO EM SEPARADO"))

                 
# ------------------------------------------------------------------------------
# Loading the data prepared by Luana -------------------------------------------
# ------------------------------------------------------------------------------

# Base URL
base_url <- "https://github.com/CedricAntunes/Mayoral_Transparency/blob/main/1-Data/"

# Extracting data URLs
read_excel_from_github <- function(year) {
  url <- paste0(base_url, year, "-lidos.xlsx")
  temp_file <- tempfile(fileext = ".xlsx")
  
  # Raw URL
  raw_url <- sub("github.com", "raw.githubusercontent.com", url) %>% 
    sub("blob/", "", .)
  
  # Downloading each file 
  tryCatch({
    download.file(raw_url, temp_file, mode = "wb")
    read_excel(temp_file)
  }, error = function(e) {
    message("Error downloading or reading the file for year: ", year, 
            ". Skipping this year.")
    NULL
  })
}

# ------------------------------------------------------------------------------
# Reading the data -------------------------------------------------------------
# ------------------------------------------------------------------------------

# Reading each file 
mayors_2012 <- read_excel_from_github(2012)
mayors_2016 <- read_excel_from_github(2016)
mayors_2020 <- read_excel_from_github(2020)


# ------------------------------------------------------------------------------
# JOIN 2012 --------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Preparing data for join: 2012
mayors_2012 <- mayors_2012 |>
  rename(UF = SG_UF,
         SIGLA_UE = SG_UE,
         NUM_TURNO = NR_TURNO,
         NOME_MUNICIPIO = NM_UE, 
         NOME_CANDIDATO = NM_CANDIDATO,
         NUMERO_CANDIDATO = NR_CANDIDATO,
         NOME_PARTIDO = NM_PARTIDO,
         SEQUENCIAL_CANDIDATO = SQ_CANDIDATO,
         CPF_CANDIDATO = NR_CPF_CANDIDATO) |>
  select(UF, 
         SIGLA_UE,
         NUM_TURNO,
         NOME_MUNICIPIO, 
         NOME_CANDIDATO,
         NUMERO_CANDIDATO,
         NOME_PARTIDO,
         SEQUENCIAL_CANDIDATO,
         CPF_CANDIDATO,
         DS_SITUACAO_CANDIDATURA,
         PDF,
         `governo aberto`,
         transparência,
         `acesso à informação`,
         `registro público/registros públicos`,
         `documento público/documentos públicos`,
         `direito a saber`,
         `lei de acesso/lai`) |>
  # Converting vars to the same class before merging
  mutate(NUM_TURNO = as.character(NUM_TURNO),
         NUMERO_CANDIDATO = as.character(NUMERO_CANDIDATO),
         SIGLA_UE = as.character(SIGLA_UE),
         SEQUENCIAL_CANDIDATO = as.character(SEQUENCIAL_CANDIDATO),
         CPF_CANDIDATO = as.character(CPF_CANDIDATO)) |>
  filter(!DS_SITUACAO_CANDIDATURA %in% c("RENÚNCIA", 
                                         "DEFERIDO COM RECURSO",
                                         "INDEFERIDO",
                                         "CANCELADO",
                                         "INDEFERIDO COM RECURSO",
                                         "CASSADO",
                                         "CASSADO COM RECURSO"))

# Performing the join: 2012
joined_df_2012 <- mayoral_votes_2012 |>
  left_join(mayors_2012, 
            by = c("UF",
                   "NUM_TURNO",
                   "SEQUENCIAL_CANDIDATO"))

# Checking consistency of the join
check_erro_2012 <- joined_df_2012 |>
  filter(is.na(PDF)) 

# Estimating (1) vote share of each candidate in each municipality and
# (2) the number of candidates in each municipality 
joined_df_2012 <- joined_df_2012 |>
  group_by(UF, 
           NUM_TURNO,
           COD_MUN_IBGE,
           ANO_ELEICAO) |>
  mutate(
    # Calculating the total of votes in the municipality
    TOTAL_VOTOS_MUNICIPIO = sum(QTDE_VOTOS, na.rm = TRUE),
    # Calculating the total of votes for the candidate in the municipality
    VOTE_SHARE_CANDIDATO = QTDE_VOTOS / TOTAL_VOTOS_MUNICIPIO       
  ) |>
  # Calculating the total number of candidates in the municipality
  mutate(NUM_CANDIDATOS_MUNICIPIO = n_distinct(NOME_CANDIDATO.x)) |>  
  ungroup()  

# ------------------------------------------------------------------------------
# JOIN 2016 --------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Preparing data for join: 2016
mayors_2016 <- mayors_2016 |>
  rename(UF = SG_UF,
         SIGLA_UE = SG_UE,
         NUM_TURNO = NR_TURNO,
         NOME_MUNICIPIO = NM_UE, 
         NOME_CANDIDATO = NM_CANDIDATO,
         NUMERO_CANDIDATO = NR_CANDIDATO,
         NOME_PARTIDO = NM_PARTIDO,
         SEQUENCIAL_CANDIDATO = SQ_CANDIDATO,
         CPF_CANDIDATO = NR_CPF_CANDIDATO) |>
  select(UF, 
         SIGLA_UE,
         NUM_TURNO,
         NOME_MUNICIPIO, 
         NOME_CANDIDATO,
         NUMERO_CANDIDATO,
         NOME_PARTIDO,
         SEQUENCIAL_CANDIDATO,
         CPF_CANDIDATO,
         DS_SITUACAO_CANDIDATURA,
         PDF,
         `governo aberto`,
         transparência,
         `acesso à informação`,
         `registro público/registros públicos`,
         `documento público/documentos públicos`,
         `direito a saber`,
         `lei de acesso/lai`) |>
  # Converting vars to the same class before merging
  mutate(NUM_TURNO = as.character(NUM_TURNO),
         NUMERO_CANDIDATO = as.character(NUMERO_CANDIDATO),
         SIGLA_UE = as.character(SIGLA_UE),
         SEQUENCIAL_CANDIDATO = as.character(SEQUENCIAL_CANDIDATO),
         CPF_CANDIDATO = as.character(CPF_CANDIDATO)) |>
  filter(!DS_SITUACAO_CANDIDATURA %in% c("RENÚNCIA", 
                                         "DEFERIDO COM RECURSO",
                                         "INDEFERIDO",
                                         "CANCELADO",
                                         "INDEFERIDO COM RECURSO",
                                         "CASSADO",
                                         "CASSADO COM RECURSO",
                                         "NA",
                                         "INAPTO"))

# Performing the join: 2016
joined_df_2016 <- mayoral_votes_2016 |>
  left_join(mayors_2016, 
            by = c("UF",
                   "NUM_TURNO",
                   "SEQUENCIAL_CANDIDATO"))

# Checking consistency of the join: 2016
check_erro_2016 <- joined_df_2016 |>
  filter(is.na(PDF))

# Estimating (1) vote share of each candidate in each municipality and
# (2) the number of candidates in each municipality 
joined_df_2016 <- joined_df_2016 |>
  group_by(UF, 
           NUM_TURNO,
           COD_MUN_IBGE,
           ANO_ELEICAO) |>
  mutate(
    # Calculating the total of votes in the municipality
    TOTAL_VOTOS_MUNICIPIO = sum(QTDE_VOTOS, na.rm = TRUE),
    # Calculating the total of votes for the candidate in the municipality
    VOTE_SHARE_CANDIDATO = QTDE_VOTOS / TOTAL_VOTOS_MUNICIPIO       
  ) |>
  # Calculating the total number of candidates in the municipality
  mutate(NUM_CANDIDATOS_MUNICIPIO = n_distinct(NOME_CANDIDATO.x)) |>  
  ungroup() 

# ------------------------------------------------------------------------------
# JOIN 2020 --------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Preparing data for join: 2020
mayors_2020 <- mayors_2020 |>
  rename(UF = SG_UF,
         SIGLA_UE = SG_UE,
         NUM_TURNO = NR_TURNO,
         NOME_MUNICIPIO = NM_UE, 
         NOME_CANDIDATO = NM_CANDIDATO,
         NUMERO_CANDIDATO = NR_CANDIDATO,
         NOME_PARTIDO = NM_PARTIDO,
         SEQUENCIAL_CANDIDATO = SQ_CANDIDATO,
         CPF_CANDIDATO = NR_CPF_CANDIDATO) |>
  select(UF, 
         SIGLA_UE,
         NUM_TURNO,
         NOME_MUNICIPIO, 
         NOME_CANDIDATO,
         NUMERO_CANDIDATO,
         NOME_PARTIDO,
         SEQUENCIAL_CANDIDATO,
         CPF_CANDIDATO,
         DS_SITUACAO_CANDIDATURA,
         PDF,
         `governo aberto`,
         transparência,
         `acesso à informação`,
         `registro público/registros públicos`,
         `documento público/documentos públicos`,
         `direito a saber`,
         `lei de acesso/lai`) |>
  # Converting vars to the same class before merging
  mutate(NUM_TURNO = as.character(NUM_TURNO),
         NUMERO_CANDIDATO = as.character(NUMERO_CANDIDATO),
         SIGLA_UE = as.character(SIGLA_UE),
         SEQUENCIAL_CANDIDATO = as.character(SEQUENCIAL_CANDIDATO),
         CPF_CANDIDATO = as.character(CPF_CANDIDATO)) |>
  filter(!DS_SITUACAO_CANDIDATURA %in% c("RENÚNCIA", 
                                         "DEFERIDO COM RECURSO",
                                         "INDEFERIDO",
                                         "CANCELADO",
                                         "INDEFERIDO COM RECURSO",
                                         "CASSADO",
                                         "CASSADO COM RECURSO",
                                         "NA",
                                         "INAPTO"))

# Performing the join: 2020
joined_df_2020 <- mayoral_votes_2020 |>
  left_join(mayors_2020, 
            by = c("UF",
                   "NUM_TURNO",
                   "SEQUENCIAL_CANDIDATO"))

# Checking consistency of the join: 2020
check_erro_2020 <- joined_df_2020 |>
  filter(is.na(PDF))


# Estimating (1) vote share of each candidate in each municipality and
# (2) the number of candidates in each municipality 
joined_df_2020 <- joined_df_2020 |>
  group_by(UF, 
           NUM_TURNO,
           COD_MUN_IBGE,
           ANO_ELEICAO) |>
  mutate(
    # Calculating the total of votes in the municipality
    TOTAL_VOTOS_MUNICIPIO = sum(QTDE_VOTOS, na.rm = TRUE),
    # Calculating the total of votes for the candidate in the municipality
    VOTE_SHARE_CANDIDATO = QTDE_VOTOS / TOTAL_VOTOS_MUNICIPIO       
  ) |>
  # Calculating the total number of candidates in the municipality
  mutate(NUM_CANDIDATOS_MUNICIPIO = n_distinct(NOME_CANDIDATO.x)) |>  
  ungroup() 

# ------------------------------------------------------------------------------
# POOLING THE DATA -------------------------------------------------------------
# ------------------------------------------------------------------------------

# Pooling the dataframes into a single dataset
final_votes_df <- bind_rows(joined_df_2012, 
                            joined_df_2016, 
                            joined_df_2020) |>
  select(-SIGLA_UE.y,
         -NOME_MUNICIPIO.y,
         -NOME_CANDIDATO.y,
         -NUMERO_CANDIDATO.y,
         -NOME_PARTIDO.y,
         -CPF_CANDIDATO.y) |>
  filter(!is.na(PDF))

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving .RDS data
saveRDS(final_votes_df,
        "final_mayor_votes.rds")

# Saving .csv data
write.csv(final_votes_df,
          "final_mayor_votes.csv",
          fileEncoding = "latin1")
