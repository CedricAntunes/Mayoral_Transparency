# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: DECEMBER 2024

# Cleaning my environment 
rm(list = ls())

# Memory management 
gc()

# Packages ---------------------------------------------------------------------
# Load required libraries
library(ribge)
library(dplyr)
library(readxl)
library(ipeadatar)
library(PNADcIBGE)

# Municipal electoral cycles in Brazil 
electoral_years <- c(2012, 2016, 2020, 2024)

# ------------------------------------------------------------------------------
# EXTRACTING THE DATA ----------------------------------------------------------
# ------------------------------------------------------------------------------

# Municipal population data ----------------------------------------------------

# Extracting population data
safe_populacao_municipios <- function(year) {
  tryCatch({
    # Fetch data and add the Year column
    populacao_municipios(year) |>
      # Creating var for electoral year 
      mutate(Ano = year)
  }, error = function(e) {
    message(paste("Error downloading population data for year", year, ":", e$message))
    NULL
  })
}

# Population data
pop_data_list <- lapply(electoral_years, 
                        safe_populacao_municipios)

# Collapsing into a single dataframe
pop_data <- do.call(rbind, 
                    pop_data_list)

# Preparing data for merge 
pop_data <- pop_data |>
  # Renaming vars
  rename(ANO = Ano,
         SIGLA_UF = uf,
         CODIGO_UF = codigo_uf,
         NOME_MUNICIPIO = nome_munic,
         POPULACAO = populacao,
         CODIGO_MUNICIPIO = cod_municipio) |>
  # Dropping unnecessary vars 
  select(-codigo_munic,
         -populacao_str,
         -cod_munic6)


# GDP Municipality data --------------------------------------------------------


# Extracting GDP data for each electoral year 
process_ibge_gdp <- function(year, file_path, sheet_name) {
  tryCatch({
    # Reading each Excel file
    gdp_data <- read_excel(file_path, sheet = sheet_name, guess_max = 10000) |>
      filter(Year == year) |>
      mutate(Year = year) 
    
    return(gdp_data)
  }, error = function(e) {
    message(paste("Error processing GDP data for year", year, ":", e$message))
    NULL
  })
}

# GDP dataframe 
gdp_data <- read_excel("C:/Users/cedric.antunes/Documents/Transparency_Paper/base_de_dados_2010_2021_xlsx/PIB_MUN_2012_2021.xlsx") |>
  # Selecting only municipal electoral cycles 
  filter(Ano %in% c(2012, 2016, 2020)) |>
  # Selecting only relevant vars for analysis
  select(Ano, 
         `Sigla da Unidade da Federação`,
         `Nome da Unidade da Federação`,
         `Código do Município`,
         `Nome do Município`,
         `Região Metropolitana`,
         `Código Concentração Urbana`,
         `Nome Concentração Urbana`,
         `Tipo Concentração Urbana`,
         `Código da Região Rural`,
         `Nome da Região Rural`,
         `Valor adicionado bruto da Agropecuária, \r\na preços correntes\r\n(R$ 1.000)`,
         `Valor adicionado bruto da Indústria,\r\na preços correntes\r\n(R$ 1.000)`,
         `Valor adicionado bruto dos Serviços,\r\na preços correntes \r\n- exceto Administração, defesa, educação e saúde públicas e seguridade social\r\n(R$ 1.000)`,
         `Valor adicionado bruto da Administração, defesa, educação e saúde públicas e seguridade social, \r\na preços correntes\r\n(R$ 1.000)`,
         `Valor adicionado bruto total, \r\na preços correntes\r\n(R$ 1.000)`,
         `Impostos, líquidos de subsídios, sobre produtos, \r\na preços correntes\r\n(R$ 1.000)`,
         `Produto Interno Bruto, \r\na preços correntes\r\n(R$ 1.000)`,
         `Produto Interno Bruto per capita, \r\na preços correntes\r\n(R$ 1,00)`,
         `Atividade com maior valor adicionado bruto`,
         `Atividade com segundo maior valor adicionado bruto`,
         `Atividade com terceiro maior valor adicionado bruto`)

# Preparing data for merge 
gdp_data <- gdp_data |>
  # Renaming vars
  rename(ANO = Ano,
         SIGLA_UF = `Sigla da Unidade da Federação`,
         NOME_UF = `Nome da Unidade da Federação`,
         CODIGO_MUNICIPIO = `Código do Município`,
         NOME_MUNICIPIO = `Nome do Município`) |>
  # Padronizing data structure 
  mutate(CODIGO_MUNICIPIO = as.character(CODIGO_MUNICIPIO)) |>
  select(-NOME_MUNICIPIO)

# ------------------------------------------------------------------------------
# JOIN -------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Performing the join
municipality_demographics <- left_join(pop_data,
                                       gdp_data,
                                       # Key for match
                                       by = c("ANO",
                                              "SIGLA_UF",
                                              "CODIGO_MUNICIPIO"))

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving RDS data
saveRDS(municipality_demographics,
        "municipality_demographics_2012_2022.RDS")

# Saving .csv data
write.csv(municipality_demographics,
          "municipality_demographics_2012_2022.csv",
          sep = ",",
          fileEncoding = "latin1")
