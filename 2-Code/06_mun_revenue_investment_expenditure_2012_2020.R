# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: MARCH 2025

# Cleaning my environment  
rm(list = ls())

# Memory management  
gc()

# Useful packages 
library(tidyverse)
library(dplyr)
library(readr)
library(censobr)

# ------------------------------------------------------------------------------
# EXTRACTING THE DATA ----------------------------------------------------------
# ------------------------------------------------------------------------------

# Loading municiapl data (IPEA-DATA) -------------------------------------------

# Municipal expenditures 
mun_expenditures <- read_delim("C:/Users/cedric.antunes/Documents/Transparency_Paper/despesa_corrente_mun_ipeadata.csv",
                               delim = ";",
                               locale = locale(encoding = "Latin1")) |>
  select(-`2013`,
         -`2014`,
         -`2015`,
         -`2017`,
         -`2018`,
         -`2019`)

# Municipal investment 
mun_investment <- read_delim("C:/Users/cedric.antunes/Documents/Transparency_Paper/municipal_investment_ipeadata.csv",
                             delim = ";",
                             locale = locale(encoding = "Latin1")) |>
  select(-`2013`,
         -`2014`,
         -`2015`,
         -`2017`,
         -`2018`,
         -`2019`) 


# Municiapl revenue
mun_revenue <- read_delim("C:/Users/cedric.antunes/Documents/Transparency_Paper/mun_revenue_ipeadata.csv",
                           delim = ";",
                           locale = locale(encoding = "Latin1")) |>
  select(-`2013`,
         -`2014`,
         -`2015`,
         -`2017`,
         -`2018`,
         -`2019`) 

# ------------------------------------------------------------------------------
# PREPARING THE DATA -----------------------------------------------------------
# ------------------------------------------------------------------------------

# Municipal Expenditures -------------------------------------------------------

# Reshaping the dataframe
mun_expenditures_long <- mun_expenditures |>
  # Selecting only electoral cycles of interest 
  pivot_longer(cols = starts_with("20"),
               # Setting var for electoral cycle 
               names_to = "ANO_ELEICAO", 
               # Setting new column for expenditures
               values_to = "mun_expenditure_per_capita") |>
  # Dropping name of the municipality
  select(-`Munic¡pio`) |>
  # Renaming vars for join
  rename(SIGLA_UF = Sigla,
         COD_MUN_IBGE = `C¢digo`)

# Municipal Investment ---------------------------------------------------------

# Reshape the dataframe
mun_investment_long <- mun_investment |>
  # Selecting only electoral cycles of interest 
  pivot_longer(cols = starts_with("20"),
               # Setting var for electoral cycle 
               names_to = "ANO_ELEICAO", 
               # Setting new column for investment 
               values_to = "mun_investment_per_capita") |>
  # Dropping name of the municipality
  select(-`Munic¡pio`) |>
  # Renaming vars for join
  rename(SIGLA_UF = Sigla,
         COD_MUN_IBGE = `C¢digo`)

# Municipal Total Revenue ------------------------------------------------------

# Reshape the dataframe
mun_revenue_long <- mun_revenue |>
  # Selecting only electoral cycles of interest 
  pivot_longer(cols = starts_with("20"),
               # Setting var for electoral cycle 
               names_to = "ANO_ELEICAO", 
               # Setting new column for expenditures
               values_to = "mun_revenue_per_capita") |>
  # Dropping name of the municipality
  select(-`Munic¡pio`) |>
  # Renaming vars for join
  rename(SIGLA_UF = Sigla,
         COD_MUN_IBGE = `C¢digo`)

# ------------------------------------------------------------------------------
# MERGING THE DATA -------------------------------------------------------------
# ------------------------------------------------------------------------------
mun_revenue_investment_expenditure <- left_join(mun_expenditures_long,
                                                mun_investment_long,
                                                by = c("SIGLA_UF",
                                                       "COD_MUN_IBGE",
                                                       "ANO_ELEICAO"))

mun_revenue_investment_expenditure <- left_join(mun_revenue_investment_expenditure,
                                                mun_revenue_long,
                                                by = c("SIGLA_UF",
                                                       "COD_MUN_IBGE",
                                                       "ANO_ELEICAO"))

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving the data as .csv file 
write.csv(mun_revenue_investment_expenditure,
          "mun_revenue_investment_expenditure_2012_2016.csv",
          fileEncoding = "latin1")
