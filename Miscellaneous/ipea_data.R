# AUTHOR: CEDRIC ANTUNES (FGV-SP)
# DATE: JANURARY 2025

# Cleaning my environment 
rm(list = ls())

# Managing memory
gc()

# Required packages ------------------------------------------------------------
library(sf)
library(dplyr)
library(stringr)
library(ipeadatar)

ipeadatar::search_series(terms = 'municipal', fields = c('name')) |>
  print(n = 50)

ipeadatar::search_series(terms = 'dívida', fields = c('name')) |>
  print(n = 50)

meta <- metadata('RTRKTOM')

teste <- ipeadata('RTRKTOM') |>
  filter(uname == "Municipality" & date %in% c("2012-01-01",
                                               "2016-01-01",
                                               "2020-01-01"))

# Municiapl debt 
municipal_debt <- ipeadata("BM12_DINEMN12")

# Fiscal capacity
municipal_fiscal_capacity <- ipeadata("RIPTUM") |>
  filter(uname == "Municipality" & date %in% c("2012-01-01",
                                               "2016-01-01",
                                               "2020-01-01"))
