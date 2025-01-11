# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: JANUARY 2025

# Cleaning my environment
rm(list = ls())

# Managing local memory
gc()

# Packages ---------------------------------------------------------------------
library(dplyr)
library(sidrar)
library(tidyverse)


# ------------------------------------------------------------------------------
# EXTRACTING THE DATA ----------------------------------------------------------
# ------------------------------------------------------------------------------

# Checking about educational data at the municipality level
info_sidra(9951, 
           wb = TRUE)

# Identifying years for which data is available 
info_9951 <- info_sidra(9951)

# Extracting available years 
available_years <- info_9951$period

# Extracting the data 
get_literacy_data <- function(year) {
  tryCatch({
    message(paste("Fetching data for year:", year))
    sidrar::get_sidra(
      api = paste0("/t/9951/n6/all/v/3597,2513/p/", year, "/c2/6794/c58/95253/c2661/32776")
    )
  }, error = function(e) {
    message(paste("Error retrieving data for year", year, ":", e$message))
    NULL
  })
}

# Storing the data 
literacy_data_list <- lapply(available_years, 
                             get_literacy_data)

# Organizing it into a single dataframe
literacy_data <- do.call(rbind, 
                         literacy_data_list)

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving data as .RDS
saveRDS(literacy_data,
        "literacy_municipalities_2022.RDS")

# Saving data as .csv
write.csv(literacy_data,
          "literacy_municipalities_2022.csv",
          fileEncoding = "latin1")
