# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: APRIL 2025

# Cleaning the environment and loading packages
rm(list = ls())

# Useful packages 
library(dplyr)
library(purrr)
library(sf)
library(tidyr)

# ------------------------------------------------------------------------------
# LOADING THE DATA -------------------------------------------------------------
# ------------------------------------------------------------------------------

# Loading candidate-year panel 
final_data <- readRDS("merged_data.rds")  

# Loading shapefile of Brazilian municipalities
municipalities <- st_read("C:/Users/cedric.antunes/Downloads/CEMBRMUNA20/CEMbrMUNa20.shp")

# ------------------------------------------------------------------------------
# PREPARING THE DATA -----------------------------------------------------------
# ------------------------------------------------------------------------------

# Ensuring valid geometries
municipalities <- st_make_valid(municipalities)

# Identifying neighbors based on spatial intersection
neighbors_df <- st_intersects(municipalities, 
                              municipalities)

# Creating summary table: one row per municipality
neighbors_lookup <- tibble(
  # Extracting municipalities indentifies 
  COD_MUN_IBGE = municipalities$GEOCOD_CH,
  neighbors = lapply(seq_along(neighbors_df), function(i) {
    neighbors_i <- neighbors_df[[i]]
    # Subctracting one: dropping the municipality itself, keeping only neighbors
    neighbors_i <- neighbors_i[neighbors_i != i]  
    # Extracting neighbors' codes
    municipalities$GEOCOD_CH[neighbors_i]         
  })
)

# Groupping candidate data to create one row per municipality-year
muni_year <- final_data |>
  group_by(COD_MUN_IBGE, 
           ANO_ELEICAO) |>
  summarise(
    has_transparency = as.integer(any(DUMMY_LAI_LEGISLATION == 1)),
    .groups = "drop"
  )

# ------------------------------------------------------------------------------
# JOIN -------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Adding neighbor info
muni_year <- left_join(muni_year, 
                       neighbors_lookup, 
                       by = "COD_MUN_IBGE")

# ------------------------------------------------------------------------------
# CALCULATION N OF NEIGHBORS WITH TRANSPARENCY LEGISLATION ---------------------
# ------------------------------------------------------------------------------

# Expanding: each row is a municipality-neighbor-year combination
muni_neighbors_long <- muni_year |>
  # Unnesting
  unnest(neighbors) |>
  # Standardizing var labels 
  rename(COD_MUN_NEIGHBOR = neighbors)

# Join again to get transparency info of neighbors
muni_neighbors_long <- muni_neighbors_long |>
  left_join(
    muni_year |> 
      rename(COD_MUN_NEIGHBOR = COD_MUN_IBGE,
             neighbor_has_transparency = has_transparency),
    # Key 
    by = c("COD_MUN_NEIGHBOR", 
           "ANO_ELEICAO")
  )

# Counting number of neighbors with transparency per municipality-year
neighbor_counts <- muni_neighbors_long |>
  group_by(COD_MUN_IBGE, 
           ANO_ELEICAO) |>
  summarise(
    N_NEIGHBORS_TRANSPARENCY = sum(neighbor_has_transparency, 
                                   na.rm = TRUE),
    .groups = "drop"
  )

# ------------------------------------------------------------------------------
# FINAL JOIN -------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Final join! 
final_data <- final_data |>
  left_join(neighbor_counts, by = c("COD_MUN_IBGE", 
                                    "ANO_ELEICAO"))

# ------------------------------------------------------------------------------
# SAVING THE DATA --------------------------------------------------------------
# ------------------------------------------------------------------------------

# Saving .RDS file
saveRDS(final_data, 
        "final_data_neighbors.rds")

# Saving .csv file 
write.csv(final_data,
          "final_data_neighbors.csv",
          fileEncoding = "latin1")
