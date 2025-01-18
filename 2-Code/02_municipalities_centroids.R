# AUTHOR: CEDRIC ANTUNES (FGV-CEPESP)
# DATE: JANURARY 2025

# Cleaning my environment 
rm(list = ls())

# Managing memory
gc()

# Required packages ------------------------------------------------------------
library(sf)
library(dplyr)
library(stringr)


# ------------------------------------------------------------------------------
# LOADING THE DATA -------------------------------------------------------------
# ------------------------------------------------------------------------------

# Loading .shapefile of Brazilian municipalities
municipalities <- st_read("C:/Users/cedri/OneDrive/Desktop/CEMBRMUNA20/CEMbrMUNa20.shp")

# Ensuring the geometry column is valid
municipalities <- st_make_valid(municipalities)

# List of Brazilian state capitals and their respective states 
state_capitals <- data.frame(
  UF_SIGLA = c("AC", "AL", "AP", 
               "AM", "BA", "CE", 
               "DF", "ES", "GO", 
               "MA", "MT", "MS", 
               "MG", "PA", "PB", 
               "PR", "PE", "PI", 
               "RJ", "RN", "RS", 
               "RO", "RR", "SC", 
               "SP", "SE", "TO"),
  NOME = c("RIO BRANCO", "MACEIO", "MACAPA", 
                   "MANAUS", "SALVADOR", "FORTALEZA", 
                   "BRASILIA", "VITORIA", "GOIANIA", 
                   "SAO LUIS", "CUIABA", "CAMPO GRANDE", 
                   "BELO HORIZONTE", "BELEM", "JOAO PESSOA", 
                   "CURITIBA", "RECIFE", "TERESINA", 
                   "RIO DE JANEIRO", "NATAL", "PORTO ALEGRE", 
                   "PORTO VELHO", "BOA VISTA", "FLORIANOPOLIS", 
                   "SAO PAULO", "ARACAJU", "PALMAS")
)

# Preparing the data: cleaning latin characters manually  
municipalities <- municipalities |>
  mutate(
    NAME_MUNI_CLEAN = str_to_upper(NOME) |>
      str_replace_all("[ÁÀÂÃÄ]", "A") |>
      str_replace_all("[ÉÈÊË]", "E") |>
      str_replace_all("[ÍÌÎÏ]", "I") |>
      str_replace_all("[ÓÒÔÕÖ]", "O") |>
      str_replace_all("[ÚÙÛÜ]", "U") |>
      str_replace_all("[Ç]", "C")
  )

# Dichotomous var for whether the municipality is a state capital or not 
municipalities <- municipalities |>
  rowwise() |>
  mutate(
    IS_CAPITAL = ifelse(
      any(NAME_MUNI_CLEAN == state_capitals$NOME & UF_SIGLA == state_capitals$UF_SIGLA),
      1, 0
    )
  ) |>
  ungroup()

# Double-checking capitals: we are good to go!
check_capitals <- municipalities |>
  filter(IS_CAPITAL == 1) |>
  select(UF_SIGLA,
         NOME,
         ANO)

# ------------------------------------------------------------------------------
# Identifying neighbouring towns -----------------------------------------------
# ------------------------------------------------------------------------------

# Identifying neighbours using spatial intersection
neighbors_list <- st_intersects(municipalities, 
                                municipalities)

# Create the variables
municipalities <- municipalities |>
  mutate(
    # Subtracting 1 to exclude the municipality itself
    total_neighbors = sapply(neighbors_list, length) - 1, 
    neighbors_names = sapply(neighbors_list, function(neighbors) {
      # Getting names of neighbors
      neighbors <- neighbors[neighbors != row_number()]
      str_c(municipalities$NOME[neighbors], collapse = ", ")
    })
  )

# ------------------------------------------------------------------------------
# Calculating the distance (in km) to the state capital ------------------------
# ------------------------------------------------------------------------------

# Calculating centroids for municipalities and state capitals 
municipalities <- municipalities |>
  mutate(centroid = st_centroid(geometry))

# Filtering the data for merge
state_capitals_centroids <- municipalities |>
  # Filtering only state capitals 
  filter(IS_CAPITAL == 1) |> 
  # Keeping UF_SIGLA and centroids
  select(UF_SIGLA, capital_centroid = centroid) 

# Droping geometries for merge
state_capitals_centroids <- st_drop_geometry(state_capitals_centroids)

# Join
municipalities <- municipalities |>
  left_join(state_capitals_centroids, 
            by = "UF_SIGLA")

# Calculating the distance to the state capital centroid in km2
municipalities <- municipalities |>
  mutate(
    distance_to_capital_km = as.numeric(
      st_distance(centroid, 
                  capital_centroid, 
                  by_element = TRUE) / 1000
    )
  )
