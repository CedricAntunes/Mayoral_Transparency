# AUTHOR: Cedric Antunes (FGV-SP)
# DATE: November 2024


# Attaching packages -----------------------------------------------------------
library(httr)   
library(zip)    
library(googledrive)  
library(dplyr) 
library(parquetize)
library(arrow)


# ------------------------------------------------------------------------------
# PREPARING DATA FOR SCRAPE ----------------------------------------------------
# ------------------------------------------------------------------------------

# Authenticating Google Drive
drive_auth()

# Municipal electoral years
years <- seq(2012, 2024, by = 4)

# Increasing timeout
options(timeout = 99999999)

# Brazilian states
states <- c("AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", 
            "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", 
            "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO")

# Base URL
base_url <- "https://cdn.tse.jus.br/estatistica/sead/odsele/proposta_governo/"

# Local path 
local_folder <- "C:/Users/cedric.antunes/Desktop/Paper_Transparency/platforms"

# Shared drive folder
drive_folder <- "Transparency_Paper_Platforms"


# ------------------------------------------------------------------------------
# LOADING CANDIDATES' DATA -----------------------------------------------------
# ------------------------------------------------------------------------------

# Loading candidates data 
candidate_data <- read_parquet("F:/Public/Documents/repositorioTSE/data/output/Candidatos/candidatos_gr_mun_final.parquet") |>
  filter(DESCRICAO_CARGO == "PREFEITO" & ANO_ELEICAO %in% c("2012", "2016", "2020"))

# Ensure that local folder exists
if (!dir.exists(local_folder)) {
  dir.create(local_folder)
}


# ------------------------------------------------------------------------------
# SCRAPING MAYORAL PLEDGES (2012-2024) -----------------------------------------
# ------------------------------------------------------------------------------

# Function to download, rename, and upload
download_and_process <- function(year, state) {
  zip_file_name <- paste0("proposta_governo_", year, "_", state, ".zip")
  zip_url <- paste0(base_url, zip_file_name)
  local_zip_path <- file.path(local_folder, zip_file_name)
  
  message(paste("Processing file:", zip_file_name))
  
  tryCatch({
    # Downloading the zip file
    download.file(zip_url, destfile = local_zip_path, mode = "wb")
    message(paste("Downloaded:", zip_file_name))
  }, error = function(e) {
    message(paste("Failed to download:", zip_file_name, "with error:", e$message))
    return(NULL)
  })
  
  # Unzip the contents
  unzip_dir <- file.path(local_folder, paste0(year, "_", state))
  if (!dir.exists(unzip_dir)) {
    dir.create(unzip_dir)
  }
  unzip(local_zip_path, exdir = unzip_dir)
  
  # Identify PDF files and rename them
  pdf_files <- list.files(unzip_dir, pattern = "\\.pdf$", full.names = TRUE)
  for (pdf in pdf_files) {
    candidate_number <- sub("^.+_(\\d+)\\.pdf$", "\\1", basename(pdf))
    message(paste("Extracted candidate_number:", candidate_number))
    
    # Candidates' information
    candidate_info <- candidate_data |>
      filter(SEQUENCIAL_CANDIDATO == candidate_number, 
             SIGLA_UF == state, 
             ANO_ELEICAO == year)
    
    if (nrow(candidate_info) > 0) {
      new_name <- paste0(state, "_", year, "_", 
                         gsub(" ", "-", candidate_info$DESCRICAO_UE[1]), "_",
                         gsub(" ", "-", candidate_info$NOME_PARTIDO[1]), ".pdf")
      new_path <- file.path(unzip_dir, new_name)
      file.rename(pdf, new_path)
      message(paste("Renamed to:", new_name))
      
      drive_upload(new_path, path = as_id(drive_folder_id))
      message(paste("Uploaded:", new_name))
    } else {
      message(paste("No match found for candidate_number:", candidate_number))
    }
  }
  
  # Removing zip file
  file.remove(local_zip_path)
  message(paste("Removed zip file:", zip_file_name))
}

# Iterating over years and states
for (year in years) {
  for (state in states) {
    download_and_process(year, state)
  }
}

# Print by the end
message("All files processed.")
