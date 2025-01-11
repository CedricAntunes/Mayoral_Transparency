AUTHOR: CEDRIC ANTUNES (FGV-SP)
DATE: JANUARY 2025

# Education and unemployment data using PNADcIBGE
get_pnadc_data <- function(year, vars) {
  tryCatch({
    pnadc_data <- get_pnadc(year = year, quarter = 4, vars = vars)
    return(pnadc_data)
  }, error = function(e) {
    message(paste("Error downloading PNAD data for year", year, ":", e$message))
    NULL
  })
}

# Education variables (adjust as needed for specific indicators)
education_vars <- c("V3001", "V3002", "V3003")

education_data_list <- lapply(electoral_years, function(year) {
  get_pnadc_data(year, education_vars)
})

education_data_list <- lapply(education_data_list, function(x) {
  as.data.frame(x$variables)
})

education_data <- bind_rows(education_data_list) |>
  select(Ano,
         Trimestre,
         UF,
         UPA,
         Estrato,
         V3001,
         V3002,
         V3003)
