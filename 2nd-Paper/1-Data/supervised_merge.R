# Author: Cedric Antunes (FGV-CEPESP) ------------------------------------------
# Date: April 8, 2026 ----------------------------------------------------------

# Cleaning my environment
rm(list = ls())

# Managing memory
gc()

# Packages --------------------------------------------------------------------- 
suppressPackageStartupMessages({
  library(dplyr)
  library(data.table)
  library(arrow)
  library(sf)
})

# Data loading -----------------------------------------------------------------
master <- readRDS("D:/Users/cedric/Downloads/final_clean_data.rds") |>
  st_drop_geometry()

# Cleaning ---------------------------------------------------------------------
master <- master |>
  select(-VERSAO_CEPESP,
         -TIPO_ELEICAO,
         -CODIGO_ELEICAO,
         -NOME_MESO,
         -NOME_MICRO,
         -COD_SITUACAO_CANDIDATURA,
         -CODIGO_GENERO,
         -CODIGO_COR_RACA,
         -CODIGO_GRAU_INSTRUCAO,
         -CODIGO_ESTADO_CIVIL,
         -CODIGO_NACIONALIDADE,
         -NOME_MUNICIPIO_NASCIMENTO,
         -DESC_SIT_TOT_TURNO.x,
         -GOVERNO.ABERTO,
         -TRANSPARÊNCIA,
         -ACESSO.À.INFORMAÇÃO,
         -REGISTRO.PÚBLICO.REGISTROS.PÚBLICOS,
         -DOCUMENTO.PÚBLICO.DOCUMENTOS.PÚBLICOS,
         -DIREITO.A.SABER,
         -LEI.DE.ACESSO.LAI,
         -AREA,
         -NOME_AC,
         -NOME,
         -AREA_KM2,
         -AREA_OF_19,
         -A_URB_EMBR,
         -RME,
         -RME_SUBDIV,
         -RME_CEM,
         -GR_REGIAO,
         -MESORREG,
         -MICRORREG,
         -RG_GEO_INT,
         -RG_GEO_IME,
         -RG_URB_AMP,
         -RG_URB_INT,
         -RG_URB_IME,
         -BIOMA,
         -BIO_LEGAL,
         -CLIMA_K,
         -BACIA_1,
         -BACIA_2,
         -ALT_MEDIA,
         -ALT_M_CLA,
         -ALT_SEDE,
         -SIGLA_UF.x,
         -REGIÃO.METROPOLITANA,
         -CÓDIGO.CONCENTRAÇÃO.URBANA,
         -NOME.CONCENTRAÇÃO.URBANA,
         -TIPO.CONCENTRAÇÃO.URBANA,
         -CÓDIGO.DA.REGIÃO.RURAL,
         -NOME.DA.REGIÃO.RURAL) |>
  select(-c(98:108)) |>
  mutate(ID_CEPESP = as.character(ID_CEPESP),
         ANO_ELEICAO = as.character(ANO_ELEICAO),
         NUM_TURNO = as.character(NUM_TURNO))

# Data loading -----------------------------------------------------------------
supervised <- read_parquet("D:/Users/cedric/Downloads/df_classified_clean_v1.parquet")

# Cleaning ---------------------------------------------------------------------
supervised <- supervised |>
  select(-NOME_CANDIDATO,
         -DESCRICAO_UE,
         -UF,
         -COD_MUN_TSE,
         -DESCRICAO_GENERO,
         -PDF,
         -DESC_SIT_TOT_TURNO,
         -STATUS_INCUMBENT)

# ------------------------------------------------------------------------------
# Join -------------------------------------------------------------------------
# ------------------------------------------------------------------------------
f_s_clean <- supervised |>
  left_join(master,
            by = c("ID_CEPESP",
                   "ANO_ELEICAO",
                   "COD_MUN_IBGE",
                   "NUM_TURNO"))

# ------------------------------------------------------------------------------
# Audit ------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Checking duplicates ----------------------------------------------------------
dup <- f_s_clean |>
  group_by_all() |>
  filter(n() > 1) |>
  ungroup()

# Dropping duplicates ----------------------------------------------------------
f_s_clean <- f_s_clean |>
  distinct()

# ------------------------------------------------------------------------------
# Saving data ------------------------------------------------------------------
# ------------------------------------------------------------------------------
saveRDS(f_s_clean,
        file = "D:/Transparency/supervised_data_clean.rds")
