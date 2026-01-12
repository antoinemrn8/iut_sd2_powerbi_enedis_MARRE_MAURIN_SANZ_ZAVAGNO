library(httr)
library(jsonlite)
library(dplyr)
library(purrr)


maj_dpe <- function(df_dpe = NULL,
                    departements = c("01", "30"),
                    base_url = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines",
                    limit = 1000) {
  
  message("Récupération des données depuis l'API ADEME (filtrage avec `qs`)...")
  
  colonnes <- c(
    "numero_dpe",
    "nom_commune_ban",
    "etiquette_ges",
    "code_departement_ban",
    "annee_construction",
    "besoin_chauffage",
    "type_batiment",
    "type_installation_chauffage",
    "date_etablissement_dpe",
    "code_postal_ban",
    "surface_habitable_logement",
    "hauteur_sous_plafond",
    "etiquette_dpe",
    "coordonnee_cartographique_x_ban",
    "coordonnee_cartographique_y_ban"
  )
  
  get_dep_data <- function(dep_code) {
    message(paste0("📦 Département ", dep_code))
    all_data <- list()
    offset <- 0
    total <- Inf
    
    repeat {
      res <- GET(
        url = base_url,
        query = list(
          qs = paste0("code_departement_ban:", dep_code),
          size = limit,
          from = offset
        ),
        timeout(30)
      )
      
      if (res$status_code != 200) {
        warning(paste0("⚠️ Erreur API pour le département ", dep_code, " (code ", res$status_code, ")"))
        break
      }
      
      json_data <- content(res, as = "text", encoding = "UTF-8")
      parsed <- fromJSON(json_data, flatten = TRUE)
      
      if (length(parsed$results) == 0) break
      if (!is.null(parsed$total)) total <- parsed$total
      
      df_part <- parsed$results %>%
        select(any_of(colonnes))
      
      all_data[[length(all_data) + 1]] <- df_part
      
      offset <- offset + limit
      message(paste0("  ➕ ", nrow(df_part), " lignes chargées (offset ", offset, "/", total, ")"))
      
      if (offset >= total) break
    }
    
    df_dep <- bind_rows(all_data)
    message(paste0("✅ Département ", dep_code, ": ", nrow(df_dep), " lignes téléchargées."))
    return(df_dep)
  }
  
  new_data <- map_dfr(departements, get_dep_data)
  message(paste0("📥 Données totales téléchargées : ", nrow(new_data), " lignes."))
  
  if (is.null(df_dpe)) {
    message("📄 Aucun dataframe existant : création du dataframe initial.")
    df_final <- new_data
  } else {
    message("🔍 Vérification des DPE manquants...")
    nouveaux <- setdiff(new_data$numero_dpe, df_dpe$numero_dpe)
    message(paste0("🆕 ", length(nouveaux), " nouveaux DPE à ajouter."))
    if (length(nouveaux) > 0) {
      df_ajout <- new_data %>% filter(numero_dpe %in% nouveaux)
      df_final <- bind_rows(df_dpe, df_ajout)
    } else {
      message("✅ Aucun nouveau DPE à ajouter.")
      df_final <- df_dpe
    }
  }
  
  message(paste0("📊 Taille finale du dataframe : ", nrow(df_final), " lignes."))
  return(df_final)
}

#df_dpe <- maj_dpe() - Pour lancer la requete initiale.

# df_dpe <- maj_dpe(df_dpe) - Met à jour le dataframe existant en ajoutant les nouveaux DPE


write.csv(df_dpe_final, "D:/Pro/IUT/R/Projet_Rshiny/df_dpe2.csv", row.names = FALSE, fileEncoding = "UTF-8")

