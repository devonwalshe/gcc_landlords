### process LLR files
process_llr = function(llr_raw){
  ### clone llr
  llr_processed = llr_raw
  
  ### Trim and tolower everything
  llr_processed = lapply(llr_processed, function(x) import_string_normalize(x))
  cat("*** All Datasets string normalized \n")
  
  cat("Processing schedule and owner codes \n")
  llr_processed = process_codes(llr_processed)
  
  ### Add new LLR UPRNs from analysis
  llr_processed = add_new_uprns(llr_processed)
  
  ### Individual processing
  cat("*** CT Live Processing \n")
  ct_processed = process_ct_live(llr_processed$ct)
  cat("*** LLR Live Processing \n")
  llr_live_processed = process_llr_live(llr_processed$llr)
  cat("*** LLR UPRN Processing \n")
  llr_uprn_processed = process_llr_uprn(llr_processed$llr_uprn)
  cat("*** LLR UPRN NEW Processing \n")
  benefits_processed = process_benefits_live(llr_processed$benefits)
  cat("*** Finref to UPRN processing\n")
  finref_uprn_processed = process_finref_to_uprn(llr_processed$finref_uprn)
  cat("*** ALL files processed ***\n")
  

  
  ### Return list of processed datasets
  return(list(ct= ct_processed, llr = llr_live_processed, llr_uprn= llr_uprn_processed, benefits= benefits_processed, finref_uprn = finref_uprn_processed))
}


### CT Live set
process_ct_live = function(ct_live){
  ct_live_processed = ct_live
  
  ### Colnames
  colnames(ct_live_processed) = c("property_ref", "lead_name", "street", "addr1", "addr2", "addr3", "addr4", "postcode", "owner_code", "owner_name", "owner_addr1", "owner_addr2", "owner_addr3", "owner_addr4", "owner_postcode", "schedule_code", "schedule_name", "schedule_addr1", "schedule_addr2", "schedule_addr3", "schedule_addr4", "schedule_postcode", "unoccupied_allowance", "disc_type", "allowance", "owner_code_tenure", "schedule_code_tenure")
  
  ### join addresses
  ct_live_processed$address_cat = trimws(gsub("\\w?NA\\w?", "", paste(ct_live_processed$addr1, ct_live_processed$addr2, ct_live_processed$addr3, ct_live_processed$addr4)))
  
  ### Remove city
  city_filter = "glasgow|glagsow|glsgow|glasgoq|gladgow|glagow|gasgow|glasagow|glasggow|glasgiw|glasgoe|glasgw|glashow|glasogw|glasow|glsagow|flasgow"
  city_select = city_filter
  ct_live_processed$street_address = regex_filter_and_extract(ct_live_processed$address_cat, city_filter, city_select)$replace_col
  
  ### House number & street leftover
  flat_position_filter = "(house [0-9\\/\\w\\.]+|flat [0-9\\/\\w\\.]+|site [0-9\\/\\w\\.]+|(flat[\\d]+))|(gr[\\s\\.]?[\\d+]?$)|(rm[\\s]?\\d+?$|room[\\s]?\\d+?$)|(\\d+\\/\\d+)|(\\w+\\/\\w+)|([0-9]+?[a-z]$)|( [\\w+]?[0-9]+$)"
  flat_position_select = flat_position_filter
  flat_cols = regex_filter_and_extract(ct_live_processed$street_address, flat_position_filter, flat_position_select, 1)
  ct_live_processed$flat_position = flat_cols$new_col
  ct_live_processed$street_address = flat_cols$replace_col
  ct_live_processed$street_address = trim_all(ct_live_processed$street_address)
  
  ### reapply to street_address
  ct_live_processed$street_address = regex_filter_and_extract(ct_live_processed$street_address, flat_position_filter, flat_position_select, 1)$replace_col
  
  ### split house_n from street
  street_n_filter = "^([\\w\\s]*?)([\\d]+[a-zA-Z]{1}|[\\d]+? [a-zA-Z]{1}|[\\d]{1,4}) ([^0-9]*)$"
  street_n_select = street_n_filter
  # street_n_cols = regex_filter_and_extract(ct_live_processed$street_address, street_n_filter, street_n_select, 3)
  street_n_cols = str_match(ct_live_processed$street_address, street_n_filter)
  
  ### Any in the match that aren't na
  street_match_rows = which(!is.na(street_n_cols[,1]))
  
  ### Add street address and house number & leftover to data frame
  ct_live_processed[,c("street_name", "house_n", "addr_other", "house_alpha")] = NA
  ct_live_processed[street_match_rows,]$street_name = street_n_cols[street_match_rows,4]
  ct_live_processed[street_match_rows,]$house_n = street_n_cols[street_match_rows, 3]
  ct_live_processed[street_match_rows,]$addr_other = street_n_cols[street_match_rows, 2]
  ct_live_processed[street_match_rows,]$addr_other = street_n_cols[street_match_rows, 2]

  ### Split house_n alpha
  house_alpha_filter = "^([0-9]+)([\\s]?)([a-z]?)$"
  house_alpha_cols = str_match(ct_live_processed[,]$house_n, "([0-9]+)([\\s]?)([a-z]?)$")
  ct_live_processed[,c("house_n", "house_alpha")] = house_alpha_cols[,c(2,4)]
  
  ### owner address cat
  ct_live_processed$owner_address_cat = trimws(gsub("\\w?NA\\w?", "", paste(ct_live_processed$owner_addr1, ct_live_processed$owner_addr2, ct_live_processed$owner_addr3, ct_live_processed$owner_addr4)))
  
  ### Schedule address cat
  ct_live_processed$schedule_address_cat = trimws(gsub("\\w?NA\\w?", "", paste(ct_live_processed$schedule_addr1, ct_live_processed$schedule_addr2, ct_live_processed$schedule_addr3, ct_live_processed$schedule_addr4)))
  
  ### reorder
  ct_live_processed = ct_live_processed[,c("property_ref", "lead_name", "address_cat", "addr_other", "flat_position", "house_n", "house_alpha", "street_name", "postcode", "owner_code", "owner_name", "owner_address_cat", "owner_postcode", "schedule_code", "schedule_name", "schedule_address_cat", "schedule_postcode", "unoccupied_allowance", "disc_type", "allowance", "owner_code_tenure", "schedule_code_tenure")]
  
  return(ct_live_processed)
}

### LLR Live database
process_llr_live = function(llr_live){
  llr_live_processed = llr_live
  colnames(llr_live_processed) = c("registration_number", "registration_status", "renewal_status", "id", "prop_number", "address1", "address2", "address3", "town", "postcode")
  
  ### join addresses
  llr_live_processed$address_cat = trimws(gsub("\\w?NA\\w?", "", paste(llr_live_processed$address1, llr_live_processed$address2, llr_live_processed$address3)))
  
  ### Remove city
  city_filter = "glasgow|glagsow|glsgow|glasgoq|gladgow|glagow|gasgow|glasagow|glasggow|glasgiw|glasgoe|glasgw|glashow|glasogw|glasow|glsagow|flasgow"
  city_select = city_filter
  llr_live_processed$street_address = regex_filter_and_extract(llr_live_processed$address_cat, city_filter, city_select)$replace_col
  
  ### House number & street leftover
  flat_position_filter = "(house [0-9\\/\\w\\.]+|flat [0-9\\/\\w\\.]+|site [0-9\\/\\w\\.]+|(flat[\\d]+))|(gr[\\s\\.]?[\\d+]?$)|(rm[\\s]?\\d+?$|room[\\s]?\\d+?$)|(\\d+\\/\\d+)|(\\w+\\/\\w+)|([0-9]+?[a-z]$)|( [\\w+]?[0-9]+$)"
  flat_position_select = flat_position_filter
  flat_cols = regex_filter_and_extract(llr_live_processed$street_address, flat_position_filter, flat_position_select, 1)
  llr_live_processed$flat_position = flat_cols$new_col
  llr_live_processed$street_address = flat_cols$replace_col
  llr_live_processed$street_address = trim_all(llr_live_processed$street_address)
  
  ### reapply to address cat
  llr_live_processed$street_address = regex_filter_and_extract(llr_live_processed$street_address, flat_position_filter, flat_position_select, 1)$replace_col
  
  ### split house_n from street
  street_n_filter = "^([\\w\\s]*?)([\\d]+[a-zA-Z]{1}|[\\d]+? [a-zA-Z]{1}|[\\d]{1,4}) ([^0-9]*)$"
  street_n_select = street_n_filter
  # street_n_cols = regex_filter_and_extract(llr_live_processed$street_address, street_n_filter, street_n_select, 3)
  street_n_cols = str_match(llr_live_processed$street_address, street_n_filter)
  
  ### Any in the match that aren't na
  street_match_rows = which(!is.na(street_n_cols[,1]))
  
  ### Add street address and house number & leftover to data frame
  llr_live_processed[,c("street_name", "house_n", "addr_other", "house_alpha")] = NA
  llr_live_processed[street_match_rows,]$street_name = street_n_cols[street_match_rows,4]
  llr_live_processed[street_match_rows,]$house_n = street_n_cols[street_match_rows, 3]
  llr_live_processed[street_match_rows,]$addr_other = street_n_cols[street_match_rows, 2]
  
  ### Split house_n alpha
  house_alpha_filter = "^([0-9]+)([\\s]?)([a-z]?)$"
  house_alpha_cols = str_match(llr_live_processed[,]$house_n, "([0-9]+)([\\s]?)([a-z]?)$")
  llr_live_processed[,c("house_n", "house_alpha")] = house_alpha_cols[,c(2,4)]
  
  ### add flat in front of any flat_positions without a word before the slashes
  naked_flat_n_rows = which(!is.na(str_match(llr_live_processed$flat_position, "^[\\w]+?[/][\\w]+?$")))
  llr_live_processed[naked_flat_n_rows,"flat_position"] = paste("flat", llr_live_processed[naked_flat_n_rows, "flat_position"])
  
  ### Add UIDs for registration number
  llr_live_processed$uid_full = paste(llr_live_processed$registration_number, llr_live_processed$id)
  llr_live_processed$uid_truncated = paste(gsub("\\/\\d+$", "", llr_live_processed$registration_number), llr_live_processed$id)
  
  ### reorder the set
  llr_live_processed = llr_live_processed[, c("uid_full", "uid_truncated", "registration_number", "registration_status", "id", "renewal_status", "address_cat", "addr_other", "flat_position", "house_n", "house_alpha", "street_name", "postcode")]
  
  return(llr_live_processed)
}

### LLR with UPRNS
process_llr_uprn = function(llr_uprn){
  llr_uprn_processed = llr_uprn
  
  ### remove unneeded cols -> Taken out a lot of the finance ones and the matching criteria from address base. 
  llr_uprn_processed = llr_uprn_processed %>% select(-match(c("OID_"), names(llr_uprn_processed)))
  
  ### colnames
  colnames = c("registration_number", "registration_status", "id", "address1", "address2", "address3", "town", "postcode", "uprn_2016", "uprn_2017")
  colnames(llr_uprn_processed) = colnames
  
  ### convert uprn_2017 to string
  llr_uprn_processed$uprn_2017 = as.character(llr_uprn_processed$uprn_2017)
  
  ### Add uid
  llr_uprn_processed$llr_uid_full = paste(llr_uprn_processed$registration_number, llr_uprn_processed$id)
  llr_uprn_processed$llr_uid_truncated = paste(gsub("\\/\\d+$", "", llr_uprn_processed$registration_number), llr_uprn_processed$id)
  
  
  return(llr_uprn_processed)
}


### Benefits processing
process_benefits_live = function(benefits_live){
  benefits_processed = benefits_live
  
  ### join addresses
  benefits_processed$address_cat = trim_all(gsub("\\w?NA\\w?", "", paste(benefits_live$addr1, benefits_live$addr2, benefits_live$addr3, benefits_live$addr4)))
  
  ### Remove city
  city_filter = "glasgow|glagsow|glsgow|glasgoq|gladgow|glagow|gasgow|glasagow|glasggow|glasgiw|glasgoe|glasgw|glashow|glasogw|glasow|glsagow|flasgow"
  city_select = city_filter
  benefits_processed$street_address = regex_filter_and_extract(benefits_processed$address_cat, city_filter, city_select)$replace_col
  
  ### House number & street leftover
  house_filter = "(house [0-9\\/\\w\\.]+|flat [0-9\\/\\w\\.]+|site [0-9\\/\\w\\.]+|(flat[\\d]+))|(gr[\\s\\.]?[\\d+]?$)|(rm[\\s]?\\d+?$|room[\\s]?\\d+?$)|(\\d+\\/\\d+)|(\\w+\\/\\w+)|([0-9]+?[a-z]$)|( [\\w+]?[0-9]+$)"
  house_select = house_filter
  house_cols = regex_filter_and_extract(benefits_processed$street_address, house_filter, house_select, 1)
  benefits_processed$house_n = house_cols$new_col
  benefits_processed$street_address = house_cols$replace_col
  benefits_processed$street_address = trim_all(benefits_processed$street_address)
  
  ### reapply to address cat
  benefits_processed$street_address = regex_filter_and_extract(benefits_processed$street_address, house_filter, house_select, 1)$replace_col
  
  return(benefits_processed)
}

### LLR with UPRN's from Brian
process_llr_uprn_brian = function(llr_uprn_brian){
  llr_uprn_brian_processed = llr_uprn_brian
  
  ### colnames
  colnames(llr_uprn_brian_processed) = c("registration_number", "id", "address_1", "address_2", "address_3", "town", "postcode", "deleted", "2012_uprn", "2012_bc", "2013_uprn", "2013_bc", "all_matches", "easting", "northing", "ward", "ward_name", "hn", "hn_name")
  
  ### subset
  llr_uprn_brian_processed = llr_uprn_brian_processed %>% select(registration_number, id, address_1, address_2, address_3, town, postcode, deleted, `2012_uprn`, `2012_bc`, `2013_uprn`, `2013_bc`, all_matches)
  
  ### add id
  llr_uprn_brian_processed$uid_truncated = paste(llr_uprn_brian_processed$registration_number, llr_uprn_brian_processed$id)
  
  return(llr_uprn_brian_processed)
}

### CT to UPRN lookup from alan
process_finref_to_uprn = function(finref_to_uprn){
  finref_to_uprn_processed = finref_to_uprn
  
  
  return(finref_to_uprn_processed)
}

process_codes = function(llr_processed){
  ### Add schedule and owner tenure to ct
  
  ### Owner code tenure
  llr_processed$ct$owner_code_tenure = llr_processed$owner_codes$Tenure[match(llr_processed$ct$Owner.Code, llr_processed$owner_codes$Owner.Code)]
  
  ### Schedule code tenure 
  llr_processed$ct$schedule_code_tenure = llr_processed$schedule_codes$Tenure[match(llr_processed$ct$Schedule.Code, llr_processed$schedule_codes$Code)]
  
  ## Return 
  return(llr_processed)
}

add_new_uprns = function(llr_processed){
  ### Work on new set
  
  ### LLR
  
  ### Seperate id column and reduce columns by those in llr_uprn
  llr_uprn_new = llr_processed$llr_uprn_new %>% select(match(c("UIDFULL", "REGISTRATIONNUMBER", "NPLG_UPRN"), names(llr_processed$llr_uprn_new))) %>% mutate(id=str_match(UIDFULL,"\\d+$"))

  ### Add columns to merge
  llr_uprn_new[,c("OID_", "Name", "Address1", "Address2", "Address3", "Town", "Postcode", "UPRN_2016")] = NA
  colnames(llr_uprn_new)[2:3] = c("Registrati", "UPRN2017")
  llr_uprn_new = llr_uprn_new[,colnames(llr_processed$llr_uprn)]
  
  ### Rbind them
  llr_processed$llr_uprn = rbind(llr_processed$llr_uprn, llr_uprn_new)
  
  ### CT
  
  ### change references to char
  llr_processed$ct_uprn_new = llr_processed$ct_uprn_new %>% mutate(PROPERTYREF = as.character(PROPERTYREF), NPLG_UPRN=as.character(NPLG_UPRN)) %>% rename(FIN_REF = PROPERTYREF, UPRN = NPLG_UPRN) %>% select(FIN_REF, UPRN)
  
  ### add onto finref_uprn
  llr_processed$finref_uprn = rbind(llr_processed$finref_uprn, llr_processed$ct_uprn_new)
  
  return(llr_processed)
  
}
