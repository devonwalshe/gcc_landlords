### Joining up attributes from datasets
join_llr = function(llr_processed){
  llr_joined = llr_processed
  
  cat("Joining our datasets into one query file\n")
  
  ### Add llr uid to finref lookup
  llr_joined$finref_uprn$llr_uid = llr_joined$llr_uprn$llr_uid_full[match(llr_joined$finref_uprn$UPRN, llr_joined$llr_uprn$uprn_2017)]
  
  ### Add benefits to ct
  llr_joined[["ct_merged"]] = merge(llr_joined$ct, llr_joined$benefits, by="property_ref", all.x=TRUE)
  
  ### Add uprn and prop ref to ct
  llr_joined$ct_merged = merge(llr_joined$ct_merged, llr_joined$finref_uprn, by.x="property_ref", by.y="FIN_REF", all.x=TRUE)
  
  ### add llr to ct
  llr_joined$ct_merged = merge(llr_joined$ct_merged, llr_joined$llr, by.x="llr_uid", by.y="uid_full", all.x=TRUE)
  
  ### rename and reorder columns
  colnames(llr_joined$ct_merged)[3:23] = paste("CT_", colnames(llr_joined$ct_merged)[3:23], sep="")
  colnames(llr_joined$ct_merged)[24:43] = paste("HB_", colnames(llr_joined$ct_merged)[24:43], sep="")
  colnames(llr_joined$ct_merged)[45:56] = paste("LLR_", colnames(llr_joined$ct_merged)[45:56], sep="")  
  
  llr_joined$ct_merged = llr_joined$ct_merged[,c(which(names(llr_joined$ct_merged) %in% c("llr_uid", "uid_truncated", "property_ref", "UPRN")), 
                                                 grep("CT", names(llr_joined$ct_merged)), 
                                                 grep("HB", names(llr_joined$ct_merged)), 
                                                 grep("LLR", names(llr_joined$ct_merged)))]
  
  ### Rename cols to get rid of .x and .y
  names(llr_joined$ct_merged)[grep("\\.[xy]", names(llr_joined$ct_merged))] = gsub("\\.[xy]", "", names(llr_joined$ct_merged)[grep("\\.[xy]", names(llr_joined$ct_merged))])
  
  cat("All files joined - ct_merged added to data list")
  
  return(llr_joined)
}