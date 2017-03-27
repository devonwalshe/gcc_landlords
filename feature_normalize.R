### Feature normalise for data match

normalize_ct_llr = function(llr_processed){
  ct_live_norm = llr_processed$ct_live
  llr_live_norm = llr_processed$llr_live
  
  ### Subset the datasets
  
  ct_live_norm = ct_live_norm %>% select(match(c("property_ref", "house_n", "street_address", "postcode"), names(ct_live_norm)))
  llr_live_norm$uid = paste(llr_live_norm$registration_number, llr_live_norm$id)
  llr_live_norm = llr_live_norm %>% select(match(c("uid", "house_n", "street_address", "postcode"), names(llr_live_norm)))
  colnames(ct_live_norm)[1] = "uid"
  ct_live_norm
  return(list(ct_live_norm=ct_live_norm, llr_live_norm=llr_live_norm))
}