### Import sets

import_llr = function(){

  cat("Importing benefits \n")
  benefits = read.csv("../data/import/benefits_live_2017_02_07.csv", fileEncoding="latin1", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  cat("Importing ctax \n")
  ct = read.csv("../data/import/ctax_live_2017_02_03.csv", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  cat("Importing llr \n")
  llr = read.csv("../data/import/full_landlord_register_2017_02_02.csv", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  cat("Importing llr_uprn \n")
  llr_uprn = read.csv("../data/import/louise_gprop_2017_02_22.csv", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  cat("Importing llr_uprn_new \n")
  llr_uprn_new = read.csv("../data/import/llr_unknown_uprn_matched.csv", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  cat("Importing finref_uprn \n")
  finref_uprn = read.csv("../data/import/academy_uprn_lookup_2017_02_06.csv", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  cat("importing ct_uprn_new \n")
  ct_uprn_new = read.csv("../data/import/CT_unknown_matched.csv", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  
  cat("Importing schedule and owner_codes \n")
  schedule_codes = read.csv("../data/import/schedule_codes.csv", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  owner_codes = read.csv("../data/import/owner_codes.csv", stringsAsFactors = FALSE, strip.white = TRUE, na.strings = c("", "^\\s+$", "N/A", "N / A", "na", "n/a", "n / a"))
  
  return(list(benefits = benefits, ct=ct, ct_uprn_new = ct_uprn_new, llr=llr, llr_uprn=llr_uprn, llr_uprn_new=llr_uprn_new, finref_uprn = finref_uprn, schedule_codes=schedule_codes, owner_codes=owner_codes))
}