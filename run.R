### Run ###
source("init.R")

llr_processed = import_files() %>% process_llr()

ct_llr_norm = import_llr() %>% process_llr() %>% normalize_ct_llr()

### Merge based on exact address matches
ct_llr_norm$ct_live_norm = ct_llr_norm$ct_live_norm %>% mutate(address_cat = gsub("NA ", "", paste(house_n, street_address, postcode)))
ct_llr_norm$llr_live_norm = ct_llr_norm$llr_live_norm %>% mutate(address_cat = gsub("NA ", "", paste(house_n, street_address, postcode)))
ct_llr_norm[["exact_matches"]] = merge(ct_llr_norm$ct_live_norm, ct_llr_norm$llr_live_norm, by="address_cat")

### Remove exact_matches from datasets
ct_llr_norm[["ct_live_norm_reduced"]] = ct_llr_norm$ct_live_norm[!(ct_llr_norm$ct_live_norm$uid %in% ct_llr_norm$exact_matches$uid.x),]
ct_llr_norm[["llr_live_norm_reduced"]] = ct_llr_norm$llr_live_norm[!(ct_llr_norm$llr_live_norm$uid %in% ct_llr_norm$exact_matches$uid.y),]

### Record Linkage
ct_llr_pairs = generate_pairs(ct_llr_norm$ct_live_norm_reduced, ct_llr_norm$llr_live_norm_reduced, c("uid"))

### Save the file for recovery later
save_rl_file(ct_llr_norm$pairs, "../data/rl_data/14_12_2016.zip")

ct_llr_classified = classify_pairs(ct_llr_pairs$pairs, lower_threshold=0.58, upper_threshold=.70)

### Get these badboys to disk
write.csv(ahgm_ctr_classified$review, "./data/rl_data/ahgm_ctr_2016-14-12.csv", row.names=FALSE)

### Cut it off at 60
ahgm_ctr_classified$pairs = ahgm_ctr_classified$pairs[ahgm_ctr_classified$pairs$Weight > .60,]

### link with ctr dataset and break down the conditions
rl_ctr_ids = ahgm_ctr_classified$pairs$uid.1
rl_ahgm_ids = ahgm_ctr_classified$pairs$uid.2
