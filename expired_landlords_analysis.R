### Analysis for brian's process

llr_joined = import_llr() %>% process_llr() %>% join_llr

ct_merged = llr_joined$ct_merged

### Pre step. How many landlords to we have mapped to current uprns?
### Take our llr_uid from ct_merged and 
### NOTE - this isn't actually unknown - just UPRN's that aren't in finref - need to go back to them with this. 
mapped_llr_uids = ct_merged[!duplicated(ct_merged$llr_uid),]$llr_uid
write.csv(llr_joined$llr %>% filter(!(uid_full %in% mapped_llr_uids) & !(uid_full %in% llr_raw$llr_uprn_new$UIDFULL)) %>% select(uid_full, address_cat, addr_other, flat_position, house_n, house_alpha, street_name, postcode) %>% replace(., is.na(.), ""), paste("../data/export/DW_llr_unknown_", Sys.Date(), ".csv", sep=""), row.names=FALSE)
write.csv(llr_joined$llr %>% filter(!(uid_full %in% mapped_llr_uids) ) %>% select(uid_full, address_cat, addr_other, flat_position, house_n, house_alpha, street_name, postcode) %>% replace(., is.na(.), ""), paste("../data/export/DW_llr_unknown_full_", Sys.Date(), ".csv", sep=""), row.names=FALSE)

### UPRN coverage / linkage between the sets ###

### Landlords registrations that we can't link to CTAX / Benefits via UPRN - 6,510 / 55,753 = 11.7%
llr_joined$llr %>% filter(!(uid_full %in% llr_joined$ct_merged$llr_uid)) %>% nrow()

### Ctax records that we don't have a uprn for - 3499 / 307,422 = 1.1%
llr_joined$ct %>% filter(!(property_ref %in% llr_joined$finref_uprn$FIN_REF)) %>% nrow()
write.csv(llr_joined$ct %>% select(property_ref, address_cat, addr_other, flat_position, house_n, house_alpha, street_name) %>% replace(., is.na(.), "") %>% filter(!(property_ref %in% llr_joined$finref_uprn$FIN_REF)), paste("../data/export/DW_CT_NO_UPRN_", Sys.Date(), ".csv",sep=""), row.names=FALSE)

### Benefits records that we don't have a uprn for - 1,218 / 98,359 = 1.2%
llr_joined$benefits %>% filter(!(property_ref %in% llr_joined$finref_uprn$FIN_REF)) %>% nrow()

### Steps from Alan

## 1. If no owner data OR no schedule data -> tenure = Owner Occupier

## 2. If there is owner or schedule data AND its not HA, Council or MOD -> tenure = Private Landlord

## 3. If Owner name IS liable name -> tenure = Owner occupier UNLESS it has an unoccupied allowance

## 4. If it has an unoccupied allowance -> tenure = Private landlord (reasonable assumption)

## 5. Note- schedule accounts (education / social work) may be occupied by tenants but landlord names liable - because they pay for it

## 6. Tenure from benefits takes priority over all the above


### My steps

### Set up columns
ct_merged$llr_llr_tenure = NA
ct_merged$llr_ct_tenure = NA
ct_merged$llr_hb_tenure = NA
ct_merged$llr_final_tenure = NA

## 1. LLR - Are they on the landlords register? -> private let
ct_merged = ct_merged %>% mutate(llr_llr_tenure = replace(llr_final_tenure, !is.na(LLR_registration_number), "pll"))

## 2. CT unoccupied allowance = y -> PLL - 8109
ct_merged = ct_merged %>% mutate(llr_ct_tenure = ifelse(CT_unoccupied_allowance == "y", "pll", llr_ct_tenure))

## 3. CT Owner name == liable name & unoccupied allowance == "n" -> "OO" - 36,615 total
ct_merged = ct_merged %>% mutate(llr_ct_tenure = ifelse((CT_lead_name == CT_owner_name & CT_unoccupied_allowance == "n"), "oo", llr_ct_tenure))

## 4. CT - no owner or schedule data from CT & no LLR code -> owner occupier - 98,944 (746 clashes with HB tenure field != owner occupier)
ct_merged = ct_merged %>% mutate(llr_ct_tenure = ifelse(is.na(CT_owner_code) & is.na(CT_schedule_code) & is.na(LLR_registration_number) & is.na(CT_owner_name) & is.na(CT_schedule_name), "oo", llr_ct_tenure))

## 5. CT - Use owner and schedule codes tenure, owner codes taking preference - 108,445 owner codes, 12,710 schedule, 3693 overlapping
ct_merged = ct_merged %>% mutate(llr_ct_tenure = ifelse(!is.na(CT_schedule_code), CT_schedule_code_tenure, llr_ct_tenure)) 
ct_merged = ct_merged %>% mutate(llr_ct_tenure = ifelse(!is.na(CT_owner_code), CT_owner_code_tenure, llr_ct_tenure)) 

## 6. HB tenure = Owner occupier -> owner occupier - 97,219 total
hb_list = list("ha" = "ha", "hac"="ha", "owner occupier" = "oo", "private landlord" = "pll", "vo-s"="vo-s")
ct_merged[!is.na(ct_merged$HB_tenure),] = ct_merged %>% filter(!is.na(HB_tenure)) %>% mutate(llr_hb_tenure = as.character(hb_list[HB_tenure]))

## 7. Merge them into final
ct_merged = ct_merged %>% mutate(llr_final_tenure = llr_hb_tenure) %>% mutate(llr_final_tenure = ifelse(is.na(llr_final_tenure), llr_ct_tenure, llr_final_tenure)) %>% mutate(llr_final_tenure = ifelse(is.na(llr_final_tenure), llr_llr_tenure, llr_final_tenure))

### LLR analysis ### 

### Analysis numbers
llr_analysis_description = c("HB tenure PLL, not in LLR", 
                             "final_tenure PLL, not in LLR", 
                             "HB_tenure OO, on LLR", 
                             "HB_tenure PLL, on LLR, status EXPIRED", 
                             "final_tenure OO, on the LLR", 
                             "final_tenure OO, on the LLR and benefits")

llr_analysis_figures = c((ct_merged %>% filter(HB_tenure == "private landlord" & is.na(LLR_registration_number)) %>% nrow()), 
                         (ct_merged %>% filter(llr_final_tenure == "pll" & is.na(LLR_registration_number)) %>% nrow()),
                         (ct_merged %>% filter(HB_tenure == "owner occupier" & !is.na(LLR_registration_number)) %>% nrow()),
                         (ct_merged %>% filter(HB_tenure == "private landlord" & LLR_registration_status %in% c("expired", "refused", "rejected", "revoked")) %>% nrow()),
                         (ct_merged %>% filter(llr_final_tenure == "oo" & !is.na(LLR_registration_number)) %>% nrow()),
                         (ct_merged %>% filter(llr_final_tenure == "oo" & !is.na(LLR_registration_number) & !is.na(HB_claim_id)) %>% nrow())
                         )

llr_analysis_table = data.frame(description = llr_analysis_description, figures = llr_analysis_figures)

### Totals
totals_description = c("HB", "CT", "LLR")
totals_nrow = c(nrow(llr_processed$benefits), 
                nrow(llr_processed$ct), 
                nrow(llr_processed$llr))

totals_uprn_nrow = c((llr_processed$benefits %>% filter(property_ref %in% llr_processed$finref_uprn$FIN_REF) %>% nrow()),
                     (llr_processed$ct %>% filter(property_ref %in% llr_processed$finref_uprn$FIN_REF) %>% nrow()),
                     (llr_joined$ct_merged %>% filter(!is.na(LLR_registration_number)) %>% nrow())
                     )

llr_totals_table = data.frame(dataset= totals_description, total_records=totals_nrow, uprns_held = totals_uprn_nrow) %>% mutate(coverage = round(uprns_held / total_records, 2))


### Tenure split
tenure_analysis = data.frame(table(ct_merged$llr_final_tenure)) %>% rbind(., data.frame(Var1 = "unknown", Freq=sum(is.na(ct_merged$llr_final_tenure)))) %>% rename(tenure=Var1, total=Freq) %>% arrange(desc(total))


