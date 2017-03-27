### Init

library(ggplot2)
library(knitr)
library(reshape2)
library(dplyr)
library(stringr)
library(tidyr)
library(RecordLinkage)
library(poster)
#### Local lib
source("functions.R")

#### Local files
source("import.R")
source("process.R")
source("recordlinkage.R")
source("feature_normalize.R")
source("join.R")

### Scipen
options(scipen = 999)
