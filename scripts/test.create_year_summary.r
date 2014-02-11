library(RSQLite)
source("logger.r")
source("create_year_summary.r")

#####################################
# Constants
#####################################
tag <- "mini"
dbname <- paste(tag, ".sqlite", sep="")
dbdir <- "/home/honda/sqlite-db"

logger.init(level=logger.DEBUG)

create_year_summary(dbdir, dbname, "FF_WKCAP", do_drop=TRUE)
