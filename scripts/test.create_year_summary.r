library(RSQLite)
source("logger.r")
source("createYearSummary.r")

#####################################
# Constants
#####################################
tag <- "mini"
dbname <- paste(tag, ".sqlite", sep="")
dbdir <- "/home/honda/sqlite-db"

logger.init(level=logger.DEBUG)

createYearSummary(dbdir, dbname, "FF_WKCAP", do_drop=TRUE)
