library(RSQLite)
source("logger.r")
source("create_year_summary.r")

#####################################
# Constants
#####################################
tag <- "mini"
dbname <- paste(tag, ".sqlite", sep="")
dbdir <- "/home/honda/sqlite-db"
dbpath <- file.path(dbdir, dbname)

#####################################
# Open Logger
#####################################
logger.init(level=logger.DEBUG)

#####################################
# Open DB
#####################################
conn <- dbConnect( SQLite(), dbpath )
logger.warn(paste("Opened SQLite database:", dbpath))

create_year_summary(conn, dbdir, "FF_WKCAP", do_drop=TRUE)

dbDisconnect(conn)