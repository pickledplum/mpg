source("getCompanyInfo.r")
source("getUniverse.r")
source("logger.r")
library(RSQLite)

db <- "/home/honda/sqlite-db/mini.sqlite"
conn <<- dbConnect( SQLite(), db )
print(paste("Opened SQLite database:", db))

#universe <- c("GOOG", "JP3902400005", "00105510", "004561")

logger.init(level=logger.DEBUG,
            do_stdout=TRUE)

# Get company IDs (FactSet ID) of which working cap > $300M as of year 2013
universe <- getUniverse(conn, 2013, 300)

# Get company info (e.g. name)
r1 <- getCompanyInfo(conn, universe)

# Get FQL values available for a company
r2 <- getAvailableFQLs(conn, universe[3])

# Get the time series between 2010/01/01 and 2013/12/31 for a company.
# The return value is a list of float values.
r3 <- getTSeries(conn, universe[2], "FF_ASSETS", "2010-01-01", "2013-12-31")

# Get the time series between 2010/01/01 and 2013/12/31 for the list of companies.
# It returns a data.frame such that column names are company IDs.  The first column is date.
r4 <- getBulkTSeries(conn, universe, "FF_ASSETS", "2010-01-01", "2013-12-31")

dbDisconnect(conn)
