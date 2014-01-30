source("getCompanyInfo.r")
source("getUniverse.r")
source("logger.r")
library(RSQLite)

db <- "/home/honda/sqlite-db/mini.sqlite"
conn <<- dbConnect( SQLite(), db )
print(paste("Opened SQLite database:", db))

universe <- c("GOOG", "JP3902400005", "00105510", "004561")

logger.init(level=logger.DEBUG,
            do_stdout=TRUE)

r <- getUniverse(conn, 2013, 300)
r1 <- getCompanyInfo(conn, universe)
r2 <- getAvailableFQLs(conn, universe[3])
r3 <- getTSeries(conn, universe[2], "FF_ASSETS", "2010-01-01", "2013-12-31")
r4 <- getBulkTSeries(conn, universe, "FF_ASSETS", "2010-01-01", "2013-12-31")


dbDisconnect(conn)
