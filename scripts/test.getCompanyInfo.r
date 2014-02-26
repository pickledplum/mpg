source("getCompanyInfo.r")
source("getUniverse.r")
source("logger.r")
source("assert.r")
source("getTseries.r")
library(xts)
library(RSQLite)


dbdir <- "/home/honda/sqlite-db/developed"
dbname <- "developed.sqlite"
db <- file.path(dbdir, dbname)
conn <- dbConnect( SQLite(), db )
print(paste("Opened SQLite database:", db))

#universe <- c("GOOG", "JP3902400005", "00105510", "004561")

logger.init(level=logger.DEBUG,
            do_stdout=TRUE)

# Get the IDs of companies with working cap > $300M as of year 2013.
universe <- getUniverse(conn, 
                        year=2013, 
                        mktval=0, 
                        market=NULL, 
                        country=NULL, 
                        region=NULL, 
                        sector=NULL, 
                        industry=NULL)


# Get company info (e.g. name)
r1 <- getCompanyInfo(conn, 
                     universe=head(universe$id, 5))
if(is.empty(universe) ){
    stop()
}
if(nrow(universe) < 3) {
    stop()
}
# Get FQL values available for a company
r2 <- getAvailableFQLs(conn, 
                        fsid=universe$id[3])

r22 <- findTablename(conn, fsid=universe$id[2], "FF_WKCAP")

# Gather all the values across universe and store in a new db/table.
# This returns a pair of dbname and tablename
dbinfo <- makeTSeriesTable(conn,
                universe=head(universe$id,5), 
                fql="P_TOTAL_RETURNC", 
                t0="2010-01-01", 
                t1="2013-12-31")

# Get the time series between 2010/01/01 and 2013/12/31 for the list of companies.
# It returns a data.frame such that column names are company IDs.  The first column is date.
#r4 <- getTSeries(conn,
#                  universe=head(universe$id,5), 
#                  fql="P_TOTAL_RETURNC", 
#                  t0="2010-01-01", 
#                  t1="2013-12-31")

dbDisconnect(conn)

