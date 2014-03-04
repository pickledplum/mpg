source("getCompanyInfo.r")
source("getUniverse.r")
source("getTseries.r")
source("logger.r")
source("assert.r")
library(xts)
library(RSQLite)
#SELECT factset_id AS id, [2013] AS 'mkval($M)' FROM country JOIN (SELECT factset_id,sector,industry,subind,domicile_country_id AS country_id,exchange_id FROM company JOIN (SELECT * FROM 'yearly_FF_WKCAP' WHERE [2013] >= 0) USING (factset_id)) USING (country_id)


dbdir <- "./japan500"
dbname <- "japan500.sqlite"
db <- file.path(dbdir, dbname)
conn <- dbConnect( SQLite(), db )
print(paste("Opened SQLite database:", db))

#universe <- c("GOOG", "JP3902400005", "00105510", "004561")

logger.init(level=logger.WARN,
            do_stdout=TRUE)

# Get the IDs of companies with working cap > $300M as of year 2013.
universe <- getUniverse(conn, 
                        year=2013, 
                        mktval=300, 
                        market=NULL, 
                        country=NULL, 
                        region=NULL, 
                        sector=NULL, 
                        industry=NULL)


# Get company info 
# 1. "factset_id"   
# 2. "company"      
# 3. "dcountry"     : domicile country
# 4. "dregion"      : domicile region
# 5. "dcurr_id"     : local currency
# 6. "market_id"    : market designation 
# 7. "sector"       
# 8. "industry"    
# 9. "exchange"     
# 10. "exchange_id"  
# 11. "excurr_id"   : trading currency
# 12."excountry_id" : trading country
#
r1 <- getCompanyInfo(conn, 
                     universe=universe$id)
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

# Get the time series between 2010/01/01 and 2013/12/31 for a company.
# The return value is a list of float values.
r3 <- getTSeries(conn, 
                 sub_db=dbConnect(SQLite(), "/home/honda/sqlite-db/japan500/P_TOTAL_RETURNC.sqlite"),
                 fsid=universe$id[2], 
                 fql="P_TOTAL_RETURNC", 
                 t0="2010-01-01", 
                 t1="2013-12-31")

# Get the time series between 2010/01/01 and 2013/12/31 for the list of companies.
# It returns a data.frame such that column names are company IDs.  
# The first column is date.
r4 <- getBulkTSeries(conn, 
                  dbdir=dbdir,
                  universe=universe$id, 
                  fql="P_TOTAL_RETURNC", 
                  t0="2010-01-01", 
                  t1="2013-12-31")

dbDisconnect(conn)
