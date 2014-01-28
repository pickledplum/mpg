#Create table summarizing the market values by year
tryCatch({
    library(tools)
    library(FactSetOnDemand)
    library(xts)
    library(RSQLite)
    
    source("read_config.r")
    source("logger.r")
    source("tryDb.r")
    source("assert.r")
    source("tryExtract.r")
    source("is.empty.r")
    source("drop_tables.r")
    source("julianday.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)
MAX_TRIALS <- 5

started <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")

config_file <- "/home/honda/mpg/dummy/createdb.conf"
config <- read_config(config_file) # returns an environment

db <- get("DB", envir=config)
stopifnot( exists("WORKING_DIR", envir=config) )
wkdir <- get("WORKING_DIR", mode="character", envir=config) 
if( !file.exists(wkdir) ){
    logger.warn(paste("Directory does not exist.  Creating...:", wkdir))
    dir.create(wkdir)
}
stopifnot( file.exists(wkdir) )

logfile <- file.path(wkdir, paste("dummy", ".log",sep=""))
logger.init(level=logger.INFO,
            do_stdout=TRUE,
            logfile=logfile)

print(paste("Log file:", logfile))
logger.info("Started...")

conn <<- dbConnect( SQLite(), db )
logger.info(paste("Opened SQLite database:", db))

########################################
# Create MARKET_VAL table
########################################
FQL <- "FF_WKCAP"
summary_tableneme <- paste("yearly-", FQL, sep="")

########################################
# Create summary table
########################################
years <- seq(1980, 2014)
yearends <- seq(as.Date("1980-12-31"), as.Date("2014-12-31"), by="year")
years_spec <- paste(enQuote(years), "FLOAT")
specs <- c("factset_id VARCHAR(20)",
           years_spec)

tryCreateTableIfNotExists(conn, summary_tableneme, specs)

########################################
# Collect all the market values by year
########################################
# collect table names from CATALOG table
#SELECT factset_id, tablename, usd, earliest, latest FROM catalog
catalog_ret <- trySelect(conn, 
                       "catalog", 
                        c("factset_id", "tablename", "usd", "earliest", "latest"),
                        c(paste("fql=", enQuote(FQL),sep="")),
                       TRUE,
                       MAX_TRIALS)

julian_yearends <- julianday(yearends)
for( i in seq(1, nrow(catalog_ret)) ){
    attributes <- catalog_ret[i,]
    fsid <- attributes$factset_id
    tablename <- attributes$tablename
    has_value <- attributes$usd
    julianday_earliest_data_on <- attributes$earliest
    julianday_latest_data_on <- attributes$latest

    earliest_date <- unjulianday(julianday_earliest_data_on)
    m <- regexpr("^[0-9]+{4}", earliest_date)
    earliest_year <- as.numeric(regmatches(earliest_date, m))
    earliest_year0101 <- as.Date(paste(earliest_year, "-01-01", sep=""))
    earliest_year1231 <- as.Date(paste(earliest_year, "-12-31", sep=""))
    
    latest_date <- unjulianday(julianday_latest_data_on)
    m <- regexpr("^[0-9]+{4}", latest_date)
    latest_year <- as.numeric(regmatches(latest_date, m))
    latest_year0101 <- as.Date(paste(latest_year, "-01-01", sep=""))
    latest_year1231 <- as.Date(paste(latest_year, "-12-31", sep=""))
    
    julian_yearstarts <- julianday(seq(earliest_year0101, latest_year0101, by="year"))
    julian_yearends <- julianday(seq(earliest_year1231, latest_year1231, by="year"))
    
    q_str <- ""
    for( k in seq(1, length(julian_yearstarts) ) ){
        if( k > 1 ){
            q_str <- paste(q_str, "UNION")
        }
        q0_str <- paste("SELECT date(max(date)) as date, usd",
                        "FROM", enQuote(tablename), 
                        "WHERE date BETWEEN", julian_yearstarts[k], "AND", julian_yearends[k])
        print(q0_str)
        
        q_str <- paste(q_str, q0_str)
    }
    r <- tryGetQuery(conn, q_str)
    
    valid_years <- seq(earliest_year, latest_year, by=1)
    tryInsert(conn, 
              summary_tableneme, 
              cbind("factset_id", valid_years),
              cbind(enQuote(fsid), r$usd)
    )
}

finished <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
print(paste("started, finished:", started, "-", finished))
print(paste("Log file:", logfile))

dbCommit(conn)
dbDisconnect(conn)
logger.info("Closed db")
logger.info("Good day!")
logger.close()