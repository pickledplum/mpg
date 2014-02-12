tryCatch({

    library(tools)
    library(xts)
    
    source("read_config.r")
    source("logger.r")
    source("assert.r")
    source("is.empty.r")
    source("drop_tables.r")
    source("create_year_summary.r")
    source("initTseriesDb.r")
    source("createFreqTable.r")
    source("createFqlTable.r")
    source("createCategoryTable.r")
    source("createCountryCompanyTables.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)    
#####################################
# Constants
#####################################
tag <- "test-frontier"
dbdir <- file.path("/home/honda/sqlite-db", tag)
if( !file.exists(dbdir) ){
    dir.create(dbdir)
}
wkdir <- dbdir
if( !file.exists(wkdir) ){
    dir.create(dbdir)
}
dbname <- paste(tag, ".sqlite", sep="")
dbpath <- file.path(dbdir, dbname)
config_file <- paste("/home/honda/mpg/", tag, ".conf", sep="")

logfile_name <- paste(tag, ".log", sep="")
do_stdout <- TRUE
#####################################
# Start...
#####################################
started <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")

#####################################
# Load config
#####################################
config <- read_config(config_file) # returns an environment
print(paste("Loaded config:", config_file))
#####################################
# Set up working directory
#####################################
if( !file.exists(wkdir) ){
    print(paste("Directory does not exist.  Creating...:", wkdir))
    dir.create(wkdir)
}
stopifnot( file.exists(wkdir) )

#####################################
# Open Logger
#####################################
logfile <- file.path(wkdir, logfile_name)
logger.init(level=logger.DEBUG,
            do_stdout=do_stdout,
            logfile=logfile)

print(paste("Log file:", logfile))

#####################################
# Open DB
#####################################
meta_conn <- dbConnect( SQLite(), dbpath )
logger.warn(paste("Opened SQLite database:", dbpath))

#####################################
# Drop tables
#####################################
#drop_tables(meta_conn, exclude=c("category", "fql", "frequency"))
drop_tables(meta_conn)

#####################################
# Bulk init DB
#####################################
createFreqTable(meta_conn)

stopifnot( exists("FQL_MAP", envir=config) )
fql_map_filename <- get("FQL_MAP", envir=config)
createFqlTable(meta_conn, fql_map_filename)

createCategoryTable(meta_conn)

createCountryCompanyTables(meta_conn, config)

tseries_dbname_list <- initTseriesDb(meta_conn, config)
logger.debug(paste("T-series dbs:", paste(tseries_dbname_list, collapse=",")))

#####################################
# Close DB
#####################################
for( pending_result in dbListResults(meta_conn) ){
    dbClearResult(pending_result)
}
dbDisconnect(meta_conn)
logger.warn("Closed db")

#####################################
# Create WKCap summary table
#####################################
create_year_summary(dbpath, "FF_WKCAP", do_drop=TRUE)


#####################################
# Close Logger
#####################################
logger.warn("Good day!")
logger.close()
#####################################
# Finishing up...
#####################################
finished <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
print(paste("started, finished:", started, "-", finished))

