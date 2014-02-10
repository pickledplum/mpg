tryCatch({
    library(tools)
    #library(FactSetOnDemand)
    library(xts)
    #library(RSQLite)
    
    source("read_config.r")
    source("logger.r")
    #source("tryDb.r")
    source("assert.r")
    #source("tryExtract.r")
    source("is.empty.r")
    source("drop_tables.r")
    #source("julianday.r")
    source("create_year_summary.r")
    #source("enQuote.r")
    source("createMetaDb.r")
    #source("initTseriesDb.r")
    
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
db_name <- "frontier"
db <- paste("/home/honda/sqlite-db/", db_name, ".sqlite", sep="")
config_file <- paste("/home/honda/mpg/", db_name, ".conf", sep="")
wkdir <- "/home/honda/sqlite-db"
logfile_name <- paste(db_name, ".log", sep="")
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
conn <<- dbConnect( SQLite(), db )
logger.warn(paste("Opened SQLite database:", db))

#####################################
# Drop tables
#####################################
drop_tables(conn)

#####################################
# Bulk init DB
#####################################
createMetaDb(conn, config)
#initTseriesDb(conn, config)

#####################################
# Bulk init DB
#####################################
#create_year_summary(conn, "FF_WKCAP", do_drop=FALSE)

#####################################
# Close DB
#####################################
dbDisconnect(conn)
logger.warn("Closed db")
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

