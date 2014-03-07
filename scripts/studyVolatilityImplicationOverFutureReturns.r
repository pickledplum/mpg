##############################################################################
#'
#' Download time series from DB and derive another
#' 
#' You need the time series database.
#' You need to know which factors are needed to drive the one you want.
#'
##############################################################################
library(RSQLite)
library(xts)

source("logger.r")
source("seeWhatHappens.r")
source("computeVolatility.r")

##############################################################################
# Configuration - things you want to change
##############################################################################

logger.init(logger.DEBUG)

# DB connection
dbdir <- "R:/temp/honda/sqlite-db/japan500-keep"
dbname <- "japan500.sqlite"

# Consolidate series that make up the control variable, and store as a table
# in the database.  Do this once, and set to FALSE for the subsequent 
# executions of this script to only read off data from the tables.  
do_create_tables <- FALSE 

# Similar to the above but for the daily returns.  
# This can take a loooong time, so do this once and only once.
do_create_return_table <- FALSE

# Time period
begin <- "1980-01-01"
end <- "2013-12-31"

# Define a universe
mkval_min <- -100000
mkval_year <- 2013

# Base factors to drive what you want
factors <- c("P_PRICE_AVG")


# future periods, in month, to project onto
periods <- c(1,3,6,12)

# Number of bins.  e.g. 5 for quintilizing
nbins <- 5

##############################################################################
# Open DB
##############################################################################
conn <- dbConnect(SQLite(), file.path(dbdir, dbname))
logger.info(paste("Opened DB:", file.path(dbdir, dbname)))

##############################################################################
# Define universe
##############################################################################
# Get the universe

if(do_create_tables || do_create_return_table){
    source("../dummy/getUniverse.r")
    source("../dummy/getTSeries.r")
    universe <- getUniverse(conn, mktval=mkval_min, year=mkval_year)$id
    universe <- tail(universe)
    logger.info(paste("Filtered universe by market value >=", mkval_min, "as of", mkval_year)) 
}

##############################################################################
# Download returns
##############################################################################
#
# Consolidate time series into a single big time table 
# where rows=:this_year_months, cols=:companies
#
totalR <- xts()
if( do_create_return_table ){
    totalR <- getBulkTSeries(conn, dbdir, universe, "bulk_P_TOTAL_RETURNC", begin, end)
} else {  
    data <- dbReadTable(conn, "bulk_P_TOTAL_RETURNC")
    
    totalR <- as.xts(data, as.Date(unjulianday(data[[1]])))
    totalR$date = NULL
    totalR <- totalR / 100.  # convert to fraction from percentage
}

##############################################################################
# Prepare the control variable - do it yourself!
##############################################################################
oldest_date <- min(index(totalR))
latest_date <- max(index(totalR)) 

oldest_yy <- as.integer(substr(as.character(oldest_date),1,4))
latest_yy <- as.integer(substr(as.character(latest_date),1,4))
oldest_mm <- as.integer(substr(as.character(oldest_date),6,7))
latest_mm <- as.integer(substr(as.character(latest_date),6,7))

sampling_points <- vector("character", (latest_yy-oldest_yy)*12)
# create a sequence of year-month
cnt=1
for( i in 0:(latest_yy-oldest_yy) ){
    year = oldest_yy + i
    for( month in 1:12 ){
        sampling_points[cnt] <- paste(formatC(year,width=4, flag="0"),
                                      formatC(month, width=2, flag="0"),
                                      sep="-")
        cnt = cnt+1
    }
}

# compute the control variable
volatility <- computeVolatility(sampling_points, totalR)

# create a table for that.
dbSendQuery(conn, "DROP TABLE IF EXISTS derived_VOLATILITY")
dbWriteTable(conn, "derived_VOLATILITY", as.data.frame(volatility))
data <- dbReadTable(conn, "derived_VOLATILITY")
control_var <- volatility


##############################################################################
# Close DB
##############################################################################
# Done with the db
dbDisconnect(conn)
logger.info("Closed the db")

##############################################################################
# Filter returns by the companies & dates available from the factor
##############################################################################
companies <- intersect(names(control_var), names(totalR))
timestamps <- as.character(intersect(substr(index(control_var),1,7), substr(index(totalR),1,7)))

totalR <- totalR[,companies]

##############################################################################
# See the effect of the control variable to the future returns
##############################################################################

seeWhatHappens(control_var, totalR, periods, nbins, "Volatility")

logger.info("Completed normally.  Good day!")
logger.close()

