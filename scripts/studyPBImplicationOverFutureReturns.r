##############################################################################
#'
#' Download time series from DB and derive another
#' 
#' You need the time series database.
#' You need to know which factors are needed to drive the one you want.
#'
##############################################################################
library(RSQLite)
library(psych)
library(xts)

source("../dummy/getUniverse.r")
source("../dummy/getTSeries.r")
source("logger.r")
#source("analyzeFactorTrend.r")
source("seeWhatHappens.r")

##############################################################################
# Configuration - things you want to change
##############################################################################

logger.init(logger.INFO)

# DB connection
dbdir <- "R:/temp/honda/sqlite-db/japan500-keep"
dbname <- "japan500.sqlite"
#dbdir <- "R:/temp/honda/sqlite-db"
#dbname <- "sandbox"

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
factors <- c("FF_BPS", "P_PRICE_AVG")

##############################################################################
# Open DB
##############################################################################
conn <- dbConnect(SQLite(), file.path(dbdir, dbname))
logger.info(paste("Opened DB:", file.path(dbdir, dbname)))

##############################################################################
# Define universe
##############################################################################
# Get the universe
universe <- getUniverse(conn, mktval=mkval_min, year=mkval_year)$id
universe <- tail(universe)
logger.info(paste("Filtered universe by market value >=", mkval_min, "as of", mkval_year)) 

##############################################################################
# Prepare the control variable - do it yourself!
##############################################################################
#
# Consolidate time series into a single big time table 
# where rows=:this_year_months, cols=:companies
#
series <- list()
if( do_create_tables ){
    for( factor in factors ){
        
        series[[factor]] <- getBulkTSeries(conn, dbdir, universe, factor, begin, end)
    }
} else {
    for( factor in factors ){
        data <- dbReadTable(conn, paste("bulk_", factor, sep=""))
        data <- as.xts(data, as.Date(unjulianday(data[[1]])))
        data$date = NULL
        series[[factor]] <- data
    }
}

# Get rid of companies that don't have all the necessary factors
common_companies <- intersect(names(series[[factors[1]]]), names(series[[factors[1]]]))
logger.debug("common companies: ", paste(common_companies,collapse=","))
# Check if they are empty.  If so, bail out.
for( factor in factors ){
    series[[factor]] <- series[[factor]][,common_companies]
    if( is.empty(series[[factor]]) ){
        logger.error("No ", factor, " data with common companies.  Bailing out...")
        stop()
    }
}
# put them together and see if there are still data points.
common_timestamps <- as.character(intersect(substr(index(series[[factors[1]]]),1,7), substr(index(series[[factors[2]]]),1,7)))
for( factor in factors ){
    series[[factor]] <- series[[factor]][common_timestamps,]
    if( is.empty(series[[factor]]) ){
        logger.error("No ", factor, " data with common timestamps.  Bailing out...")
        stop()
    }
}
pbs <- series[[factors[1]]]
price <- series[[factors[2]]]

# compute the control variable
book_per_share <- price / pbs

# create a table for that.
dbSendQuery(conn, "DROP TABLE derived_BOOK_PER_SHARE")
dbWriteTable(conn, "derived_BOOK_PER_SHARE", as.data.frame(book_per_share))
data <- dbReadTable(conn, "derived_BOOK_PER_SHARE")
control_var <- as.xts(data, by=rownames(data))

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

# future periods, in month, to project onto
periods <- c(1,3,6,12)

# Number of bins.  e.g. 5 for quintilizing
nbins <- 5

#analyzeFactorTrend(totalR, price, bps, nbins, periods, pallet, c(factors, "P_TOTAL_RETURNC"))
seeWhatHappens(control_var, totalR, periods, nbins)
logger.info("Completed normally.  Good day!")
logger.close()

