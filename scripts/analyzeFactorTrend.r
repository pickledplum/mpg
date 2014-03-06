#' Mining indicators
#' 
library(RSQLite)
library(psych)
library(xts)

source("../dummy/getUniverse.r")
source("../dummy/getTSeries.r")
source("logger.r")
source("advanceMonths.r")
source("getCompoundedReturn.r")

logger.init(logger.INFO)

# DB connection
dblocation <- "R:/temp/honda/sqlite-db/japan500-keep/japan500.sqlite"
#dblocation <- "R:/temp/honda/sqlite-db/sandbox"
conn <- dbConnect(SQLite(), dblocation)
logger.info(paste("Opened DB:", dblocation))


# Define a universe
mkval_min <- -100000
mkval_year <- 2013
universe <- getUniverse(conn, mktval=mkval_min, year=mkval_year)$id
universe <- tail(universe)
logger.info(paste("Filtered universe by market value >=", mkval_min, "as of", mkval_year)) 

# Pick factors to use
factors <- c("FF_BPS", "P_PRICE_AVG", "P_TOTAL_RETURNC")

#
# Consolidate time series into a single big time table where rows=:this_year_months, cols=:companies
#
#bps <- getBulkTSeries(conn, "R:/temp/honda/sqlite-db/japan500", universe, factors[1], "1980-01-01", "2013-12-31")
#price <- getBulkTSeries(conn, "R:/temp/honda/sqlite-db/japan500", universe, factors[2], "1980-01-01", "2013-12-31")
#WARNING: Tthis daily data has taken 6 hours on XP32 w/ 3GB mem
#total_r <- getBulkTSeries(conn, "R:/temp/honda/sqlite-db/japan500", universe, factors[3], "1980-01-01", "2013-12-31")

# price to book
x1 <- dbReadTable(conn, "bulk_FF_BPS")
bps <- as.xts(x1, as.Date(unjulianday(x1[[1]])))
bps$date = NULL
logger.debug("Downloaded the monthly book per share table data")
# price
x2 <- dbReadTable(conn, "bulk_P_PRICE_AVG")
price <- as.xts(x2, as.Date(unjulianday(x2[[1]])))
price$date = NULL
logger.debug("Downloaded the monthly price data")

# total return, compounded
x3 <- dbReadTable(conn, "bulk_P_TOTAL_RETURNC")
totalR <- as.xts(x3, as.Date(unjulianday(x3[[1]])))
totalR$date = NULL
totalR <- totalR / 100.  # convert to fraction from percentage
logger.debug("Downloaded the daily total returns")

# Done with the db
dbDisconnect(conn)
logger.info("Closed the db")

# grab intersection of the two time series (price and book per share) by year and month.
valid_months <- as.character(intersect(substr(index(price),1,7), substr(index(bps),1,7)))

# landmark periods
periods <- c(1,12,24,36)

nbins <- 5

oldest_this_year_month <- min(valid_months)
tokens <- as.integer(strsplit(oldest_this_year_month, "-")[[1]])
oldest_yy <- tokens[1]
oldest_mm <- tokens[2]
latest_this_year_month <- max(valid_months)
tokens <- as.integer(strsplit(latest_this_year_month, "-")[[1]])
latest_yy <- tokens[1]
latest_mm <- tokens[2]

n_periods <- length(periods)

logger.debug(paste("Valid months: min,max,# - ", paste(oldest_this_year_month, latest_this_year_month, n_periods),sep=","))
# iterate from the most recent to the oldest
for( this_year_month in tail(valid_months,100) ){
    logger.info("Processing ", this_year_month, "...")
    
    yy <- rep(0,n_periods)
    mm <- rep(0,n_periods)

    # Get rid of company data whose bps was NA
    valid_companies <- intersect(names(price), names(bps))
    valid_companies <- intersect(valid_companies, names(totalR))
    logger.debug("common companies: ", paste(valid_companies,collapse=","))
    bps <- bps[,valid_companies]
    totalR <- totalR[,valid_companies]
    price <- price[,valid_companies]
    
    # Covnert to vector in order to ignore possible date difference
    # because we only need month resolution
    # But, keep track of company names attached to the price values
    logger.debug("price: ", paste(price[this_year_month], collapse=","))
    logger.debug("bps: ", paste(bps[this_year_month], collapse=","))   
    pbk <- as.vector(price[this_year_month]) / as.vector(bps[this_year_month])
    logger.debug("pbk: ", paste(pbk, collapse=","))
    names(pbk) <- colnames(price)
    logger.debug("Names(pbk): ", paste(names(pbk), collapse=","))
    # Get rid of NAs
    pbk <- pbk[!is.na(pbk)]
    if( is.empty(pbk) ){
        logger.warn("No pbk data.  Skipping...")
        next
    }
    logger.debug("pbk on ", this_year_month, ":", paste(pbk,collapse=","))
    # Sort in the ascending order
    sorted_index <- order(pbk, decreasing=FALSE)
    sorted_pbk <- pbk[sorted_index]

    logger.debug("Sorted index: ", sorted_index)
    logger.debug("sorted by P/B: ", paste(valid_companies[sorted_index],collapse=","))
    
    # collect points to the companies data in each bin
    range <- length(sorted_pbk)
    if( range < nbins ){
        logger.warn("Not enough data.  Bin size is ", nbins, " but there is only ", range, " non-null data points.  Skipping...")
        next
    }
    h <- as.integer(range/nbins)
    bin_ids <- list()
    sorted_ids <- names(pbk)[sorted_index] 
    for(i in 1:nbins){
        begin <- (i-1) * h + 1
        end <- begin + h - 1
        bin_ids[[i]] <- sorted_ids[begin:end]      
        logger.debug("Bin ", as.integer(h*(i-1)/range*100.), " percentile: ", bin_ids[[i]])
    }
    
    # Will store the returns for the future periods
    dailyR <- list()
    
    # Extract the year and the month of this iteration
    tokens <- as.integer(strsplit(this_year_month, "-")[[1]])
    yy0 <- tokens[1]
    mm0 <- tokens[2]
        
    # Get the price for each company for this month
    # Keep carrying the company IDs attached to the values
    # so that we can filter & sort by IDs later.
    if( is.empty(totalR[ paste(yy0,mm0,sep="-")] )){
        logger.warn("No return data on the reference month.  Skipping...")
        next
    }

    r0 <- as.vector(totalR[ paste(yy0,mm0,sep="-")][1,sorted_index])
    names(r0) <- sorted_ids
    #logger.debug("Return on (", this_year_month, ")")
    #print(r0)
    
    # Get the price for each company for this month
    if( is.empty(price[ paste(yy0,mm0,sep="-")] )) {
        logger.warn("No price data on the reference month.  Skipping...")
        next
    } 

    
    # Gather incremental returns by months
    # Example: if periods = c(1,3,6,12) then, 
    # dailyR[[1]] contains returns from the reference month to the next month period. 0,1,2,3
    # dailyR[[2]] contains rturns from the first period to the second period. 4,5,6
    # dailyR[[3]] contains 7,8,9,10,11,12
    are_we_done = FALSE
    previous_period <- this_year_month
    for(period in seq(1,n_periods)) {

        next_period <- advanceMonths(this_year_month, periods[period]-1)  
        logger.info("Looking into ", next_period) 
        data <- totalR[next_period]
        if( !is.empty((totalR[next_period])) ){
            
            dailyR[[period]] <- totalR[paste(previous_period, next_period, sep="/")]
            #logger.debug("For period from ", previous_period, " to the end of ", next_period, ": ")
            #print(dailyR[[period]])

        } else{
            are_we_done <- TRUE
        }
        previous_period <- next_period
        
    }
    # We need 12 months in the future to do this marching, or we are done.
    if( are_we_done ){
        logger.warn("No more ", periods[n_periods], " months in the future.  We are done.")
        break
    }
    
    # Compute compounded return for each period
    #
    compoundedR <- list()
    projectedPrice <- list()
    
    for( period in 1:n_periods ){
        compoundedR_this_bin <- list()
        for( bin in 1:nbins ){
            logger.debug(as.integer(h*(bin-1)/range*100.), " percentile")
            r <- rep(NA, length(bin_ids[[bin]]))
            names(r) <- bin_ids[[bin]]
            for( i in 1:length(bin_ids[[bin]]) ){
                company <- bin_ids[[bin]][i]
                
                logger.debug("Visiting ", company)
                # set the base case
                
                r[i] <- getCompoundedReturn( 1., as.vector(dailyR[[period]][,company]) )
            }   

            compoundedR_this_bin[[bin]] = r
            #logger.debug("Compounded return for bin=", bin, ": ")
            #print(r)
        }
        compoundedR[[period]] <- compoundedR_this_bin

    }
    #print(compoundedR)
    # average out across the companies within a bin, and annualize
    m <- matrix(NA, nrow=nbins, ncol=n_periods)
    colnames(m) <- periods
    for( bin in 1:nbins) {
        for( period in 1:n_periods) {
            bin_r <- compoundedR[[period]][[bin]] #/ max(periods) * periods[period]
            m[bin,period] <- mean(bin_r)
        }
    }
    m <- m*100.  # to percent 
    if(TRUE) {
        # lower percentile to higher percentile
        pallet <- c("blue","green","yellow","orange","red")
        y_max <- 100 #ceiling(max(m[!is.na(m)]))
        y_min <- -100 #floor(min(m[!is.na(m)]))
        plot(x=1,y=1, type="n", xlim=c(0,max(periods)), ylim=c(y_min,y_max), axes=FALSE, 
             xlab=this_year_month, ylab="Total Return (%)") 
        axis(side=1, at=c(0,periods) )
        axis(side=2, at=seq(y_min,y_max,1) )
        for(i in 1:nbins){
            points(c(0,periods), c(0,m[i,]), col=pallet[i])
            lines(c(0,periods), c(0,m[i,]), col=pallet[i])
        }
        #Sys.sleep(1)
    }
}


logger.info("Completed normally.  Good day!")
logger.close()

