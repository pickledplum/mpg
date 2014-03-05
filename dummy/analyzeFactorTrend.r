#' Mining indicators
#' 
library(RSQLite)
library(psych)

source("getUniverse.r")
source("getTSeries.r")
source("logger.r")
source("incrementByNMonth.r")

logger.init(logger.DEBUG)

# DB connection
dblocation <- "R:/temp/honda/sqlite-db/japan500/japan500.sqlite"
conn <- dbConnect(SQLite(), dblocation)
logger.info(paste("Opened DB:", dblocation))


# Define a universe
mkval_min <- -100000
mkval_year <- 2013
universe <- getUniverse(conn, mktval=mkval_min, year=mkval_year)$id
logger.info(paste("Filtered universe by market value >=", mkval_min, "as of", mkval_year)) 

# Pick factors to use
factors <- c("FF_BPS", "P_PRICE_AVG", "P_TOTAL_RETURNC")

#
# Consolidate time series into a single big time table where rows=:timestamps, cols=:companies
#
#bps <- getBulkTSeries(conn, "/home/honda/sqlite-db/japan500", universe, factors[1], "1980-01-01", "2013-12-31")
#price <- getBulkTSeries(conn, "/home/honda/sqlite-db/japan500", universe, factors[2], "1980-01-01", "2013-12-31")
#WARNING: Tthis daily data has taken 6 hours on XP32 w/ 3GB mem
#total_r <- getBulkTSeries(conn, "/home/honda/sqlite-db/japan500", universe, factors[3], "1980-01-01", "2013-12-31")

# price to book
x1 <- dbReadTable(conn, "bulk_FF_BPS")
bps <- as.xts(x1, as.Date(unjulianday(x1[[1]])))
logger.debug("Downloaded the monthly book per share table data")

# price
x2 <- dbReadTable(conn, "bulk_P_PRICE_AVG")
price <- as.xts(x2, as.Date(unjulianday(x2[[1]])))
logger.debug("Downloaded the monthly price data")

# total return, compounded
x3 <- dbReadTable(conn, "bulk_P_TOTAL_RETURNC")
trc <- as.xts(x3, as.Date(unjulianday(x3[[1]])))
logger.debug("Downloaded the daily total returns")

# Done with the db
dbDisconnect(conn)
logger.info("Closed the db")

# grab intersection of the two time series (price and book per share) by year and month.
valid_months <- as.character(intersect(substr(index(price),1,7), substr(index(bps),1,7)))

# landmark months
months <- c(0,1,3,6,12)

nmons <- length(months)
nbins <- 5
logger.debug(paste("Valid months: min,max,# - ", paste(min(valid_months), max(valid_months),nmons),sep=","))

# iterate from the most recent to the oldest
for( timestamp in tail(valid_months,100) ){
    yy <- rep(0,nmons)
    mm <- rep(0,nmons)
    
    # Returns for each month
    r <- list()
    
    logger.info(timestamp)
    
    # Reference point data
    tokens <- as.integer(strsplit(timestamp, "-")[[1]])
    yy[1] <- tokens[1]
    mm[1] <- tokens[2]
    r[[1]] <- trc[ paste(yy[1],mm[1],sep="-")][1,]

    for(j in seq(2,nmons)) {

        tokens <- incrementByNMonth(yy[1],mm[1],months[j])
        yy[j] <- tokens[1]
        mm[j] <- tokens[2]    
        if( is.empty(trc[ paste(yy[j],mm[j],sep="-")]) )
            break
        r[[j]] <- trc[ paste(yy[j],mm[j],sep="-")][1,]
    
    }
    # if there is no 12 months from now, we are done
    if( is.empty(trc[ paste(yy[5],mm[5],sep="-")]) ) {
        logger.warn("No more 12 months in forward.  Done...")
        break
    }
  
    # covnert to vector in order to ignore possible date difference
    # we only care about month resolution
    pbk <- as.vector(price[timestamp]) / as.vector(bps[timestamp])
    names(pbk) <- colnames(price)[2:ncol(price)]
    pbk <- pbk[!is.na(pbk)]
    
    sorted_index <- order(pbk, decreasing=FALSE)
    sorted_pbk <- pbk[sorted_index]

    # indicies for each quintile
    range <- length(sorted_pbk)
    h <- as.integer(range/nbins)
    q_begin <- seq(0,nbins,1)
    q_index <- rep(0,nbins)
    q_ids <- rep("",nbins)
    mu <- rep(0,nbins)
    
    q_mu <- matrix(NA, nrow=nbins,ncol=nmons)
    q_div <- matrix(NA, nrow=nbins,ncol=nmons)
    rr.over <- matrix(NA, nrow=nbins, ncol=nmons)
    
    for(i in 1:nbins){
        
        q_begin[i] <- q_begin[i] * h + 1
        q_index[i] <- sorted_index[q_begin[i]:q_begin[i]+h-1]
        q_ids[i] <- names(pbk)[q_index[i]]
    }
    #quintile
    for(i in 1:nbins){
        # lookout points
        for(j in 1:nmons){
            # mean over all data for each period
            if(i==1) mu[j] <- mean(r[[j]])
            
            # mean over data within the quintile for each period
            q_mu[i,j] <- mean(as.vector(r[[j]][,q_ids[i]], mode="numeric"), na.rm=TRUE)
            # diviation from the mean
            q_div[i,j] <- q_mu[i,j] - mu[j]
            # growth rate of return from the reference month
            rr.over[i,j] <- (q_mu[i,j] - q_mu[i,1]) / q_mu[i,1]
        }
    }

    y_max <- +20 #ceiling(max(q_mu))
    y_min <- -20 # floor(min(q_mu))
    plot(x=1,y=1, type="n", xlim=c(0,12), ylim=c(y_min,y_max), axes=FALSE, 
         xlab=timestamp, ylab="Delta Return (%)") 
    axis(side=1, at=months) 
    axis(side=2, at=seq(y_min,y_max,1) )
    points(months, q_mu[1,], col="dark red")
    lines(months, q_mu[1,], col="dark red")
    #points(months, q_mu[2,], col="dark orange")
    #lines(months, q_mu[2,], col="dark orange")
    ##points(months, q_mu[3,], col="dark yellow")
    #lines(months, q_mu[3,], col="dark yellow")
    #points(months, q_mu[4,], col="dark green")
    #lines(months, q_mu[4,], col="dark green")
    points(months, q_mu[5,], col="dark blue")
    lines(months, q_mu[5,], col="dark blue")
    points(months, mu, col="magenta")
    lines(months, mu, col="magenta")
    Sys.sleep(1)
}



