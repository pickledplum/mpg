#' Volatility is measured as the annualized standard deviation of daily total returns
#' over the prior year.
#' 
#' Multiplying the standard diviation by square root of 260
#' 
#' @param prices Price data in XTS format
#' 
library(xts)
library(PerformanceAnalytics)
source("is.empty.r")
source("logger.r")
<<<<<<< HEAD
source("advanceMonths.r")

=======
>>>>>>> 267a4e3dd3af1ae24f0f7e7fa5cf83e133789455
logger.init(logger.DEBUG)
computeVolatility <- function( sampling_points, returns ){
    nrows <- length(sampling_points)
    ncols <- ncol(returns)
    m <- matrix(NA, nrow=nrows, ncol=ncols)

    rownames(m) <- paste(sampling_points, "-01", sep="")
    colnames(m) <- colnames(returns)
    for( i in 1:nrows ){  
        point_of_interest <- sampling_points[i]
        
        # prior year data
        twelve_months_ago <- advanceMonths(point_of_interest, -11)
        logger.debug("point of interest: ", point_of_interest, ", 12 mons ago: ", twelve_months_ago)
        chunk <- returns[paste(twelve_months_ago, point_of_interest, sep="/")]
        
        if( is.empty(chunk) ){
            logger.warn("No data for period from ", twelve_months_ago, " to ", point_of_interest)
            next
        }
<<<<<<< HEAD
        #print(chunk)
        tryCatch({
            m[i,] <- apply(chunk, 2, function(column){
                annstd <- sd.annualized(column, scale=260)
=======
        print(chunk)
        tryCatch({
            m[i,] <- apply(chunk, 2, function(column){
                annstd <- sd.annualized(column, scale=260) * sqrt(260)
>>>>>>> 267a4e3dd3af1ae24f0f7e7fa5cf83e133789455
                return(annstd)
                })

        }, error=function(msg){
            logger.error(t, ": ", msg)
            next
        })
    }
    
    # convert to xts
    ret <- as.xts(m, by=as.Date(rownames(m)))
    return(ret)
}

<<<<<<< HEAD
=======


if(TRUE){
    timestamps <- c("2010-01-01",
                    "2010-03-01",
                    "2010-06-01",
                    "2010-09-01",
                    "2010-12-01",
                    "2011-01-01",
                    "2011-03-01",
                    "2011-06-01",
                    "2011-09-01",
                    "2011-12-01")
    
    # in per cent
    values <- c(10,10,10,10,10,1,10,10,1,1)
    
    data <- data.frame(values)
    rownames(data) <- timestamps
    returns <- as.xts(data)
    
    ret <- computeVolatility(c("2010-12","2011-12"), returns)
    print(ret)
}
>>>>>>> 267a4e3dd3af1ae24f0f7e7fa5cf83e133789455
