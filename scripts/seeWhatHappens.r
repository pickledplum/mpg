#' Mining indicators
#' 
library(RSQLite)
library(xts)

have_GridExtra = FALSE
if( "ggplto2" %in% installed.packages()
    && "gridExtra" %in% installed.packages() ){
    library(ggplot2)
    require(gridExtra)
    have_GridExtra = TRUE
} else{
    logger.warn("No ggplot2 or gridExtra packages re installed.  No funcy grid plotting...")
}
source("../dummy/getUniverse.r")
source("../dummy/getTSeries.r")
source("logger.r")
source("advanceMonths.r")
source("getCompoundedReturn.r")

# colors in the order of lower to higher bins
pallet <- c("blue","dark green","yellow","dark orange","red")

seeWhatHappens <- function( controlVar, totalR, periods, nbins, factorName ){
   # browser()
    n_periods <- length(periods)
    
    valid_months <- as.character(intersect(substr(index(controlVar),1,7), substr(index(totalR),1,7)))
    oldest_this_year_month <- min(valid_months)
    tokens <- as.integer(strsplit(oldest_this_year_month, "-")[[1]])
    oldest_yy <- tokens[1]
    oldest_mm <- tokens[2]
    latest_this_year_month <- max(valid_months)
    tokens <- as.integer(strsplit(latest_this_year_month, "-")[[1]])
    latest_yy <- tokens[1]
    latest_mm <- tokens[2]
    logger.debug(paste("Valid months: min,max,# - ", paste(oldest_this_year_month, latest_this_year_month, n_periods),sep=","))

    
    # iterate from the most recent to the oldest
    for( this_year_month in valid_months ){
        logger.info("Processing ", this_year_month, "...")
        
        yy <- rep(0,n_periods)
        mm <- rep(0,n_periods)

        factor <- as.vector(controlVar[this_year_month])
        #logger.debug("factor: ", paste(factor, collapse=","))
        names(factor) <- colnames(controlVar)
        #logger.debug("Names(factor): ", paste(names(factor), collapse=","))
        # Get rid of NAs
        factor <- factor[!is.na(factor)]
        if( is.empty(factor) ){
            logger.warn("No factor data.  Skipping...")
            next
        }
        #logger.debug("factor on ", this_year_month, ":", paste(factor,collapse=","))
        # Sort in the ascending order
        sorted_index <- order(factor, decreasing=FALSE)
        sorted_pbk <- factor[sorted_index]
    
        #logger.debug("Sorted index: ", sorted_index)
        
        # collect points to the companies data in each bin
        range <- length(sorted_pbk)
        if( range < nbins ){
            logger.warn("Not enough data.  Bin size is ", nbins, " but there is only ", range, " non-null data points.  Skipping...")
            next
        }
        percentile_notations <- vector("numeric", nbins)
        h <- as.integer(range/nbins)
        bin_ids <- list()
        sorted_ids <- names(factor)[sorted_index] 
        for(i in 1:nbins){
            begin <- (i-1) * h + 1
            end <- begin + h - 1
            bin_ids[[i]] <- sorted_ids[begin:end]   
            percentile_notations[i] <- as.integer(h*(i-1)/range*100.)
            #logger.debug("Bin ", percentile_notations[i], " percentile: ", bin_ids[[i]])
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
    
        # Gather incremental returns by months
        # Example: if periods = c(1,3,6,12) then, 
        # dailyR[[1]] contains returns from the reference month to the next month period. 0,1,2,3
        # dailyR[[2]] contains rturns from the first period to the second period. 4,5,6
        # dailyR[[3]] contains 7,8,9,10,11,12
        are_we_done = FALSE
        previous_period <- this_year_month
        for(period in seq(1,n_periods)) {
    
            next_period <- advanceMonths(this_year_month, periods[period]-1)  
            logger.debug("Looking into ", next_period) 
            if( !is.empty((totalR[next_period])) ){

                dailyR[[period]] <- totalR[paste(previous_period, next_period, sep="/")]
                #logger.debug("For period from ", previous_period, " to the end of ", next_period, ": ")
                #print(dailyR[[period]])
    
            } else{
                # put dummy in the slot
                dummy <- t(data.frame(rep(NA, ncol(totalR))))
                rownames(dummy) <- paste(next_period, "-01", sep="")
                colnames(dummy) <- colnames(totalR)
                dummy <- as.xts(dummy, by=as.Date(index(dummy)))
                dailyR[[period]] <- dummy
                # check if there is really no more data beyond that point
                tokens <- as.integer(strsplit(next_period, "-")[[1]])
                next_yy <- tokens[1]
                next_mm <- tokens[2]
                if( next_yy > latest_yy ) {
                    # obvious, this is done
                    are_we_done <- TRUE
                } else if( next_yy == latest_yy ) {
                    if( next_mm > latest_mm ){
                        # sure, went beyond
                        are_we_done <- TRUE
                    }
                } else {
                    # No, it's just missing data for this period
                    logger.warn("Skipping this time period in the future due to the lack of data: ", next_period)
                    next
                }
            }
            #browser()
            previous_period <- next_period
            
        }
        # We need 12 months in the future to do this marching, or we are done.
        if( are_we_done ){
            logger.warn("No more ", periods[n_periods], " months in the future.  We are done.")
            break
        }
        # Report how many stocks in each bin
        num_companies_in_each_bin <- vector("numeric", nbins)
        for( bin in 1:nbins ){
            num_companies_in_each_bin[bin] <- length(bin_ids[[bin]])
        }
        logger.info("Number of stocks in bins in ascending order of percentile: ", 
                    paste(num_companies_in_each_bin, collapse=","))
        
        # Compute compounded return upto the period
        #
        compoundedR <- list()
        projectedPrice <- list()
        
        for( period in 1:n_periods ){
            #ndays <- as.integer(periods[period]*(260/12))
            compoundedR_this_bin <- list()
            for( bin in 1:nbins ){
                #logger.debug(as.integer(h*(bin-1)/range*100.), " percentile")
                r <- rep(NA, length(bin_ids[[bin]]))
                names(r) <- bin_ids[[bin]]
                for( i in 1:length(bin_ids[[bin]]) ){
                    company <- bin_ids[[bin]][i]
                    returns <- as.vector(dailyR[[period]][,company]) 
                    #logger.debug("Visiting ", company)   
                    
                    ##############################################
                    # ToDO: Revisit by Sachko, 3/7/2014
                    #
                    # Not quite sure what to do with
                    # "annualization".  Here's the code where
                    # total returns are compounded upto this 
                    # month period.  Do I need to divide
                    # the daily return by 260, or divide the
                    # M-month compounded R by M/12?
                    # 
                    #
                    ##############################################
                    r[i] <- getCompoundedReturn( 1., returns ) #/ 12. * periods[period]
                }   
                if( is.empty(r[!is.na(r)]) ){
                    logger.warn("No non-NA returns for this bin in period: ", bin, ", ", period)
                }
                # Compounded upon the previous time period.  
                if(period>1){
                    r <- r * compoundedR[[period-1]][[bin]]
                }
                compoundedR_this_bin[[bin]] = r
                #logger.debug("Compounded return for bin=", bin, ": ")
                #print(r)
            }
            compoundedR[[period]] <- compoundedR_this_bin
    
        }

        #print(compoundedR)
        # average out across the companies within a bin
        #
        # Example: The resulting matrix has the dimensions of nbins by nperiods.
        #         1mon     3mon     6mon     12mon
        # 0-20     r11      r12      r13       r14
        # 21-40
        # 41-60
        # 61-80
        # 81-100
        all_returns <- matrix(NA, nrow=nbins, ncol=n_periods)
        colnames(all_returns) <- periods
        for( bin in 1:nbins) {
            for( period in 1:n_periods) {
                bin_r <- compoundedR[[period]][[bin]] #/ max(periods) * periods[period]
                all_returns[bin,period] <- mean(bin_r)
            }
        }
        period_mean <- vector("numeric", length=n_periods)
        rel_returns <- matrix(NA, nrow=nbins, ncol=n_periods)
        colnames(rel_returns) <- periods
        for ( period in 1:n_periods ) {
            period_mean[period] <- mean(all_returns[,period], na.rm=TRUE)
            rel_returns[,period] <- all_returns[,period] - period_mean[period]
        }

        #print(rel_returns)
        rel_returns <- rel_returns * 100.  # to percent 
        
        if(TRUE) {
            y <- rel_returns[!is.na(rel_returns)]
            
            if( !is.empty(y) ) {
                percentiles <- seq(0, 100-as.integer(100/nbins), as.integer(100/nbins))
                y_min <- -5 #floor(min(y))
                y_max <- 5 #max(ceiling(max(y)), y_min)
                logger.debug("y min,max: ", y_min, ",", y_max)
                plot(x=1,y=1, type="n", xlim=c(0,max(periods)), ylim=c(y_min,y_max), axes=FALSE, 
                     xlab="Months in Future", ylab="Relative Return (%)") 
                
                axis(side=1, at=c(0,periods) )
                axis(side=2, at=seq(y_min,y_max,1) )
                abline(0,0, col="black")
                for(i in 1:nbins){
                    points(c(0,periods), c(0,rel_returns[i,]), col=pallet[i], pch="*")
                    lines(c(0,periods), c(0,rel_returns[i,]), col=pallet[i], pch="*")
                    
                }
                legend("bottom", paste(percentiles, "%"), col=pallet[1:nbins], 
                       lty=1:nbins, pch="*", ncol=5)
                title(main=paste(this_year_month, " - ", factorName, " vs Returns"))
                
                
                
                # Delay for view
                Sys.sleep(1)
            } else{
                logger.warn("All Y values are NAs, skipping plotting for ", this_year_month, " ...")
            }
        }
    }
}