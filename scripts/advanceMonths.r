
.advanceMonths <- function( yy, mm, N ){
    deltaYY <- as.integer(N / 12)
    if( N >= 0 ){
        deltaMM <- N %% 12
    } else { # N < 0
        deltaMM <- -(12 - N %% 12)
        if( mm + deltaMM <= 0 ){
            deltaYY <- deltaYY - 1
            deltaMM <- deltaMM + 12
        }
    }
    return(c(yy+deltaYY, mm+deltaMM))
}

is.endOfMonth <- function(timestamp){
    tokens <- strsplit(timestamp,'-')[[1]]
    t <- as.integer(tokens)
    if( t[2] == 2 ){
        if( is.leapYear(t[1]) ){
            if( t[3] == 29 ){
                return(TRUE)
            }
        } else if( t[3]==28 ){
            return(TRUE)
        }
    } else if( t[2] %in% c(4,6,9,11) ){
        if( t[3] == 30 ){
            return(TRUE)
        }
    } else if( t[3] == 31 ){
        return(TRUE)
    }
    return(FALSE)
}
is.leapYear <- function(year){
    if( year %% 100 == 0 ) {
        if( year %% 400 == 0 ){
            return(TRUE)
        } else{
            return(FALSE)
        }
    } else if( year %% 4 == 0 ) {
        return(TRUE)
    }
    return(FALSE)
}

advanceMonths <- function(timestamp, N) {
    # advance N months
    tokens <- strsplit(timestamp,'-')[[1]]
    t0 <- as.integer(tokens)
    yymm0 <- t0[1:2]
    yymm1 <- .advanceMonths(yymm0[1],yymm0[2], N)
    if( length(t0) >= 3 ){
        
        dd1 <- t0[3]
        # adjust dates according to the calendar
        if(is.endOfMonth(timestamp)){    
            # if the future month falls into Feburary, adjust the end of the month date
            # according to the leap year
            if( yymm1[2] == 2 ){
                if(is.leapYear(yymm1[1]) ) {
                    dd1 <- 29
                } else {
                    dd1 <- 28
                }
                
            } else if( yymm1[2] %in% c(4,6,9,11) ){
                dd1 <- 30
            }
        }
        return( paste(formatC(yymm1[1], width=4, format="d", flag="0"),
                formatC(yymm1[2], width=2, format="d", flag="0"),
                formatC(dd1, width=2, format="d", flag="0"),
                sep="-") )
    } else{
        return(paste(formatC(yymm1[1], width=4, format="d", flag="0"),
                     formatC(yymm1[2], width=2, format="d", flag="0"),
                     sep="-"))
        
    }
}
