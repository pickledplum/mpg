#' Mining indicators
#' 
library(RSQLite)
source("getUniverse.r")
source("getTSeries.r")
source("logger.r")
logger.init(logger.DEBUG)

# DB connection
#conn <- dbConnect(SQLite(), "/home/honda/sqlite-db/mini/mini.sqlite")
conn <- dbConnect(SQLite(), "/home/honda/sqlite-db/japan500/japan500.sqlite")

# look ahead months
study_months <- c(1,3,6,12)

# Define a universe
universe <- getUniverse(conn, mktval=-100000, year=2013)$id


# Pick a factor or two
factors <- c("FF_BPS", "P_PRICE_AVG", "P_TOTAL_RETURNC")
#bps <- getBulkTSeries(conn, "/home/honda/sqlite-db/japan500", universe, factors[1], "1980-01-01", "2013-12-31")
#price <- getBulkTSeries(conn, "/home/honda/sqlite-db/japan500", universe, factors[2], "1980-01-01", "2013-12-31")
#careful, this will take 6-8 hours
#total_r <- getBulkTSeries(conn, "/home/honda/sqlite-db/japan500", universe, factors[3], "1980-01-01", "2013-12-31")

# price to book
x1 <- dbReadTable(conn, "bulk_FF_BPS")
bps <- as.xts(x1, as.Date(unjulianday(x1[[1]])))
# price
x2 <- dbReadTable(conn, "bulk_P_PRICE_AVG")
price <- as.xts(x2, as.Date(unjulianday(x2[[1]])))

# total return, compounded
x3 <- dbReadTable(conn, "bulk_P_TOTAL_RETURNC")
trc <- as.xts(x3, as.Date(unjulianday(x3[[1]])))

valid_days <- as.character(as.Date(intersect(index(price), index(bps))))

# iterate from the most recent to the oldest
for( timestamp in tail(valid_days,13) ){
#timestamp <- tail(valid_days,1)
    browser()
    tokens <- as.integer(strsplit(timestamp, "-")[[1]])
    yy <- tokens[1]
    mm <- tokens[2]

    # calculate returns over the next ... periods
    
    # base case
    r0 <- trc[ paste(yy,mm,sep="-")][1,]
    
    # 12 months
    tokens <- incrementByNMonth(yy,mm,12)
    yy4 <- tokens[1]
    mm4 <- tokens[2]    
    if( is.empty(trc[ paste(yy4,mm4,sep="-")]) ) {
        logger.warn("Insufficient look ahead data")
        next
    }
    #browser()
    r4 <- trc[ paste(yy4,mm4,sep="-")][1,]

    # 6 months
    tokens <- incrementByNMonth(yy,mm,6)
    yy3 <- tokens[1]
    mm3 <- tokens[2]
    r3 <- trc[ paste(yy3,mm3,sep="-")][1,]
    
    # 3 months
    tokens <- incrementByNMonth(yy,mm,3)
    yy2 <- tokens[1]
    mm2 <- tokens[2]
    r2 <- trc[ paste(yy2,mm2,sep="-")][1,]
    
    # one month
    tokens <- incrementByNMonth(yy,mm,1)
    yy1 <- tokens[1]
    mm1 <- tokens[2]
    r1 <- trc[ paste(yy1,mm1,sep="-")][1,]
    
    pbk <- price[timestamp] / bps[timestamp]
    pbk <- pbk[,!is.na(pbk)]
    sorted_index <- order(pbk, decreasing=FALSE)
    sorted_pbk <- pbk[,sorted_index]
    
    # indicies for each quintile
    range <- ncol(sorted_pbk)
    q1_begin <- 1
    q2_begin <- range/5
    q3_begin <- range/5*2
    q4_begin <- range/5*3
    q5_begin <- range/5*4
    q1 <- sorted_index[q1_begin:q2_begin-1]
    q2 <- sorted_index[q2_begin:q3_begin-1]
    q3 <- sorted_index[q3_begin:q4_begin-1]
    q4 <- sorted_index[q4_begin:q5_begin-1]
    q5 <- sorted_index[q5_begin:range]
}
dbDisconnect(conn)

incrementByNMonth <- function( yy, mm, N ){
    if( mm + N > 12 ){
        yy = yy + 1
        mm = (mm+N)-12
    } else{
        mm = mm + N
    }
    return( c(yy,mm) )
}
decrementByNMonth <- function( yy, mm, N ){
    if( mm - N < 1 ){
        yy = yy - 1
        mm = (mm-N)+12
    } else{
        mm = mm - N
    }
    return( c(yy,mm) )
}

annualizeSigmaDailyTotalReturn <- function(x, y) {
    
}

