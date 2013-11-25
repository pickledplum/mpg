###############################################################
#
# Follow up failures
# 
# Download failed transactions and merged the picked up data
# into the master.
#
# <Inputs>
# A CSV in which the first column contains the company ID.
# The other columns data will be completely ignored.
# The first row is also ignored, assuming it is the column 
# header.
#
# A CSV containing the partial master t-series matrix.
# Rows represent dates, columns represent companies.
# The first row is considered the columns header, and
# the first column the dates.
#
# <Output>
# A new CSV containing T+1 by N+1 matrix, where the first 
# row contains companies IDs and the first column contains
# dates.
#
###############################################################
#library(FactSetOnDemand)
library(xts)

INPUT_DIR <- "R:/temp/honda/mpg/frontier/fs_output/"
OUTPUT_DIR <- "R:/temp/honda/mpg/frontier/fs_output/"
FAILED_FILE_PREFIX <- "ex-fm-"
FAILED_FILE_POSTFIX <- ".failed.txt"
IS_HEADER <- FALSE
BINSIZE <- 100
t0 <- "19900101"
t1 <- "0"
currencies = c("USD")#, "local")
FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/acwi/fs-sorted-params.txt"

failed_files <- dir(INPUT_DIR, full.names=FALSE, pattern=paste("^", FAILED_FILE_PREFIX, ".+", FAILED_FILE_POSTFIX, sep=""))
print(failed_files)

failed_files <- head(failed_files,1)
for( failed_file in failed_files ){
    
    a <- regexpr(FAILED_FILE_POSTFIX, failed_file)
    tmp <- regmatches(failed_file, a, invert=TRUE)
    b <- regexpr(FAILED_FILE_PREFIX, tmp[[1]][1])
    tmp <- regmatches(tmp[[1]][1], b, invert=TRUE)
    s <- tmp[[1]][2]
    print(s)
    tokens <- strsplit(s, "-")
    print(tokens)
    stmt <- tokens[[1]][1]
    freq <- tokens[[1]][2]
    curr <- tokens[[1]][3]
    
    df <- read.csv(file.path(INPUT_DIR, failed_file), header=IS_HEADER)
    ids <- df[[1]]
    print(ids)
    for( begin in seq(1,length(ids), by=BINSIZE) ) {
        end <- min(begin + BINSIZE - 1, length(ids))
        ts <- get_a_series(stmt, ids[begin:end], t0, t1, freq, curr)
    }
}
# Get the daily price returns from day t0 to t1 for the companies.
# Used FQL: P_TOTAL_RETURNC()
# Return a time series (xts).  
# The data column are labeled by the company ID.
get_a_series <- function(stmt, ids, t0, t1, freq, currency) {
    # for testing transaction failure
    #if( currency == "USD") stop(id)
    master <- xts()
    data <- FF.ExtractFormulaHistory(ids,stmt,paste(t0,":",t1, ":",freq, sep=""), paste("curr=",curr,sep=""))
    # break the array of data by company IDs
    is.first <- TRUE
    for( id in ids ){
        flags <- data$Id==id
        a_comp <- data.frame(data[flags,])
        
        rownames(a_comp) <- as.Date(a_comp[,2])
        a_comp$Id <- c()
        a_comp$Date <- c()
        colnames(a_comp) <- c(id)
        
        x_comp <- as.xts(a_comp)
        if( is.first ){
            master <- x_comp
            is.first <- FALSE
        } else{
            master <- merge(master, x_comp)
        }
    }
    
    return(master)
}

