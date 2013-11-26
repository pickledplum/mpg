library(xts)
#
# Recover the whole or partial results from the temporary file, and return as XTS.
#
recover_from_tempfile2 <- function(filename) {
    tmp <- read.table(filename,header=FALSE,sep=",",colClasses=c("character"))
    colcompnames(tmp) <- tmp[1,]
    tmp <- subset(tmp, ID!="ID")
    rowcompnames(tmp) <- tmp[,1]
    tmp$ID <- c()
    tmp <- t(tmp)
    ncols <- ncol(tmp)
    nrows <- nrow(tmp)
    tmp <- matrix(as.numeric(tmp),nrow=nrows,ncol=ncols,dimcompnames=list(rowcompnames(tmp),colcompnames(tmp)))
    return(as.xts(tmp))
}

recover_from_tempfile <- function(filename) {
    # the file contains a m by n matrix, where m is the number of companyes, n is the number of dates.
    # N     t1  t2
    # A    v11 v22
    # B    v12 v22
    # C    v13 v23
    
    # in other words, it's the transpose of xts time series.
    # N     A   B   C
    # t1    v11 v12 v13
    # t2    v21 v22 v23
    conn <- file(filename, "r", blocking=FALSE)
    line <- readLines(conn, n=1, ok=TRUE, warn=TRUE)
    headers <- unlist(strsplit(line, ","))
    ncomps <- as.integer(headers[1])
    dates <- as.Date(headers[2:length(headers)])
    m <- matrix(double(), length(dates), ncomps)
    compids <- vector(mode="character", length=ncomps)
    for( i in seq(1, ncomps) ){
        line <- readLines(conn, n=1, ok=TRUE, warn=TRUE)
        if( length(line) < 1 ) break
        row <- unlist(strsplit(line, ","))
        compids[i] <- row[1]
        suppressWarnings( vals <- as.double(row[2:length(row)]))
        m[,i] <- vals
    }

    close(conn)
    colnames(m) <- compids
    tseries <- xts(m, dates)
    return(tseries)
}