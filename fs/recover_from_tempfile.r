library(xts)

recover_from_tempfile <- function(filename) {
    # the file contains a m by n matrix, where m is the number of companies, n is the number of dates.
    # The first item in the header contains the value of m.
    #
    # <m>   t1  t2
    # A    v11 v22
    # B    v12 v22
    # C    v13 v23
    conn <- file(filename, "r", blocking=FALSE)
    line <- readLines(conn, n=1, ok=TRUE, warn=TRUE)
    headers <- unlist(strsplit(line, ","))
    # first 
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