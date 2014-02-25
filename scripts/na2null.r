
#' Replace character sequence "NA" with character sequence "NULL" in a string
#'
#' @param str String representation of comma delimited list of values.  
#' @example 
#' x <- c(NA,1,2,NA,3,NA)
#' x_str <- paste(x, collapse=",")
#' y <- na2null( x_str) )
#' print(y)
#' 
#' "NULL,1,2,NULL,3,NULL"
na2null <- function( str ){
    str <- gsub("^ *NA *$", "NULL", str)
    str <- gsub("^ *NA *,", "NULL,", str)
    str <- gsub(", *NA *$", ",NULL", str)
    str <- gsub(", *NA *,", ",NULL,", str)
    str <- gsub("= *NA *$", "=NULL", str)
    str <- gsub("=NA *,", "=NULL", str)
    str <- gsub("[(] *NA *[)])", "(NULL)", str)
    str <- gsub("[(] *NA *,", "(NULL", str)
    str <- gsub(", *NA *[)]", ",NULL)", str)
    return(str)
}

source("assert.r")
expected = "INSERT OR REPLACE INTO 'P_PRICE_AVG-B3DCPD' ('date','usd','local') VALUES (1,2,NA),(3,4,NA),(5,6,NA),(7, 8,NA)"
actual = "INSERT OR REPLACE INTO 'P_PRICE_AVG-B3DCPD' ('date','usd','local') VALUES (1,2,NA),(3,4,NA),(5,6,NA),(7, 8,NA)"
assert.equal(actual, expected)

expected = "NULL,1,2,NULL,3,NULL"
actual = na2null(paste(c(NA,1,2,NA,3,NA), collapse=","))
assert.equal(actual, expected)