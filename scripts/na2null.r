
#' Replace "NA" words with "NULL"
#'
#' @param str String representation of comma delimited list of values.  
#' @example 
#' x <- c(NA,1,2,NA,3,NA)
#' y <- na2null( paste(x, collapse=",") )
#' print(y)
#' 
#' NULL,1,2,NULL,3,NULL
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

#x <- "INSERT OR REPLACE INTO 'P_PRICE_AVG-B3DCPD' ('date','usd','local') VALUES (2456566,0.322401940823,NA),(2456597,0.329246491194,NA),(2456627,0.325390160084,NA),(2456658,0.330765843391,NA)"
#na2null(x)