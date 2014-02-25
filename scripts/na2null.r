
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
