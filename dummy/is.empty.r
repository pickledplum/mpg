#' Test if an objec is empty.
#'
#' @param obj Any object.
#' @return True if an object is null, empty or NA in case of string.  False othewise.
#' 
#' 
is.empty <- function( obj ){

    if( length(obj)<1 ) return(TRUE)
    if( is.data.frame(obj) && nrow(obj)<1 ) return(TRUE)
    if( is.character(obj) && nchar(obj)<1 ) return(TRUE)

    return(FALSE)
}