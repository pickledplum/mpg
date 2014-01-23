is.empty <- function( obj ){

    if( length(obj)<1 ) return(TRUE)
    if( is.data.frame(obj) && nrow(obj)<1 ) return(TRUE)

    return(FALSE)
}
# is.empty(NULL)
# is.empty(c(NULL,NULL))
# is.empty(NA)
# is.empty(c(NA,NA,NULL))
