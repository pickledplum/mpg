JDN0 <- -2440588 #the first day of 4317 BC
julianday <- function( day, ... ){
    return( julian(day, JDN0) )
}
unjulianday <- function( jdn, ... ){
    return( as.Date(jdn+JDN0, origin="1970-01-01") )
}