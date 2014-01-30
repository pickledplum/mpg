JDN0 <- -2440588 #the first day of 4317 BC
julianday <- function( date, ... ){
    return( julian(date, JDN0) )
}
unjulianday <- function( jdn, ... ){
    return( as.Date(jdn + JDN0) )
}