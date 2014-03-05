
incrementByNMonth <- function( yy, mm, N ){
    if( mm + N > 12 ){
        yy = yy + 1
        mm = (mm+N)-12
    } else{
        mm = mm + N
    }
    return( c(yy,mm) )
}
decrementByNMonth <- function( yy, mm, N ){
    if( mm - N < 1 ){
        yy = yy - 1
        mm = (mm-N)+12
    } else{
        mm = mm - N
    }
    return( c(yy,mm) )
}
