
advanceMonths <- function( yy, mm, N ){
    if(N>=0){
        if( mm + N > 12 ){
            yy = yy + 1
            mm = (mm+N)-12
        } else{
            mm = mm + N
        }
        return( c(yy,mm) )
    } else {
        if( mm - N < 1 ){
            yy = yy - 1
            mm = (mm-N)+12
        } else{
            mm = mm - N
        }
        return( c(yy,mm) )
    }
}
