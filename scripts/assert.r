#assert
assert.equal <- function(a,b){
    #stopifnot( (is.null(a)&&is.null(b)) || (is.na(a)&&is.na(b)) || a==b ) 
    if(is.null(a) && is.null(b)) return(TRUE)
    if(is.null(a) && !is.null(b)) stop(FALSE)
    if(!is.null(a) && is.null(b)) stop(FALSE)
    if(is.na(a) && is.na(b)) return(TRUE)
    if(is.na(a) && !is.na(b)) return(FALSE)
    if(!is.na(a) && is.na(b)) return(FALSE)
    stopifnot(a==b)
    return(TRUE)
}
assert.equal.tol <- function(a,b) {
    delta <- abs(a-b)
    stopifnot( delta <= .Machine$double.eps )
}
assert.equal.reltol <- function(a,b,tol){
    delta <- abs(a-b)/a
    stopifnot( delta <= tol+.Machine$double.eps )
}
assert.not.equal <- function(a,b){
    stopifnot(a!=b)
}
assert.lt <- function(a,b){
    stopifnot(a<b)
}
assert.le <- function(a,b){
    stopifnot(a<=b)
}
assert.gt <- function(a,b){
    stopifnot(a>b)
}
assert.ge <- function(a,b){
    stopifnot(a>=b)
}
assert.null <- function(a){
    stopifnot(is.null(a))
}
assert.empty <- function(a){
    if(is.null(a)) return(TRUE)
    if(length(a)<1) return(TRUE)
    if(is.character(a) && a=="") return(TRUE)
    return(FALSE)
}
assert.non.empty <- function(a){
    return(!assert.empty(a))
}
assert.false <- function(a){
    stopifnot(!a)
}
assert.true <- function(a){
    stopifnot(a)
}

