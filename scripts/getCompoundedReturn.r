#' Compound returns
#' @param p0 
compoundReturns <- function(p0, returns){
    p1 <- p0*prod(1.+returns)
    return(p1)
}
#' Get the compounded return
#' @param P0 The reference price
#' @param R A vector of returns in fraction, not %
#' @return Compounded return R in fraction, not %,
#' s.t. R=P1/P0-1, 
#' P1=P0*(1+R1)*(1+R2)*...*(1+Rn) 
#' where n is the number of periods (e.g. days)
#' 
#' @example 
#' P0 <- 1.00
#' R <- c(0.03,0.025,0,-0.01)
#' P1 <- getCompoundedReturn(P0, R)
#' 
#' print(p1)
#' 
#' 0.0451925
getCompoundedReturn <- function(p0, returns){
    r <- compoundReturns(p0, returns) / p0 - 1.
    return(r)
}

