
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
    p1 <- p0 * prod(1.0 + returns) - 1.
    return(p1)
}

source("assert.r")
p0 = 1.
expectedR = (1.1)^3 - 1
returns <- c(.1,.1,.1)
print(paste("P0=", p0))
print(paste("expected compounded return: (1.1)^3=", expectedR))
getCompoundedReturn(p0, returns)
assert.equal( expectedR, getCompoundedReturn(p0, returns))
