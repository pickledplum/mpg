#'
#' Quote, double-quote, curly brancket,
#' square brancket, paranthesis the list of
#' strings.
#'
#'
#' $Id: $

#' Single quote
enQuote <- function( items ){
    return(paste("'", items, "'", sep=""))
}
#' Double quote
enQuote2 <- function( items) {
    return(paste("\"", items, "\"", sep=""))
}

#' Parenthesis
enParen <- function( items ){
    return(paste("(", items, ")", sep=""))
}

#' Square brancket
enSquare <- function( items ){
    return(paste("[", items, "]", sep=""))
}

#' Curly brancket
enCurly <- function( items ){
    return(paste("{", items, "}", sep=""))
}