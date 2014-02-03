#'
#' Get the list of company IDs (Factset IDs)
#' according to the given criteria
#' 
#' @return Data frame containing the table of company IDs and market values.
#'         The fist frame is titled "id", the second "mkval($M)".
#'         The market values are in million dollars.
#' 
#' @param conn A connection to the database
#' @param year The year of which criteria are matched
#' @param mktval The minimal market value (inclusive)
#' @param market The market designation by which the matches are made.  
#'        DM, EM, FM, WORLD for developed, emerging, frontier and undetermined.
#' @param country The country by which matches are made.  Use two leter ISO country code.
#' @param sector The sector by which matches are made.
#' @param industry The industry by which matches are made.
#' 
#' All but year and mktval (market value) are optional.
#' 
#' @examples
#' db <- "/home/honda/sqlite-db/mini.db"
#' conn <- dbConnect( "SQLite", db)
#' universe <- getUniverse( conn, 2013, 300, market="DM")
#' print(universe)
#' 
#' >            id mkval($M)
#' >1         GOOG 46117.000
#' >2       004561  4360.100
#' >3 JP3902400005  7642.003
#' >...
#' 
#' universe <- getUniverse(conn, 2013, 300, country="US")
#' print(universe)
#' 
#' >    id mkval($M)
#' >1 GOOG     46117
#'
#' $Id$ 
tryCatch({
    source("tryDb.r")
    source("assert.r")
    source("is.empty.r")
    source("julianday.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)

getUniverse <- function( 
    conn,
    year, 
    mktval, 
    market=NULL, 
    country=NULL, 
    region=NULL, 
    sector=NULL, 
    industry=NULL ){
    tablename <- 'yearly_FF_WKCAP'
    
    valcond_templ <- "[YEAR] >= MKTVAL"
    
    where <- c()
    if( !is.null(market) ){
        where <- c(where, paste("market=", enQuote(market), sep=""))
    }
    if( !is.null(country) ) {
        where <- c(where, paste("country_id=", enQuote(country), sep=""))
    }
    if( !is.null(region) ){
        where <- c(where, paste("region=", enQuote(region), sep=""))
    }
    if( !is.null(sector) ){
        where <- c(where, paste("sector=", enQuote(sector), sep=""))
    }
    if( !is.null(industry) ){
        where <- c(where, paste("industry=", enQuote(industry), sep=""))
    }
    select_templ <- "SELECT factset_id AS id, [YEAR] AS 'mkval($M)'"
    from_templ <- "FROM country JOIN (SELECT * FROM company JOIN (SELECT * FROM TABLENAME WHERE VALCOND) USING (factset_id)) USING (country_id)"    
    
    templ <- paste(select_templ, from_templ)
    
    if( !is.empty(where) ) {
        where_tmple <- paste("WHERE", paste(where, collapse=" AND "))
        
        templ <- paste(templ, where_tmple)
    }
    q_str <- gsub("TABLENAME", enQuote(tablename), templ)
    q_str <- gsub("VALCOND", valcond_templ, q_str)
    q_str <- gsub("YEAR", year, q_str)
    q_str <- gsub("MKTVAL", mktval, q_str)
    print(q_str)
    ret <- tryGetQuery(conn, q_str)
    if( is.empty(ret) ) return(c())
    
    return(ret)
}
