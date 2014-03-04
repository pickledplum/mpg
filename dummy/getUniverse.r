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
    year=2013, 
    mktval=-100000, 
    market=NULL, 
    country=NULL, 
    region=NULL, 
    sector=NULL, 
    industry=NULL,
    subind=NULL){
    tablename <- 'yearly_FF_WKCAP'
    
    valcond_templ <- "[YEAR] >= MKTVAL"
    
    country_cond <- c()
    if( !is.null(market) ){
        country_cond <- c(country_cond, paste("market=", enQuote(market), sep=""))
    }
    if( !is.null(region) ){
        country_cond <- c(country_cond, paste("region=", enQuote(region), sep=""))
    }
    if( !is.null(country) ) {
        country_cond <- c(country_cond, paste("country_id=", enQuote(country), sep=""))
    }
    
    company_cond <- c()
    if( !is.null(sector) ){
        company_cond <- c(company_cond, paste("sector=", enQuote(sector), sep=""))
    }
    if( !is.null(industry) ){
        company_cond <- c(company_cond, paste("industry=", enQuote(industry), sep=""))
    }
    if( !is.null(subind) ) {
        company_cond <- c(company_cond, paste("subind=", enQuote(subind), sep=""))
    }
    
    templ <-
    "SELECT factset_id AS id, [<YEAR>] AS 'mkval($M)' FROM
    (SELECT country_id, market FROM country <COUNTRY_COND>)
    JOIN
    (
        SELECT factset_id, company_name, country_id, [<YEAR>]
        FROM (SELECT factset_id, company_name, sector, industry, subind, country_id  AS country_id
              FROM company <COMPANY_COND>)
        JOIN
        (SELECT * FROM <TABLENAME> WHERE [<YEAR>] >= <MKTVAL>)
        USING (factset_id)
    )
    USING (country_id)
    "
    q_str <- gsub("<TABLENAME>", enQuote(tablename), templ)
    q_str <- gsub("<YEAR>", year, q_str)
    q_str <- gsub("<MKTVAL>", mktval, q_str)
    if( !is.empty(country_cond) ){
        str <- paste("WHERE", paste(country_cond, collapse=","))
        q_str <- gsub("<COUNTRY_COND>", str, q_str)
    } else{
        q_str <- gsub("<COUNTRY_COND>", "", q_str)
    }
    if( !is.empty(company_cond) ){
        str <- paste("WHERE", paste(company_cond, collapse=","))
        q_str <- gsub("<COMPANY_COND>", str, q_str)
    } else{
        q_str <- gsub("<COMPANY_COND>", "", q_str)
    }
    q_str <- gsub("\n", "", q_str)
    logger.debug(q_str)

    ret <- tryGetQuery(conn, q_str)
    if( is.empty(ret) ) return(c())
    
    return(ret)
}
