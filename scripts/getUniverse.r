# Define universe, limiting by market value as of a specified year
#
# value : market value in million (USD)
# asof:   year as of
# 
# Returns a data frame:
#    1st frame is titled "id" and lists FactSet IDs
#    2nd frame is titled "mkval($M)", and lists market values in million USD
#
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
    minval, 
    market=NULL, 
    country=NULL, 
    region=NULL, 
    sector=NULL, 
    industry=NULL ){
    tablename <- 'yearly_FF_WKCAP'
    
    valcond_templ <- "[YEAR] >= MINVAL"
    where_templ <- ""
    if( !is.null(market) ){
        where_templ <- paste(where_templ, "AND market= ", enQuote(market), sep="")
    }
    if( !is.null(country) ) {
        where_templ <- paste(where_templ, "AND country= ", enQuote(country), sep="")
    }
    if( !is.null(region) ){
        where_templ <- paste(where_templ, "AND region= ", enQuote(region), sep="")
    }
    if( !is.null(sector) ){
        where_templ <- paste(where_templ, "AND sector= ", enQuote(sector), sep="")
    }
    if( !is.null(industry) ){
        where_templ <- paste(where_templ, "AND industry= ", enQuote(industry), sep="")
    }
    select_templ <- "SELECT factset_id AS id, [YEAR] AS 'mkval($M)'"
    from_templ <- "FROM country JOIN (SELECT * FROM company JOIN (SELECT * FROM TABLENAME WHERE VALCOND) using (factset_id)) using (country_id)"    
    
    templ <- paste(select_templ, from_templ)
    if( !is.empty(where_templ) ){
        templ <- paste(templ, where_templ)
    }
    q_str <- gsub("TABLENAME", enQuote(tablename), templ)
    q_str <- gsub("VALCOND", valcond_templ, q_str)
    q_str <- gsub("YEAR", year, q_str)
    q_str <- gsub("MINVAL", minval, q_str)
    print(q_str)
    return( tryGetQuery(conn, q_str) )
}
