
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
#' Get company info
#'
#' @param universe: list of factset ids
#' @return A data frame containing the table of company info.
#' 
#' <frames>
#' 1. factset_id : FactSet ID
#' 2. company_name: Company name
#' 3. country_id: 2-letter ISO country ID
#' 4. sector: Sector name
#' 5. indgrp: Industry group name
#' 6. industry: Industry name
#' 7. subind: Sub-industry name
#' 
getCompanyInfo <- function(conn, universe) {
    master_data <- NULL
    for( fsid in universe ){
        q_str <- "SELECT * FROM company JOIN country using (country_id) WHERE factset_id='FACTSET_ID'"
        q_str <- gsub("FACTSET_ID", fsid, q_str)
        data <- tryGetQuery(conn, q_str)
        if( is.null(master_data) ){
            master_data <- data
        } else {
            master_data <- rbind(master_data, data)
        }
    }
    return(master_data)
}
#' Get all the avaiable FQL parameters
#' @param fsid FactSet ID
#' @return A data frame containing table of FQL params and their related info available to the company.
#' 
#' <frames>
#' 1. fql : FQL param
#' 2. syntax: FQL syntax to extract "fql" values
#' 3. description: description
#' 4. unit: unit of values
#' 5. report_freq: FactSet reporting frequency
#' 6. category_id: type of FQL param ("company_fund", "company_meta", "price", "country_fund")
#' 7. note: some notes if any
#' 
getAvailableFQLs <- function(conn, fsid){
    q_str <- "SELECT fql, syntax, description, unit, report_freq, category_id, note FROM catalog JOIN fql USING(fql) WHERE factset_id=FACTSET_ID"
    q_str <- gsub("FACTSET_ID", enQuote(fsid), q_str)
    data <- tryGetQuery(conn, q_str)
    return(data)
}

#' Get the SQL database name and the table name
#' 
#' @param fsid A FactSet ID
#' @param fql A FQL parameter
#' 
#' @example 
#' The time series for FF_ASSETS for Google is stored in the FF_ASSETS.sqlite database.
#' 
#' t <- findTablename(conn, "GOOG", "FF_ASSETS")
#' print(t)
#' print(class(t))
#' 
#' > "FF_ASSETS.sqlite" "FF_ASSETS-GOOG"
#' > character()
#' 
findTablename <- function(conn, fsid, fql) {
    str <- "factset_id='FSID' AND fql='FQL'"
    str <- gsub("FSID", fsid, str)
    str <- gsub("FQL", fql, str)
    ret <- trySelect(conn, "catalog", 
                     c("dbname", "tablename"), 
                     str)
    if( is.empty(ret) ){
        return(xts())
    }
    pair <- c(ret$dbname, ret$tablename)
    names(pair) <- c("dbname", "tablename")
    
    return(pair)
}
#' Get the time series corresponding to the company & the FactSet parameter
#' 
#' @param fsid FactSet ID
#' @param fql FQL parameter
#' @param t0 Start of the time range in YYYY-MM-DD format
#' @param t1 End of the time range in YYYY-MM-DD format
#' @return A non-empty XTS object containg the time series if successful.  An empty but non-null XTS if failed.
#' 
#' @example Get the price data, devident compounded, from 1/1/2000 to 12/31/2013.
#' t <- getTSeries(conn, "GOOG", "P_PRICE_RETURNC", "2000-01-01", "2013-12-31")
#' tail(t)
#' 
#' > 2013-12-12  -0.680416822
#' > 2013-12-13  -0.857031345
#' > 2013-12-16   1.149141788
#' > 2013-12-17  -0.290781260
#' > 2013-12-18   1.391768456
#' > 2013-12-19   0.135517120
#' > 2013-12-20   1.325702667
#' > 2013-12-23   1.315617561
#' > 2013-12-24  -0.292348862
#' > 2013-12-26   0.505471230
#' > 2013-12-27   0.084125996
#' > 2013-12-30  -0.799363852
#' > 2013-12-31   1.014006138
#' 
getTSeries <- function(meta_conn, dbdir, fsid, fql, t0, t1){

    j0 <- julianday(as.Date(t0))
    j1 <- julianday(as.Date(t1))
    
    ret <- findTablename(meta_conn, fsid, fql)
    if( is.empty(ret) ){
        logger.error(paste("No table found:", fsid, fql))
        return()
    }
    dbname <- ret["dbname"]
    db <- file.path(dbdir, dbname)
    tablename <- ret["tablename"]

    if( is.empty(tablename) ){
        logger.error(paste("No table for:", fsid, ",", fql))
        return(xts())
    }
    conn <- dbConnect(SQLite(), db)
    logger.info(paste("Opened db:", db))
    str <- "SELECT date(date) AS date, usd FROM 'TABLENAME' WHERE date BETWEEN J0 AND J1"
    str <- gsub("TABLENAME", tablename, str)
    str <- gsub("J0", j0, str)
    str <- gsub("J1", j1, str)
    ret <- tryGetQuery(conn, str)
    dbDisconnect(conn)
    
    if( is.empty(ret) ) {
        logger.warn(paste("No data avaiable:", fsid, fql))
        return(xts())
    }
    rownames(ret) <- as.Date(ret$date)
    ret <- ret[-c(1)]
    tryCatch({
        ret$usd <- as.numeric(ret$usd)
    }, warn=function(){
        #ignore
    })
    t <- as.xts(ret) 
    
    return(t)
}
#' Get the time series corresponding to the company & the FactSet parameter
#' 
#' @param universe The list of FactSet IDs
#' @param fql FQL parameter
#' @param t0 Start of the time range in YYYY-MM-DD format
#' @param t1 End of the time range in YYYY-MM-DD format
#' @return A non-empty XTS object containg the time series if successful.  An empty but non-null XTS if failed.
#' 
#' @example Get the price data, devident compounded, from 1/1/2000 to 12/31/2013.
#' getBulkTSeries(conn, c("GOOG","JP3902400005"), universe=universe$id, fql="FF_ASSETS", t0="2010-01-01", t1="2013-12-31")
#' 
#'             GOOG JP3902400005
#' 2010-03-31    NA     34408.11
#' 2010-12-31 57851           NA
#' 2011-03-31    NA     40210.90
#' 2011-12-31 72574           NA
#' 2012-03-31    NA     41213.33
#' 2012-12-31 93798           NA
#' 2013-03-31    NA     36273.25
#' 
getBulkTSeries <- function(conn, dbdir, universe, fql, t0, t1) {
    j0 <- julianday(as.Date(t0))
    j1 <- julianday(as.Date(t1))
    temptable <- paste("bulk_", fql, sep="")
    trySendQuery(conn, paste("DROP TABLE IF EXISTS", temptable))
    specs <- c("date INTEGER PRIMARY KEY NOT NULL UNIQUE", paste(enQuote2(universe), rep("FLOAT", length(universe))))
    tryCreateTempTable(conn, temptable, specs)

    for(fsid in universe){
        tseries <- getTSeries(conn, dbdir, fsid, fql, t0, t1)
        #print(tseries)
        if( is.empty(tseries) ){
            logger.warn(paste("No data:", fsid, fql, "between", t0, "and", t1))
            next
        }
        dates <- rownames(as.data.frame(tseries))
        for( k in seq(1, nrow(tseries)) ){
            
            date <- dates[k]
            tryCatch({
                julianday(as.Date(date) )
            }, error=function(msg){
                logger.error(paste("can't convert to julianday:", date))
                next
            })
            jdate <- julianday(as.Date(date))
            val <- tseries$usd[k]
            ret <- trySelect(conn, temptable, c("date"), paste("date=", jdate, sep=""))
            if( is.empty(ret) ){
                #insert
                
                tryInsert(conn, temptable, c("date", fsid),
                          data.frame(c(jdate), c(val))
                )
            } else{
                str <- "UPDATE TABLENAME SET COLUMNNAME=VALUE WHERE date=JULIANDATE"
                str <- gsub("TABLENAME", enQuote2(temptable), str)
                str <- gsub("COLUMNNAME", enQuote2(fsid), str)
                str <- gsub("VALUE", val, str)
                str <- gsub("JULIANDATE", jdate, str)
                trySendQuery(conn, str)
            }
        }
    }
    ret <- trySelect(conn, temptable, c("date(date) as date", enQuote2(universe)), c())

    rownames(ret) <- as.Date(ret$date)
    ret <- ret[-c(1)]
    t <- as.xts(ret) 
    return(t)
}
#' Get the list of all possible industry groups
getIndustryGroups <- function(conn) {
    l <- trySelect(conn, "company", c("indgrp"), is_distinct=TRUE)
    return(l$indgrp)
}
#' Get the list of all possible industries
getIndustories <- function(conn){
    l <- trySelect(conn, "company", c("industry"), is_distinct=TRUE)
    return(l$industry)
}
#' Get the list of all possible sub-industries
getSubIndustries <- function(conn){
    l <- trySelect(conn, "company", c("subind"), is_distinct=TRUE)
    return(l$subind)
}
#' Get the list of all possible sectors
getSectors <- function(conn){
    l <- trySelect(conn, "company", c("sector"), is_distinct=TRUE)
    return(l$sector)
}
