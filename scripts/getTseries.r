tryCatch({
    source("tryDb.r")
    source("assert.r")
    source("is.empty.r")
    source("julianday.r")
    source("extractDirFromPath.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)


#' Get the SQL database name and the table name
#' @param conn connection to the meta db
#' @param fsid A FactSet ID
#' @param fql A FQL parameter
#' @return a pair of dbname and tablename
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
#' @param conn Open connection to the FQL specific sub-db.
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
.getTSeries <- function(meta_conn, conn, fsid, fql, t0, t1){

    ret <- findTablename(meta_conn, fsid, fql)
    
    tablename <- ret["tablename"]
    if( is.empty(tablename) ){
        logger.error(paste("No table for:", fsid, ",", fql))
        return(xts())
    }

    j0 <- julianday(as.Date(t0))
    j1 <- julianday(as.Date(t1))
    
    str <- "SELECT date(date) AS date, usd FROM 'TABLENAME' WHERE date BETWEEN J0 AND J1"
    str <- gsub("TABLENAME", tablename, str)
    str <- gsub("J0", j0, str)
    str <- gsub("J1", j1, str)
    ret <- tryGetQuery(conn, str)
    
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

makeTSeriesTable <- function(meta_conn, universe, fql, t0, t1) {

    dbinfo <- dbGetInfo(meta_conn)
    dbdir <- extractDirFromPath(dbinfo$dbname)
    
    j0 <- julianday(as.Date(t0))
    j1 <- julianday(as.Date(t1))
    temptable <- paste("bulk_", fql, sep="")
    trySendQuery(meta_conn, paste("DROP TABLE IF EXISTS", temptable))
    specs <- c("date CHAR(10) PRIMARY KEY NOT NULL UNIQUE", 
               paste(enQuote2(universe), rep("FLOAT", length(universe))))
    tryCreateTable(meta_conn, temptable, specs)
    
    sub_conns <- new.env(hash=TRUE)
    
    for(fsid in universe){
        
        ret <- findTablename(meta_conn, fsid, fql)
        sub_dbname <- ret["dbname"]
        tablename <- ret["tablename"]
        sub_conn <- NULL
        if( exists(sub_dbname, sub_conns) ){
            sub_conn <- get(sub_dbname, envir=sub_conns)
        } else {
            sub_conn <- dbConnect(SQLite(), file.path(dbdir, sub_dbname))
            assign(sub_dbname, sub_conn, envir=sub_conns)
        }
        
        tseries <- .getTSeries(meta_conn, sub_conn, fsid, fql, t0, t1)
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
            humandate <- as.Date(date) #julianday(as.Date(date))
            val <- tseries$usd[k]
            ret <- trySelect(meta_conn, temptable, c("date"), paste("date=", enQuote(humandate), sep=""))

            if( is.empty(ret) ){
                #insert
                
                tryInsert(meta_conn, temptable, c("date", fsid),
                          c(enQuote(humandate),val)
                )
            } else{
                str <- "UPDATE TABLENAME SET COLUMNNAME=VALUE WHERE date=HUMANDATE"
                str <- gsub("TABLENAME", enQuote(temptable), str)
                str <- gsub("COLUMNNAME", enQuote(fsid), str)
                str <- gsub("VALUE", val, str)
                str <- gsub("HUMANDATE", enQuote2(humandate), str)
                trySendQuery(meta_conn, str)
            }
        }
    }
    return(temptable)
}
getTSeries <- function(meta_conn, universe, fql, t0, t1){
    summary_tablename <- makeTSeriesTable(meta_conn, universe, fql, t0, t1)
    
    ret <- trySelect(meta_conn, summary_tablename, c("date(date) as date", enQuote2(universe)), c())
    rownames(ret) <- as.Date(ret$date)
    ret <- ret[-c(1)]
    t <- as.xts(ret) 
    
    for( sub_dbname in ls(sub_conns ) ){
        dbDisconnect(get(sub_dbname, envir=sub_conns))
    }
    return(t)
}
