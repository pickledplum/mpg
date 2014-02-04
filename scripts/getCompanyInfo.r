
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
# Get company info
#
# universe: list of factset ids
getCompanyInfo <- function(conn, universe ) {
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
getAvailableFQLs <- function(conn, fsid){
    q_str <- "SELECT fql AS FACTSET_ID, description, unit, report_freq FROM catalog JOIN fql using(fql) WHERE factset_id=FACTSET_ID"
    q_str <- gsub("FACTSET_ID", enQuote(fsid), q_str)
    data <- tryGetQuery(conn, q_str)
    
    
    return(data)
}

findTablename <- function(conn, fsid, fql) {
    str <- "factset_id='FSID' AND fql='FQL'"
    str <- gsub("FSID", fsid, str)
    str <- gsub("FQL", fql, str)
    ret <- trySelect(conn, "catalog", 
                     c("tablename"), 
                     str)
    if( is.empty(ret) ) return("")
    return(ret$tablename)
}
getTSeries <- function(conn, fsid, fql, t0, t1){

    j0 <- julianday(as.Date(t0))
    j1 <- julianday(as.Date(t1))

    tablename <- findTablename(conn, fsid, fql)
    str <- "SELECT date(date) AS date, usd FROM 'TABLENAME' WHERE date BETWEEN J0 AND J1"
    str <- gsub("TABLENAME", tablename, str)
    str <- gsub("J0", j0, str)
    str <- gsub("J1", j1, str)
    ret <- tryGetQuery(conn, str)
    rownames(ret) <- as.Date(ret$date)
    ret <- ret[-c(1)]
    t <- as.xts(ret) 
    
    return(t)
}

getBulkTSeries <- function(conn, universe, fql, t0, t1) {
    j0 <- julianday(as.Date(t0))
    j1 <- julianday(as.Date(t1))
    temptable <- paste("bulk_", fql, sep="")
    trySendQuery(conn, paste("DROP TABLE IF EXISTS", temptable))
    tryCreateTempTable(conn, temptable, c("date", enQuote(universe)))

    for(fsid in universe){
        tseries <- getTSeries(conn, fsid, fql, t0, t1)
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


