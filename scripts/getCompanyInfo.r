

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
    
    return(ret)
}

getBulkTSeries <- function(conn, fsid_list, fql, t0, t1) {
    j0 <- julianday(as.Date(t0))
    j1 <- julianday(as.Date(t1))
    temptable <- paste("bulk_", fql, sep="")
    trySendQuery(conn, paste("DROP TABLE IF EXISTS", temptable))
    tryCreateTable(conn, temptable, c("date", enQuote(fsid_list)))
    
    for(fsid in fsid_list){
        tseries <- getTSeries(conn, fsid, fql, t0, t1)
        if( is.empty(tseries) ){
            logger.warn(paste("No data:", fsid, fql, "between", t0, "and", t1))
            next
        }
        for( k in seq(1, length(tseries$date)) ){
            date <- tseries$date[k]
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
    ret <- trySelect(conn, temptable, c("date(date) as date", enQuote2(fsid_list)), c())
    return(ret)
}


db <- "/home/honda/sqlite-db/mini.sqlite"
conn <<- dbConnect( SQLite(), db )
print(paste("Opened SQLite database:", db))

universe <- c("GOOG", "JP3902400005", "00105510", "004561")

logger.init(level=logger.DEBUG,
            do_stdout=TRUE)


r1 <- getCompanyInfo(conn, universe)
r2 <- getAvailableFQLs(conn, universe[3])
r3 <- getTSeries(conn, universe[2], "FF_ASSETS", "2010-01-01", "2013-12-31")
r4 <- getBulkTSeries(conn, universe, "FF_ASSETS", "2010-01-01", "2013-12-31")


dbDisconnect(conn)
