

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

getTSeries <- function(conn, fsid, fql, t0, t1){

    j0 <- julianday(as.Date(t0))
    j1 <- julianday(as.Date(t1))

    ret <- trySelect(conn, "catalog", 
              c("tablename"), 
              paste("factset_id='", fsid, "' AND fql='", fql, "'", sep=""))
    return(ret$tablename)
}

db <- "/home/honda/sqlite-db/mini.sqlite"
conn <<- dbConnect( SQLite(), db )
print(paste("Opened SQLite database:", db))

universe <- c("GOOG", "JP3902400005", "00105510")

logger.init(level=logger.DEBUG,
            do_stdout=TRUE)

print(paste("Log file:", logfile))

r1 <- getCompanyInfo(conn, universe)
r2 <- getAvailableFQLs(conn, universe[3])
r3 <- getTSeries(conn, universe[3], "FF_ASSETS", unjulianday(244330), unjulianday(2445791))
dbDisconnect(conn)
