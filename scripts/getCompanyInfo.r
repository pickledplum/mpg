

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

db <- "/home/honda/sqlite-db/mini.sqlite"
conn <<- dbConnect( SQLite(), db )
print(paste("Opened SQLite database:", db))
universe <- c("GOOG", "JP3902400005")
r1 <- getCompanyInfo(conn, universe)
r2 <- getAvailableFQLs(conn, universe[2])

dbDisconnect(conn)
