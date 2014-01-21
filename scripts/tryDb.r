library(RSQLite)
source("logger.r")
#logger.init(logger.DEBUG, do_stdio=TRUE)
trySendQuery <- function(conn, q_str, max_trials=1){
    
    nfailure <- 0
    while( nfailure <= max_trials ) {
        tryCatch({
            dbSendQuery(conn, q_str)
            break
        }, error = function(msg){
            nfailure = nfailure + 1
            if( nfailure > max_trials){
                logger.error(msg)
                stop()
            }
            logger.warn(msg)
            Sys.sleep(1)
        }
        )
    }
}
tryGetQuery <- function(conn, q_str, max_trials=1){
    nfailure <- 0
    ret <- NULL
    while( nfailure <= max_trials ) {
        tryCatch({
            ret <- dbGetQuery(conn, q_str)
            break
        }, error = function(msg){
            nfailure = nfailure + 1
            if( nfailure > max_trials){
                logger.error(msg)
                stop()
            }
            logger.warn(msg)
            Sys.sleep(1)
        }
        )
    }
    return(ret)
}
# 
# db <- "D:/home/honda/sqlite-db/mini.sqlite"
# conn <<- dbConnect( SQLite(), db )
# q_str <- "CREATE TABLE IF NOT EXISTS dummy (id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, val INTEGER)"
# trySendQuery(conn, q_str)