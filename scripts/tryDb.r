library(RSQLite)
source("logger.r")
#logger.init(logger.DEBUG, do_stdio=TRUE)
trySendQuery <- function(conn, q_str, max_trials=1){

    for( nfailure in seq(0, max_trials)) {
        print(nfailure)
        tryCatch({
            dbSendQuery(conn, q_str)
            break
        }, error = function(msg){
            if( nfailure >= max_trials){
                logger.error(paste(nfailure, "th failure:", msg, sep=""))
                return(NULL)
            }
            logger.warn(paste(nfailure, "th failure:", msg, sep=""))
            Sys.sleep(1)
        }
        )
    }
    return(TRUE)
}
tryGetQuery <- function(conn, q_str, max_trials=1){

    ret <- NULL
    for( nfailure in seq(0, max_trials)) {
        tryCatch({
            ret <- dbGetQuery(conn, q_str)
            break
        }, error = function(msg){
            if( nfailure >= max_trials){
                logger.error(paste(nfailure, "th failure:", msg, sep=""))
                return(NULL)
            }
            logger.warn(paste(nfailure, "th failure:", msg, sep=""))
            Sys.sleep(1)
        }
        )
    }
    return(ret)
}
# 
#db <- "D:/home/honda/sqlite-db/mini.sqlite"
#conn <<- dbConnect( SQLite(), db )
#q_str <- "CREATE TABLE IF NOT EXISTS dummy (id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, val INTEGER)"
#trySendQuery(conn, q_str)
#q_str <- "INSERT OR REPLACE INTO dummy (vallll) VALUES (1)"
#trySendQuery(conn, q_str)
#dbDisconnect(conn)