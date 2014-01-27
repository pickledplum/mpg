library(RSQLite)
source("logger.r")
#logger.init(logger.DEBUG, do_stdio=TRUE)


enQuote <- function( items ){
    return(paste("'", items, "'", sep=""))
}
enQuote2 <- function( items) {
    return(paste("\"", items, "\"", sep=""))
}
enParen <- function( items ){
    return(paste("(", items, ")", sep=""))
}
# tablename: tablename name
# conditions: vector of SQL WHERE conditions
# outwhat: vector of items to be returned
trySelect <- function(conn, tablename, columns, conditions, max_failures=0){
    q_str <- paste("SELECT", paste(columns, collapse=","), "FROM", enQuote(tablename), "WHERE", conditions)
    return(tryGetQuery(conn, q_str, max_failures))
}
tryCreateTable <- function(conn, tablename, column_specs, max_failures=0){
    q_str <- paste("CREATE TABLE", enQuote(tablename), enParen(paste(column_specs,collapse=",")))
    return(trySendQuery(conn, q_str, max_failures))
}
tryCreateTableIfNotExists <- function(conn, tablename, column_specs, max_failures=0){
    q_str <- paste("CREATE TABLE IF NOT EXISTS", enQuote(tablename), enParen(paste(column_specs, collapse=",")))
    return(trySendQuery(conn, q_str, max_failures))
}
tryDrop <- function(conn, tablename, max_failures=0){
    q_str <- paste("DROP TABLE IF EXISTS", tablename)
    return(trySendQuery(conn, q_str, max_failures))
}
tryInsert <- function(conn, tablename, columns, values, max_failures=0){
    q_str <- paste("INSERT INTO", enQuote(tablename), enParen(paste(enQuote(columns), collapse=",")), "VALUES", 
                   paste(apply(values, 1, FUN=function(record){ enParen(paste(record, collapse=",")) }), collapse=","))
    return(trySendQuery(conn, q_str, max_failures))
}
tryInsertOrReplace <- function(conn, tablename, columns, values, max_failures=0){
    q_str <- paste("INSERT OR REPLACE INTO", enQuote(tablename), enParen(paste(enQuote(columns), collapse=",")), "VALUES", 
                   paste(apply(values, 1, FUN=function(record){ enParen(paste(record, collapse=",")) }), collapse=","))
    return(trySendQuery(conn, q_str, max_failures))
}
tryUpdate <- function(conn, tablename, keyname, keyval, columns, values, max_failures=0){
    assert.equal(length(columns), length(values))
    to_exclude <- which(columns==keyname)
    pairs <- paste(enQuote(columns),"=",values,sep="")
    q_str <- paste("UPDATE", enQuote(tablename), "SET", paste(pairs,collapse=","), "WHERE", enQuote(keyname), "=", keyval)
    return(trySendQuery(conn, q_str, max_failures))
}
tryInsertOrUpdate <- function(conn, tablename, keyname, columns, values, max_failures=0) {
    to_insert <- values
    keyloc <- which(columns==keyname)
    n <- nrow(values)
    for( i in seq(1,n) ){
        vals <- values[i,]
        keyval <- unlist(vals[keyloc])
        if( nrow(trySelect(conn, tablename, c(keyname), paste(keyname, keyval, sep="=")) )>0 ){
            to_insert <- to_insert[-c(which(to_insert==keyval)),]
            tryUpdate(conn, tablename, keyname, keyval, columns[-c(keyloc)], vals[,-c(keyloc)], max_failures)
        }
    }
    if( nrow(to_insert) > 0 ){
        tryInsert(conn, tablename, columns, to_insert)
    }
}

trySendQuery <- function(conn, q_str, max_failures=0){
    logger.debug(q_str)
    for( nfailure in seq(0, max_failures)) {
        tryCatch({
            dbSendQuery(conn, q_str)
            break
        }, error = function(msg){
            if( nfailure >= max_failures){
                logger.error(paste(nfailure, "th failure:", msg, sep=""))
                stop(msg)
            }
            logger.warn(paste(nfailure, "th failure:", msg, sep=""))
            Sys.sleep(1)
        }
        )
    }
    return(TRUE)
}
tryGetQuery <- function(conn, q_str, max_failures=0){
    logger.debug(q_str)
    ret <- NULL
    for( nfailure in seq(0, max_failures)) {
        tryCatch({
            ret <- dbGetQuery(conn, q_str)
            break
        }, error = function(msg){
            if( nfailure >= max_failures){
                logger.error(paste(nfailure, "th failure:", msg, sep=""))
                stop(msg)
            }
            logger.warn(paste(nfailure, "th failure:", msg, sep=""))
            Sys.sleep(1)
        }
        )
    }
    if( is.null(ret) ) return(data.frame())
    return(ret)
}
