#'
#' Wrappers for dbSendQuery and dbGetQuery from RSQLite package
#' These conveniense wrappers encapsulate error handling.
#' Requires the "logger" unit being already initialized.
#' 
#' @seealso also: logger.r

library(RSQLite)
source("logger.r")
source("enQuote.r")
source("na2null.r")

#' Maxinum number of failures allowed before bailing out
MAX_FAILURES = 5

#' SELECT <columns> FROM <tablename> WHERE <condition>
#'  
#' @param conn A connection to the database
#' @param tablename The tablename name from which data is drawn.
#' @param columns The vector of column names
#' @param condition The WHERE clause, in string. NULL for none by default.
#' @param is_distinct TRUE if you want duplicates elimiated.  FALSE to keep duplicates.
#' @param max_failures The maximum number of failures before bailing out.
#' 
#' @return A non-empty data.frame if successful.  An empty, non-null, data.frame object otherwise.
#' 
#' @example  Draw from the company table the company IDs and belonging country IDs only if they are from USA or Great Britain.
#' Try no more than once in case of transaction failure.  Elimiate duplicates.
#' 
#' trySelect(conn=conn, 
#'           columns=c("company", "company_id", "country_id"),
#'           condition="country_id='US' OR country_id='GB'",
#'           is_disntict=TRUE,
#'           max_failures=1)
#' 
#' Note that elements of the condition string must be properly quoted, as it is passed into SQL as it is.
#' On the other hand, the tablename and the column names are automatically quoted.
#' 
trySelect <- function(conn, tablename, columns, condition=NULL, is_distinct=TRUE, max_failures=MAX_FAILURES){
    if( is.empty(condition) ){
        q_str <- paste("SELECT", ifelse(is_distinct, "DISTINCT", ""), paste(columns, collapse=","), "FROM", enQuote(tablename))
        
    } else{
        q_str <- paste("SELECT", ifelse(is_distinct, "DISTINCT", ""), paste(columns, collapse=","), "FROM", enQuote(tablename), "WHERE", condition)
    }
    return(tryGetQuery(conn, q_str, max_failures))
}
#' CREATE TABLE <tablename> (<spec_list>)
#'  
#' @param conn A connection to the database
#' @param tablename The tablename name from which data is drawn.
#' @param columns The vector of column names
#' @param spec_list The vector of column specifications.
#' @param max_failures The maximum number of failures before bailing out.
#' 
#' @return TRUE if successful.  FALSE otherwise.
#' 
#' @example  Create a table called "item", which has two columns "id" and "item_name".
#' "id" is the primary key, and automatically increamented.  "item_name" holds the name of item.
#' 
#' 
#' trySelect(conn=conn, 
#'           columns=c("id", "item_name"),
#'           spec_list=c("INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE",
#'                       "VARCHAR(20) NOT NULL"))
#'           
#' 
#' Note: he tablename and the column names are automatically quoted.
#'
tryCreateTable <- function(conn, tablename, column_specs, max_failures=MAX_FAILURES){
    q_str <- paste("CREATE TABLE", enQuote(tablename), enParen(paste(column_specs,collapse=",")))
    return(trySendQuery(conn, q_str, max_failures))
}
tryCreateTempTable <- function(conn, tablename, column_specs, max_failures=MAX_FAILURES){
    q_str <- paste("CREATE TEMP TABLE", enQuote(tablename), enParen(paste(column_specs,collapse=",")))
    return(trySendQuery(conn, q_str, max_failures))
}
tryCreateTableIfNotExists <- function(conn, tablename, column_specs, max_failures=MAX_FAILURES){
    q_str <- paste("CREATE TABLE IF NOT EXISTS", enQuote(tablename), enParen(paste(column_specs, collapse=",")))
    return(trySendQuery(conn, q_str, max_failures))
}
tryDrop <- function(conn, tablename, max_failures=MAX_FAILURES){
    q_str <- paste("DROP TABLE IF EXISTS", tablename)
    return(trySendQuery(conn, q_str, max_failures))
}
tryCreateTempTableIfNotExists <- function(conn, tablename, column_specs, max_failures=MAX_FAILURES){
    q_str <- paste("CREATE TEMP TABLE IF NOT EXISTS", enQuote(tablename), enParen(paste(column_specs, collapse=",")))
    return(trySendQuery(conn, q_str, max_failures))
}
tryInsert <- function(conn, tablename, columns, values, max_failures=MAX_FAILURES){
    q_str <- paste("INSERT INTO", enQuote(tablename), enParen(paste(enQuote(columns), collapse=",")), "VALUES", 
                   paste("(", paste(values, collapse=",")), ")")
    return(trySendQuery(conn, q_str, max_failures))
}
tryInsertOrReplace <- function(conn, tablename, columns, values, max_failures=MAX_FAILURES){
    q_str <- paste("INSERT OR REPLACE INTO", enQuote(tablename), enParen(paste(enQuote(columns), collapse=",")), "VALUES", 
                   paste("(", paste(values, collapse=",")), ")")
    return(trySendQuery(conn, q_str, max_failures))
}
tryUpdate <- function(conn, tablename, keyname, keyval, columns, values, max_failures=MAX_FAILURES){
    assert.equal(length(columns), length(values))
    to_exclude <- which(columns==keyname)
    pairs <- paste(enQuote(columns),"=",values,sep="")
    q_str <- paste("UPDATE", enQuote(tablename), "SET", paste(pairs,collapse=","), "WHERE", enQuote(keyname), "=", keyval)
    return(trySendQuery(conn, q_str, max_failures))
}
# value: data.frame, each frame contains the list of values for a variable.
tryBulkInsert <- function(conn, tablename, columns, values, max_failures=MAX_FAILURES){
    q_str <- paste("INSERT INTO", enQuote(tablename), enParen(paste(enQuote(columns), collapse=",")), "VALUES", 
                   paste(apply(values, 1, FUN=function(record){ enParen(paste(record, collapse=",")) }), collapse=","))
    return(trySendQuery(conn, q_str, max_failures))
}
# value: data.frame, each frame contains the list of values for a variable.
tryBulkInsertOrReplace <- function(conn, tablename, columns, values, max_failures=MAX_FAILURES){
    q_str <- paste("INSERT OR REPLACE INTO", enQuote(tablename), enParen(paste(enQuote(columns), collapse=",")), "VALUES", 
                   paste(apply(values, 1, FUN=function(record){ enParen(paste(record, collapse=",")) }), collapse=","))
    return(trySendQuery(conn, q_str, max_failures))
}
# value: data.frame, each frame contains the list of values for a variable.
tryBulkUpdate <- function(conn, tablename, keyname, keyval, columns, values, max_failures=MAX_FAILURES){
    assert.equal(length(columns), length(values))
    to_exclude <- which(columns==keyname)
    pairs <- paste(enQuote(columns),"=",values,sep="")
    q_str <- paste("UPDATE", enQuote(tablename), "SET", paste(pairs,collapse=","), "WHERE", enQuote(keyname), "=", keyval)
    return(trySendQuery(conn, q_str, max_failures))
}
# value: data.frame, each frame contains the list of values for a variable.
tryBulkInsertOrUpdate <- function(conn, tablename, keyname, columns, values, max_failures=MAX_FAILURES) {
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

trySendQuery <- function(conn, q_str, max_failures=MAX_FAILURES){
    logger.debug(q_str)
    q_str <- na2null(q_str)
    logger.debug(q_str)
    for( nfailure in seq(1, max_failures)) {
        tryCatch({
            dbSendQuery(conn, q_str)
            break
        }, error = function(msg){
            if( nfailure > max_failures){
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
tryGetQuery <- function(conn, q_str, max_failures=MAX_FAILURES){
    logger.debug(q_str)
    q_str <- na2null(q_str)
    logger.debug(q_str)
    ret <- NULL
    for( nfailure in seq(1, max_failures)) {
        tryCatch({
            ret <- dbGetQuery(conn, q_str)
            break
        }, error = function(msg){
            if( nfailure > max_failures){
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
