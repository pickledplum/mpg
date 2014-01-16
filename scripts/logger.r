# Requires logger (log file fullpath name) defined globally.
logger.DEBUG=2
logger.INFO=3
logger.WARN=4
logger.ERROR=5

logger.logfile <- NULL
logger.level <- logger.INFO
logger.do_stdout <- TRUE

logger.init <- function(level, do_stdout=TRUE, logfile=NULL) {
    logger.logfile <- logfile
    logger.level <- level
    logger.do_stdout <- do_stdout
}

logger.error <- function(msg){
    logger.logmsg(logger.ERROR, msg)
}
logger.warn <- function(msg){
    logger.logmsg(logger.WARN, msg)
}
logger.info <- function(msg){
    logger.logmsg(logger.INFO, msg)
}
logger.debug <- function(msg){
    logger.logmsg(logger.DEBUG, msg)
}
logger.logmsg <- function(level, msg){
    t <- format(Sys.time(), "%H:%M:%S")
    level_str <- ""
    if( level==logger.DEBUG ){
        level_str <-"DEBUG"
    } else if( level==logger.INFO ){
        level_str <-"INFO"
    } else if( level==logger.WARN ){
        level_str <- "WARN"
    } else if( level==logger.ERROR ){
        level_str <- "ERROR"
    } else {
        level_str <- "???"
    }
    msggg <- paste(t, level_str, "-", msg)
    if( level >= logger.level ){
        if( !is.null(logger.logfile) ){
            con <- file(logger.logfile, open="w+", blocking=FALSE)
            writeLines(msggg, con)
                
        }
        if( logger.do_stdout ){
            writeLines(msggg, stdout())
        }
    }
}