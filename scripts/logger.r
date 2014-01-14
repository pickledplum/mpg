# Requires logger (log file fullpath name) defined globally.

log.error <- function( msg){
    t <- format(Sys.time(), "%H:%M:%S")
    con <- file(logger, open="at", blocking=FALSE)
    writeLines(paste(t, "ERROR", "-", msg), con)
    flush(con)
    close(con)
    
}
log.warn <- function( msg ){
    t <- format(Sys.time(), "%H:%M:%S")
    con <- file(logger, open="at", blocking=FALSE)
    writeLines(paste(t, "WARN", "-", msg), con)
    flush(con)
    close(con)
}
log.info <- function( msg ){
    t <- format(Sys.time(), "%H:%M:%S")
    con <- file(logger, open="at", blocking=FALSE)
    writeLines(paste(t, "INFO", "-", msg), con)
    flush(con)
    close(con)
}
