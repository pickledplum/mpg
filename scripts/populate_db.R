# populate db
# Download specified FS parameters
library(tools)
library(xts)
library(RSQLite)

source("/home/honda/mpg/scripts/read_config.r")
db <<- "D:/home/honda/sqlite-db/financial.sqlite3"

# Max number of db write access trials before bailing out
MAX_TRIALS <<- 5

populate_db <- function(config_file) {
    
    on.exit(exit_f)
    ########################################
    # Load config
    ########################################
    config <- read_config(config_file) # returns an environment
    
    source_root <- ""
    universe <- c()
    prefix <- ""
    default_currency <- ""
    
    stopifnot( exists("OUTPUT_ROOT", envir=config) )
    source_root <- get("OUTPUT_ROOT", mode="character", envir=config) # "D:/home/honda/mpg/dummy/fs_output"
    
    stopifnot(exists("UNIVERSE", envir=config) )
    universe_file <- get("UNIVERSE", mode="character", envir=config) # "list_of_constituents.txt"
    print(paste("Opening:", universe_file))
    stopifnot(file.exists(universe_file))
    unverse_conn <- file(universe_file, open="r", blocking=FALSE)
    temp <- read.table(unverse_conn, header=TRUE, strip.white=TRUE, blank.lines.skip=TRUE, comment.char="#")
    universe <-temp[[1]]
    close(unverse_conn)

    if( exists("PREFIX", envir=config) ){
        prefix <- get("PREFIX", mode="character", envir=config) # "dummy"
    }
    
    stopifnot( exists("T0", envir=config) )
    t0 <- get("T0", mode="character", envir=config) # "1980-01-01" NOTE: implement "last"?
    
    stopifnot( exists("T1", envir=config) )
    t1 <- get("T1", mode="character", envir=config) # "1980-12-12" "now"
    
    stopifnot( exists("DEFAULT_CURRENCY", envir=config) )
    default_currency <- get("DEFAULT_CURRENCY", mode="character", envir=config) # USD
    
    stopifnot( file.exists(source_root) )
    print(paste("source_root:", source_root))
    
    #assertWarning( length(universe)>0, verbose=TRUE)
    if( length(universe) == 0){
        print("Warning: Empty universe.  Nothing to do.  Exiting normally...")
        return(0)
    }
    print(paste("UNIVERSE(", length(universe), "): ", paste(universe, collapse=",")))
    if( is.na(prefix) ) {
        print("Warning: No prefix to output files")
    } else{
        print(paste("PREFIX:", prefix))
    }
    
    stopifnot( !is.na(t0))
    stopifnot( !is.na(t1) )
    print(paste("T0,T1:", t0,",",t1))
    stopifnot( !is.na(default_currency) )
    print(paste("DEFAULT_CURRENCY:", default_currency))

    ########################################
    # Extract FactSet parameter strings
    # Prefixed with "FACTSET_"
    ########################################

    fs_prefix <- get("FACTSET_PREFIX", envir=config)
    fs_prefix_pattern <- paste("^", fs_prefix, sep="")
    config_param_list <- grep(fs_prefix_pattern, ls(config), value=TRUE)
    config_param_list <- config_param_list[-c(which(config_param_list=="FACTSET_PREFIX"))]   
    param_list <- gsub(fs_prefix_pattern, "", config_param_list)
    
    print(paste(param_list, collapse=","))
    
    timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
    logger <<- file.path(source_root, paste(timestamp, ".log",sep=""))
    print(paste("Log file:", logger))
    populate_data(universe, param_list, source_root)

}
populate_data <- function(universe, param_list, source_root)
{
    dbconn <<- dbConnect( SQLite(), db )
    table_list <- dbGetQuery(dbconn, "SELECT name FROM sqlite_master WHERE type='table'")
    table_list <- list(table_list[[1]])
    names(table_list) <- table_list
    table_hash <- new.env(hash=TRUE)
    if( length(table_list) > 0 ){

        table_hash <- list2env(table_list, hash=TRUE)
    }
    
    for( company in universe ){
        for( param in param_list ){
        
            filename <- file.path(source_root, param, paste(paste(param, company, sep="-"), ".csv", sep=""))
            if( !file.exists(filename) ){
                log.error(paste("No such file:", filename))
                next
            }
            log.info(paste("Processing", filename, "..."))
        
            tablename <- paste(param, company, sep="-")
            # does the table already exist?
            if( !exists(tablename, inherits=FALSE, envir=table_hash) ){
                q_str <- paste("CREATE TABLE IF NOT EXISTS '", tablename, "'(julian INTEGER PRIMARY_KEY NOT NULL UNIQUE, USD REAL, local REAL)", sep="")
                
                tryCatch({
                    dbSendQuery(dbconn, q_str)
                    log.info(paste("Created a table:", tablename))
                    },
                     error=function(e){
                         log.error(paste("DB faulure:", tablename, ", skipping to the next."))
                         next
                     }
                )
            }

            data <- read.table(filename, sep=",", 
                               as.is=TRUE, header=TRUE, strip.white=TRUE,
                               colClasses=c("character"),
                               quote="")
            
            apply(data, 1, f, tablename)
            log.info(paste("Populated table:", tablename))
        }
    }
    
    
}
f <- function(x, tablename){

    date <- NULL

    tryCatch({
        date <- as.Date(x[1])
        
    }, error=function(msg){
        #ignore
        log.error(msg)
        log.error(paste(tablename, paste(x, collapse=","), sep=":"))
        return(NULL)
    }
    )
    if( class(date) != "Date" ){
        #print(paste("FALSE!!!!", date))
        return(NULL)
    }
    date <- julian(date)
    vals <- as.array(x[2:length(x)])
    if( all(vals=="NA") ){
        log.info("All values are NA, skipping to the next entry")
        next
    }

    vals <- apply(vals, 1, function(val) {
        tryCatch({
            tmp <- as.numeric(val)
            val <- tmp
        }, warning=function(val){
            val <- "NULL"
        })
    })
    
    q_str <- paste("INSERT OR REPLACE INTO ", 
                   "'", tablename, "'",
                   " (", paste("julian", paste(names(vals), collapse=","), sep=","), ")",
                   " VALUES ",
                   " (", paste(date, paste(vals, sep=","), sep=","), ")",
                   sep="")
    
    #log.info(q_str)
    success <- FALSE
    n_trials = 1
    while(!success){
        tryCatch({
                    dbSendQuery(dbconn,q_str)
                    success <- TRUE
                },
                 error=function(e){
                     
                     print(e)
                     if( n_trials <= MAX_TRIALS ){
                        n_trials = n_trials + 1
                        msg <- paste("DB failure:", q_str, ", trying again...")
                        log.warn(msg)
                     } else{
                         msg <- paste("DB failure:", q_str, ", bailing out..")
                         log.error(msg)
                         return(NULL)
                     }
                 }
        )
    }
}  
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

exit_f <- function(){
    dbDisconnect(dbconn)
}

config_file <- "/home/honda/mpg/dummy/params.conf"
populate_db(config_file)