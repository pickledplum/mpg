tryCatch({
    library(tools)
    library(FactSetOnDemand)
    library(xts)
    library(RSQLite)
    
    source("read_config.r")
    source("logger.r")
    source("tryDb.r")
    source("assert.r")
    source("tryExtract.r")
    source("is.empty.r")
    source("drop_tables.r")
    source("julianday.r")
    source("enQuote.r")
    source("extractDirFromPath.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
) 

initTseriesDb <- function(meta_conn, config) {
    
    dbdir <- extractDBDirFromPath(meta_conn)

    ########################################
    # Configure FS
    ########################################
    # Query time out.  Default is 120 secs.  120-3600 secs on client side.
    FactSet.setConfigurationItem( FactSet.TIMEOUT, 900 )
    logger.info(paste("FactSet timetout:", 900, "secs"))
    
    ########################################
    # Load config
    ########################################
    
    portfolio_ofdb <- ""
    universe <- c("character")
    t0 <- ""
    t1 <- ""
    default_currency <- ""
    
    stopifnot( exists("T0", envir=config) )
    t0 <- get("T0", mode="character", envir=config) # "1980-01-01" NOTE: implement "last"?
    stopifnot(grepl("[[:digit:]]+{4}-[[:digit:]]+{2}-[[:digit:]]+{2}", t0))
    fs.t0 <- gsub("-", "", t0)
    
    stopifnot( exists("T1", envir=config) )
    t1 <- get("T1", mode="character", envir=config) # "1980-12-12" "now"
    stopifnot(grepl("[[:digit:]]+{4}-[[:digit:]]+{2}-[[:digit:]]+{2}", t1))
    fs.t1 <- gsub("-", "", t1)
    
    stopifnot( exists("DEFAULT_CURRENCY", envir=config) )
    default_currency <- get("DEFAULT_CURRENCY", mode="character", envir=config) 
    default_currency <- toupper(default_currency)
    stopifnot( default_currency %in% c("USD","LOCAL") )
    
    stopifnot( exists("INDEX", envir=config) )
    finance.index <- get("INDEX", mode="character", envir=config)
    
    if( exists("PORTFOLIO_OFDB", envir=config) ){
        portfolio_ofdb <- get("PORTFOLIO_OFDB", mode="character", envir=config) # "PERSONAL:HONDA_MSCI_ACWI_ETF"
        stopifnot( !is.null(portfolio_ofdb) && portfolio_ofdb != "" )
        data <- FF.ExtractOFDBUniverse(portfolio_ofdb, "0O")
        logger.info(paste("Loaded universe from", portfolio_ofdb))
        universe <- data$Id
        
    } else if( exists("UNIVERSE", envir=config) ){
        universe_file <- get("UNIVERSE", mode="character", envir=config) # "list_of_constituents.txt"
        stopifnot( !is.null(universe_file) && universe_file != "" )
        stopifnot( file.exists(universe_file))
        unicon <- file(universe_file, open="r", blocking=FALSE)
        temp <- read.table(unicon, colClasses=c("character"), header=TRUE, strip.white=TRUE, blank.lines.skip=TRUE, comment.char="#")
        universe <-temp[[1]]
        close(unicon)
        logger.info(paste("Loaded universe from file:", universe_file))
        
    } else{
        #error
        stop(paste("Either", "PORTFOLIO_OFDB", "or", "UNIVERSE", "must be specified"))
    }
    if( length(universe) == 0){
        logger.warn("Empty universe.  Nothing to do.  Exiting normally...")
        return(0)
    }
    
    logger.info(paste("UNIVERSE(", length(universe), "): ", paste(universe, collapse=",")))
    logger.info(paste("T0,T1:", t0,",",t1))
    logger.info(paste("DEFAULT_CURRENCY:", default_currency))
    logger.info(paste("INDEX:", finance.index))
    
    ########################################
    # Extract FactSet FQL strings
    # Prefixed with "FACTSET_"
    ########################################
    stopifnot( exists("FACTSET_PREFIX", envir=config) )
    fs_prefix <- get("FACTSET_PREFIX", envir=config)
    logger.info(paste("FACTSET_PREFIX:", fs_prefix))
    
    fs_prefix_pattern <- paste("^", fs_prefix, sep="")
    config_fql_list <- grep(fs_prefix_pattern, ls(config), value=TRUE) 
    fql_list <- gsub(fs_prefix_pattern, "", config_fql_list)
    logger.info(paste("FACTSET items:", paste(fql_list, collapse=",")))
    
    ########################################
    # Create COUNTRY Table
    ########################################
    country.column.name <- c("country_id",
                             "country",
                             "region",
                             "exchange",
                             "curr", 
                             "curr_iso", 
                             "market")
    
    ########################################
    # Create COMPANY table
    ########################################
    company.column.name <- c("factset_id", 
                             "company_name", 
                             "country_id", 
                             "sector", 
                             "indgrp", 
                             "industry", 
                             "subind")
    
    
    ########################################
    # Create FQL table
    ########################################
    fql.column.name <- c("fql", 
                     "syntax", 
                     "description", 
                     "unit", 
                     "report_freq",
                     "category_id", 
                     "note")
    ########################################
    #  Create CATALOG table  
    ########################################
    catalog.column.name <- c("dbname",
                             "tablename",
                             "factset_id",
                             "fql",
                             "usd",
                             "local",
                             "earliest",
                             "latest")
    
    ########################################
    # Create fql-company tables
    ########################################
    tseries.column.name <- c("date", "usd", "local")
    tseries.column.type <- c("INTEGER PRIMARY KEY NOT NULL UNIQUE", 
                             "FLOAT", 
                             "FLOAT")
    tseries.specs <- paste(tseries.column.name, tseries.column.type)    
    
    ########################################
    # Create CATALOG tables
    ########################################
    catalog.column.name <- c("dbname",
                             "tablename",
                             "factset_id",
                             "fql",
                             "usd",
                             "local",
                             "earliest",
                             "latest")
    
    catalog.column.type <- c("VARCHAR(30) NOT NULL",
                             "VARCHAR(41) NOT NULL UNIQUE",
                             "VARCHAR(20) NOT NULL",
                             "VARCHAR(20) NOT NULL",
                             "INTEGER",
                             "INTEGER",
                             "INTEGER",
                             "INTEGER")
    catalog.column.constraint <- c("PRIMARY KEY(factset_id, fql)",
                                   "FOREIGN KEY(factset_id) REFERENCES company(factset_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
                                   "FOREIGN KEY(fql) REFERENCES fql(fql) ON DELETE NO ACTION ON UPDATE CASCADE")
    catalog.specs <- c(paste(catalog.column.name, catalog.column.type), catalog.column.constraint)
    #tryDrop(meta_conn, "catalog")
    tryCreateTableIfNotExists(meta_conn, "catalog", catalog.specs)
    logger.info(paste("Created CATALOG table:", paste(catalog.specs, collapse=",")))
    tseries_dbpath_list <- c()
    
    # Remember objs to keep so far
    keep1 <- c("dbdir", 
               "t0", 
               "fs.t0", 
               "t1", 
               "fs.t1", 
               "default_currency",
               "finance.index", 
               "universe", 
               "fs_prefix", 
               "fql_list",
               "country.column.name", 
               "company.column.name", 
               "fql.column.name", 
               "catalog.column.name", 
               "tseries.column.name",
               "tseries.column.type", 
               "tseries.specs", 
               "catalog.column.name", 
               "catalog.column.type", 
               "catalog.column.constraint",
               "catalog.specs", 
               "tseries_dbpath_list")
    
    for( fql in fql_list ){

        dbname <- paste(fql, ".sqlite", sep="")
        db_filepath <- file.path(dbdir, dbname)
        tseries_dbpath_list <- c(tseries_dbpath_list, db_filepath)
        this_fql_dbconn <- dbConnect( SQLite(), db_filepath)
        logger.info(paste("Connected to db:", db_filepath))
       
        ret <- trySelect(meta_conn, "fql", c("syntax"), c(paste("fql=",enQuote(fql))))
        fql_syntax <- ret$syntax
        #dbClearResult(ret)
        
        controls <- get(paste(fs_prefix, fql, sep=""), envir=config)
        curr_list <- c(tolower(default_currency))

        if( all("LOCAL" %in% controls) ) {
            if( default_currency != "LOCAL" ){
                curr_list <- cbind(curr_list, "local")
            }
        }
        if( all("USD" %in% controls) ){
            if( default_currency != "USD" ){
                curr_list <- cbind(curr_list, "usd")
            }
        }
        
        freq <- "Y"
        if( all("D" %in% controls) ) {
            freq <- "D"
        } else if( all("M" %in% controls) ) {
            freq <- "M"
        } else if( all("Q" %in% controls) ) {
            frqe <- "Q"
        } else if( all("Y" %in% controls) ) {
            freq <- "Y"
        }

        # Objs to keep in the FQL params loop
        keep2 <- c("dbname", 
                   "fql_syntax", 
                   "controls", 
                   "curr_list", 
                   "freq")
        
        i_fsid <- 1
        UBINSIZE=50
        for(ubegin in seq(1, length(universe), UBINSIZE ) ) {
            uend <- min(ubegin+UBINSIZE-1, length(universe))
            
            page_str <- paste("[", ubegin, "-", uend, "]/", length(universe), sep="")           
            mini_universe <- universe[ubegin:uend]
            local_data <- NULL
            usd_data <- NULL

            for( curr in curr_list ){
                # the list of data frames.  Each frame has three columns ("Id", "Date", fsid)
                data_list <- tryBulkExtract(fql_syntax, mini_universe, fs.t0, fs.t1, freq, curr)
                if( is.empty(data_list) ){
                    logger.warn(paste(page_str, ") No data:", fql, " ..., skipping..."))
                    #browser()
                    next
                }
                
                if( curr == "usd" ){
                    usd_data <- data_list
                } else if( curr == "local" ){
                    local_data <- data_list
                } else{
                    #browser()
                    stop(paste("Unsupported currency:", curr))
                }
            }
            if( is.empty(usd_data) && is.empty(local_data) ){
                logger.error(paste(page_str, ") Extraction failed.  Skipping this bin:", 
                                   fql_syntax, fs.t0, fs.t1, freq, paste(curr_list, ",")))
                next
            }

            for( fsid in mini_universe ){           
                master_data <- NULL
                is_data_ready <- FALSE
                tryCatch({  
                    for( curr in curr_list ){
                        if( curr == "local" ){
                            if( !is.empty(local_data) && fsid %in% names(local_data) ){
                                data <- local_data[[fsid]]
                            } else{
                                logger.warn(paste("No local data for", fsid))
                            }
                        } else if( curr == "usd" ){
                            if( !is.empty(usd_data) && fsid %in% names(usd_data) ){
                                data <- usd_data[[fsid]]
                            } else{
                                logger.warn(paste("No USD data for", fsid))
                            }
                        } else{
                            logger.error(paste("Unsupported currency: [", curr, "]", sep=""))
                            next
                        }
                        colnames(data) <- c("id", "date", curr)
                        if( is.empty(master_data) ){
                            master_data <- data
                        } else {
                            master_data <- merge(master_data, data, by=c("id", "date"))
                        }   
                    }
                    if( is.empty(master_data) ){
                        logger.warn(paste("No data from", t0, "to", t1, ":", fsid, ",", fql))
                        next
                    }
                    filter <- vector("logical", nrow(master_data))
                    for( j in seq(1,nrow(master_data)) ){
                        if( all(is.na(master_data[j,][3:ncol(master_data)]) ) ){
                            filter[j] <- FALSE
                        } else{
                            filter[j] <- TRUE
                        }
                    }
                            
                    filtered_data <- master_data[filter,]
                    if( is.empty(filtered_data) ){
                        logger.warn(paste("No data after filtering out NULL from", t0, "to", t1, ":", fsid, ",", fql))
                        next
                    }
                    tryCatch({
                        as.Date(filtered_data[[2]])
                    }, warning=function(msg){
                        browser()
                        logger.error(paste("Some date string must be mulformed:", filtered_data[[2]]))
                        stop(msg)
                    }, error=function(msg){
                        browser()
                        logger.error(paste("Some date string must be mulformed:", filtered_data[[2]]))
                        stop(msg)
                    })
                    is_data_ready <- TRUE
                }, error=function(msg){
                    logger.error(msg)
                })
                
                if( ! is_data_ready ) next
                
                tryCatch({                    
                    trySendQuery(this_fql_dbconn, "BEGIN")
                    trySendQuery(meta_conn, "BEGIN")
                    
                    tablename <- paste(fql, fsid, sep="-")
                    tryCreateTableIfNotExists(this_fql_dbconn, tablename, tseries.specs)
                    logger.info(paste("Created tseries table:", tablename, paste(tseries.specs, collapse=",")))
                    
                    # Populate the t-series table, chunk at a time
                    TSERIES_BINSIZE <- 100
                    for( tbegin in seq(1, nrow(filtered_data), TSERIES_BINSIZE) ){
                        tend <- min(nrow(filtered_data), tbegin+TSERIES_BINSIZE-1)
                        values <- NULL
                        if( ncol(filtered_data) > 3 ){
                            values <- data.frame(julianday(as.Date(filtered_data[tbegin:tend,][[2]])),
                                            filtered_data[tbegin:tend,][c(3,4)])
                        } else {
                            values <- data.frame(julianday(as.Date(filtered_data[tbegin:tend,][[2]])),
                                            filtered_data[tbegin:tend,][c(3)])
                        }
                        
                        tryBulkInsertOrReplace(this_fql_dbconn, 
                                           tablename, 
                                           c("date", curr_list), 
                                           values
                        )
                    }
                    
                    # Register to catalog

                    earliest <- julianday(as.Date(filtered_data[1,2]))
                    latest <- julianday(as.Date(filtered_data[nrow(filtered_data),2]))
                    columns <- c("dbname", "tablename","factset_id","fql","usd","local","earliest","latest")
                    values <- data.frame(enQuote(dbname),
                                         enQuote(tablename),
                                         enQuote(c(fsid)),
                                         enQuote(c(fql)),
                                         c( ifelse( "usd" %in% curr_list, 1, 0) ),
                                         c( ifelse( "local" %in% curr_list, 1, 0) ),
                                         earliest,
                                         latest
                    )
                    tryBulkInsertOrReplace(meta_conn, "catalog", columns, values)
                    logger.info(paste("Registered to CATALOG table:", fsid, ",", fql))
        
                    
                    trySendQuery(this_fql_dbconn, "COMMIT")
                    trySendQuery(meta_conn, "COMMIT")
                    logger.info(paste("Commited on", fsid, "(", i_fsid, "/", length(universe), ")"))
                    
                }, error=function(msg){
                    logger.error(paste("Rolling back:", msg))
                    trySendQuery(this_fql_dbconn, "ROLLBACK")
                    trySendQuery(meta_conn, "ROLLBACK")
                }, finally = function(){
                    to_remove <- ls()
                    
                }
                )
                i_fsid <- i_fsid+1
            }
        }
        for(pending_result in dbListResults(this_fql_dbconn)){
            dbClearResult(pending_result)
        }
        dbDisconnect(this_fql_dbconn)
        all_objs <- ls()
        to_remove <- all_objs[-which(c(all_objs %in% c(keep1, keep2)))]
        remove(to_remove)
     }
     
     return(tseries_dbpath_list)
 }