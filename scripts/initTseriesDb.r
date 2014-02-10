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
    source("create_year_summary.r")
    source("enQuote.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)    
initTseriesDb <- function( conn, config) {

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
    catalog.column.name <- c("tablename",
                             "factset_id",
                             "fql",
                             "usd",
                             "local",
                             "earliest",
                             "latest")
    
    # Company & contry tables
    for( fsid in universe ) {
        dbSendQuery(conn, "BEGIN")
        
        tryCatch({
            # Get company meta data
            company <- FF.ExtractDataSnapshot(fsid, paste(company.meta.fql, collapse=","))
            if( is.empty(company) ){
                stop(paste("Empty company meta data:", fsid))
            }
            if( ncol(company) != length(company.meta.titles) ) {
                stop(paste("Collapted company meta data:", fsid))
            }
            colnames(company) <- company.meta.titles 
            
            # Extract country info and register to the country table
            
            # See if the country is already registered
            country_id <- trySelect(conn, "country", c("country_id"), paste("country_id=",enQuote(company$country_id), sep=""))
            if( is.empty(country_id) ){
                tryInsert(conn,
                          "country", 
                          country.column.name, 
                          c(enQuote(company$country_id),
                            enQuote(company$country),
                            enQuote(company$region),
                            enQuote(company$exchange),
                            enQuote(company$curr),
                            enQuote(company$curr_code),
                            enQuote(market)
                          )
                )
                logger.info(paste("Registered to the country table:", company$country))
                country_id <- company$country_id
            }
            if( is.empty(country_id) ){
                logger.error(paste("Country ID is missing:", fsid, fql))
                stop()
            }
            if( is.empty(company$id)
                || is.empty(company$name)
                || is.empty(company$sector)
                || is.empty(company$indgrp)
                || is.empty(company$industry)
                || is.empty(company$subind) ) {
                logger.error(paste("Something is missing to register the country to the country table:", country_id))
                stop()
            }
            columns <- c("factset_id",
                         "company_name",
                         "country_id"
            )
            
            values <- c(enQuote(company$id),
                        enQuote(company$name),
                        enQuote(country_id)
            )
            if( !is.na(company$sector) ){
                columns <- c(columns, "sector")
                values <- c(values, enQuote(company$sector))
            }
            if( !is.na(company$indgrp) ){
                columns <- c(columns, "indgrp")
                values <- c(values, enQuote(company$indgrp))
            }
            if( !is.na(company$industry) ){
                columns <- c(columns, "industry")
                values <- c(values, enQuote(company$industry))
            }
            if( !is.na(company$subind) ){
                columns <- c(columns, "subind")
                values <- c(values, enQuote(company$subind))
            }
            # Register to the company table
            tryInsertOrReplace(conn, 
                               "company", 
                               columns, values)
            logger.info(paste("Registered to the company table:", company$id, company$name))
            # Create per-T series tables
        dbSendQuery(conn, "COMMIT")
                 logger.info(paste("[COMPANY] Committed", fsid))
        }, error=function(msg){
            logger.error(paste("Rolling back:", msg))
            dbSendQuery(conn, "ROLLBACK")
        })
    }
    ########################################
    # Create fql-company tables
    ########################################
    tseries.column.name <- c("date", "usd", "local")
    tseries.column.type <- c("INTEGER PRIMARY KEY NOT NULL UNIQUE", 
                             "FLOAT", 
                             "FLOAT")
    tseries.specs <- paste(tseries.column.name, tseries.column.type)    
    for( fql in fql_list ){
        
        controls <- get(paste(fs_prefix, fql, sep=""), envir=config)
        curr_list <- c(tolower(default_currency))
        freq_list <- c()
        
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
        if( all("D" %in% controls) ) {
            freq_list <- cbind(freq_list, "D")
        }
        if( all("M" %in% controls) ) {
            freq_list <- cbind(freq_list, "M")
        }
        if( all("Q" %in% controls) ) {
            freq_list <- cbind(freq_list, "Q")
        }
        if( all("Y" %in% controls) ) {
            freq_list <- cbind(freq_list, "Y")
        }
        for( fsid in universe ){
            trySendQuery(conn, "BEGIN")
            
            tablename <- paste(fql, fsid, sep="-")
            
            tryCreateTable(conn, tablename, tseries.specs)
            logger.info(paste("Created tseries table:", tablename, tseries.specs))
            
            for( freq in freq_list ){
                ret <- trySelect(conn, "fql", c("syntax"), c(paste("fql=",enQuote(fql))))
                fql_syntax <- ret$syntax
                master_data <- NULL
                for( curr in curr_list ){
                    fs_str2 <- paste(fs.t0, fs.t1, freq, sep=":")
                    data <- tryExtract(fql_syntax, fsid, fs.t0, fs.t1, freq, curr)
                    if( is.empty(data) ){
                        logger.warn(paste("No data:", fsid, ",", fql, " ..., skipping..."))
                        stop()
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
                do_skip <- TRUE
                for( j in seq(1,nrow(master_data)) ){
                    if( all(is.na(master_data[j,][3:ncol(master_data)]) ) ){
                        if(do_skip){
                            filter[j] <- FALSE
                        } else{
                            filter[j] <- TRUE
                        }
                    } else{
                        #do_skip <- FALSE
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
                    logger.error(paste("Some date string must be mulformed:", filtered_data[[2]]))
                    next
                }, error=function(msg){
                    logger.error(paste("Some date string must be mulformed:", filtered_data[[2]]))
                    next
                })
                         
                
                # Populate the t-series table, chunk at a time
                BINSIZE <- 100
                for( begin in seq(1, nrow(filtered_data), BINSIZE) ){
                    end <- min(nrow(filtered_data), begin+BINSIZE-1)
                    values <- NULL
                    if( ncol(filtered_data) > 3 ){
                        values <- data.frame(julianday(as.Date(filtered_data[begin:end,][[2]])),
                                        filtered_data[begin:end,][c(3,4)])
                    } else {
                        values <- data.frame(julianday(as.Date(filtered_data[begin:end,][[2]])),
                                        filtered_data[begin:end,][c(3)])
                    }
                    
                    tryBulkInsertOrReplace(conn, 
                                       tablename, 
                                       c("date", curr_list), 
                                       values
                    )
                }
                # Register to catalog
                earliest <- julianday(as.Date(filtered_data[1,2]))
                latest <- julianday(as.Date(filtered_data[nrow(filtered_data),2]))
                columns <- c("tablename","factset_id","fql","usd","local","earliest","latest")
                values <- data.frame(enQuote(tablename),
                                     enQuote(c(fsid)),
                                     enQuote(c(fql)),
                                     c( ifelse( "usd" %in% curr_list, 1, 0) ),
                                     c( ifelse( "local" %in% curr_list, 1, 0) ),
                                     earliest,
                                     latest
                )
                tryBulkInsert(conn, "catalog", columns, values)
                logger.info(paste("Registered to CATALOG table:", fsid, ",", fql))
            }
            dbSendQuery(conn, "COMMIT")
            logger.info(paste("Commited on", fsid))
        }, error=function(msg){
            logger.error(paste("Rolling back:", msg))
            dbSendQuery(conn, "ROLLBACK")
        })
    }
    
    return(0)
}