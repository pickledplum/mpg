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
createMetaDb <- function(conn, config) {
    
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
    
    stopifnot( exists("MARKET", envir=config) )
    market <- toupper(get("MARKET", mode="character", envir=config))
    stopifnot( market %in% c("WORLD", "EM", "DM", "FM") )
    
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
    logger.info(paste("MARKET:", market))
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
    country.column.type <- c("CHAR(2) PRIMARY KEY NOT NULL UNIQUE",
                             "TEXT(100)",
                             "TEST(50)",
                             "TEXT(50)",
                             "CHAR(3)",
                             "TEXT(100)",
                             "VARCHAR(25)" )
    country.specs <- paste(country.column.name, country.column.type)
    
    tryCreateTable(conn, "country", country.specs)
    logger.info(paste("Created COUNTRY table:", paste(country.specs, collapse=",")))
    
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
    company.column.type <- c("VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
                             "TEXT(100)", 
                             "CHAR(2)", 
                             "VARCHAR(100)",
                             "VARCHAR(100)",
                             "VARCHAR(100)",
                             "VARCHAR(100)")
    company.column.constraint <- c("FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE")
    company.specs <- c(paste(company.column.name, company.column.type), company.column.constraint)
    tryCreateTable(conn, "company", company.specs)
    logger.info(paste("Created COMPANY table:", paste(company.specs, collapse=",")))
    
    ########################################
    # Create CATEGORY table
    ########################################
    category.column.name <- c("category_id", 
                              "category_descript")
    category.column.type <- c("VARCHAR(50) PRIMARY KEY NOT NULL UNIQUE", 
                              "TEXT(50) NOT NULL UNIQUE")
    category.specs <- paste(category.column.name, category.column.type)
    tryCreateTable(conn, "category", category.specs)
    logger.info("Created CATEGORY table")
    
    category.category_id <- c("company_fund", 
                              "price", 
                              "company_info", 
                              "country_fund")
    category.category_descript <- c("company fundamental",
                                    "price",
                                    "company info",
                                    "country fundamental")
    tryBulkInsertOrReplace(conn, "category", category.column.name, 
                           data.frame(enQuote(category.category_id), 
                                      enQuote(category.category_descript)))
    logger.info(paste("Created CATEGORY table:", paste(category.specs, collapse=",")))
    
    ########################################
    # Create FREQUENCY table
    ########################################
    frequency.column.name <- c("freq", 
                               "freq_name")
    
    frequency.column.type <- c("VARCHAR(1) PRIMARY KEY NOT NULL UNIQUE",
                               "VARCHAR(20) UNIQUE"
    )
    frequency.specs <- paste(frequency.column.name, frequency.column.type)
    
    tryCreateTable(conn, "frequency", frequency.specs)
    logger.info("Created FREQUENCY table")
    
    tryBulkInsertOrReplace(conn, "frequency", 
                           frequency.column.name, 
                           data.frame(enQuote(c("Y","S","Q","M","D")),
                                      enQuote(c("Anuual","Semiannual","Quarterly","Monthly","Daily"))) )
    logger.info(paste("Created FREQUENCY table:", paste(frequency.specs, collapse=",")))
    
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
    fql.column.type <- c("VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
                         "TEXT(200)",
                         "TEXT(100)", 
                         "FLOAT", 
                         "CHAR(1)", 
                         "VARCHAR(50)",
                         "VARCHAR(200)")
    fql.column.constraint <-  c("FOREIGN KEY(category_id) REFERENCES category(category_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
                                "FOREIGN KEY(report_freq) REFERENCES frequency(freq) ON DELETE NO ACTION ON UPDATE CASCADE")
    
    fql.specs <- c(paste(fql.column.name, fql.column.type), fql.column.constraint)
    tryCreateTable(conn, "fql", fql.specs)
    logger.info(paste("Created FQL table:", paste(fql.specs, collapse=",")))
    
    stopifnot( exists("FQL_MAP", envir=config) )
    fql_map_filename <- get("FQL_MAP", envir=config)
    stopifnot(file.exists(fql_map_filename))
    fql_map <- read.csv(fql_map_filename)
    rownames(fql_map) <- fql_map$fql
    tryBulkInsert(conn, "fql", 
                  fql.column.name,
                  data.frame(enQuote2(fql_map$fql),
                             enQuote2(fql_map$syntax),
                             enQuote(fql_map$description),
                             fql_map$unit,
                             enQuote(fql_map$report_freq),
                             enQuote(fql_map$category_id),
                             enQuote(fql_map$note)
                  ))
    
    logger.info(paste("Populated FQL table:", nrow(fql_map)))
    ########################################
    #  Create CATALOG table  
    ########################################
    
    company.meta.fql <- c("FG_COMPANY_NAME",
                          "P_DCOUNTRY",
                          "P_COUNTRY_ISO",    
                          "P_DCOUNTRY(REG)",
                          "P_EXCHANGE",
                          "P_CURRENCY",
                          "P_CURRENCY_CODE",
                          "FE_COMPANY_INFO(ISIN)",
                          "FE_COMPANY_INFO(SEDOL)",
                          "FG_GICS_SECTOR",
                          "FG_GICS_INDGRP",
                          "FG_GICS_INDUSTRY",
                          "FG_GICS_SUBIND")
    
    company.meta.titles <- c(
        "id", 
        "date", 
        "name", 
        "country",
        "country_id",                           
        "region", 
        "exchange", 
        "curr", 
        "curr_code", 
        "isin", 
        "sedol",
        "sector",
        "indgrp",
        "industry",
        "subind")
    catalog.column.name <- c("tablename",
                             "factset_id",
                             "fql",
                             "usd",
                             "local",
                             "earliest",
                             "latest")
    
    catalog.column.type <- c("VARCHAR(41) NOT NULL UNIQUE",
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
    tryCreateTable(conn, "catalog", catalog.specs)
    logger.info(paste("Created CATALOG table:", paste(catalog.specs, collapse=",")))

    BINSIZE = 100
    for( begin in seq(1, length(universe), BINSIZE )){
        end <- min(begin+BINSIZE-1, length(universe))

        mini_universe <- universe[begin:end]
        # Get companies meta data
        all_company <- FF.ExtractDataSnapshot(mini_universe, paste(company.meta.fql, collapse=","))
        if( is.empty(all_company) ){
            stop(paste("Empty company meta data:", mini_universe))
        }
        if( ncol(all_company) != length(company.meta.titles) ) {
            stop(paste("Collapted company meta data within:", mini_universe))
        }
        colnames(all_company) <- company.meta.titles 
    
        # Company & contry tables
        while(!is.empty(mini_universe)) {
            successful <- c()
            
            for( i in seq(1,length(mini_universe)) ) {
                trySendQuery(conn, "BEGIN")
                
                tryCatch({
                    
                    fsid <- mini_universe[i]
                    company <- all_company[i,]   
                    country_id <- NULL
                    if( !is.empty(company$country_id) ){
                        
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
                    }
                    if( is.empty(country_id) ){
                        logger.warn(paste("Country ID is missing:", fsid))
                    }

                    columns <- c("factset_id",
                                 "company_name" )
                    
                    values <- c(enQuote(company$id),
                                enQuote(company$name))
                    
                    if( !is.empty(country_id ) ) {
                        columns <- c(columns, "country_id")
                        values <- c(values, enQuote(country_id))
                    }
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
                    trySendQuery(conn, "COMMIT")
                    logger.info(paste("[COMPANY] Committed", fsid))
                    successful <- c(successful, fsid)
                    
                }, error=function(msg){
                    logger.error(paste("Rolling back:", msg))
                    dbSendQuery(conn, "ROLLBACK")
                    browser()
                })
            }
            mini_universe <- setdiff(mini_universe, successful)
            if( !is.empty(mini_universe) ) logger.warn(paste("Retry:", paste(mini_universe, collapse=",")))
        }
    }
}