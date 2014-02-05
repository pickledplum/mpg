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
createdb <- function( conn, config) {

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
    # Create Country Table
    ########################################
    #q_str <- "CREATE TABLE IF NOT EXiSTS country (country_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, country TEXT(100), region TEXT(50), exchange TEXT(50), curr_iso VARCHAR(3), curr TEXT(100), market VARCHAR(25))"
    #stopifnot(trySendQuery(conn, q_str))
    specs <- c("country_id CHAR(2) PRIMARY KEY NOT NULL UNIQUE", 
               "country TEXT(100)", 
               "region TEST(50)", 
               "exchange TEXT(50)", 
               "curr_iso VARCHAR(3)", 
               "curr TEXT(100)", 
               "market VARCHAR(25)" )
    tryCreateTable(conn, "country", specs)
    logger.info("Created COUNTRY table")
    ########################################
    # Create COMPANY table
    ########################################
    #q_str <- "CREATE TABLE IF NOT EXISTS company (factset_id VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE, company_name TEXT(100), country_id INTEGER, sector VARCHAR(100),indgrp VARCHAR(100),industry VARCHAR(100),subind VARCHAR(100),FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE)"
    #stopifnot(trySendQuery(conn, q_str))
    specs <- c("factset_id VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
               "company_name TEXT(100)", 
               "country_id CHAR(2)", 
               "sector VARCHAR(100)",
               "indgrp VARCHAR(100)",
               "industry VARCHAR(100)",
               "subind VARCHAR(100)",
               "FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE"
    )
    
    tryCreateTable(conn, "company", specs)
    logger.info("Created COMPANY table")
    
    ########################################
    # Create CATEGORY table
    ########################################
    #q_str <- "CREATE TABLE IF NOT EXISTS category (category_id VARCHAR(30) PRIMARY KEY NOT NULL UNIQUE, category_descrip TEXT(50) NOT NULL UNIQUE)"
    #trySendQuery(conn, q_str)
    specs <- c("category_id VARCHAR(50) PRIMARY KEY NOT NULL UNIQUE", 
               "category_descript TEXT(50) NOT NULL UNIQUE"
    )
    tryCreateTable(conn, "category", specs)
    logger.info("Created CATEGORY table")
    
    #q_str <- "INSERT OR REPLACE INTO category (category_id, category_descript) VALUES ('company_fund', 'Company fundamental'), ('price', 'Price'), ('company_info', 'Company info'), ('country_fund', 'Country fundamental')"
    #stopifnot(trySendQuery(conn, q_str))
    tryBulkInsertOrReplace(conn, "category", c("category_id", "category_descript"), 
                       data.frame(enQuote(c("company_fund", "price", "company_info", "country_fund")), 
                                  enQuote(c("company fundamental","price","company info","country fundamental"))))
    logger.info("Populated CATEGORY table")
    
    ########################################
    # Create FREQUENCY table
    ########################################
    #q_str <- "CREATE TABLE IF NOT EXISTS frequency (freq VARCHAR(1) PRIMARY KEY NOT NULL UNIQUE, freq_name VARCHAR(20) UNIQUE)"
    #trySendQuery(conn, q_str)
    specs <- c("freq VARCHAR(1) PRIMARY KEY NOT NULL UNIQUE",
               "freq_name VARCHAR(20) UNIQUE"
    )
    tryCreateTable(conn, "frequency", specs)
    logger.info("Created FREQUENCY table")
    #q_str <- "INSERT OR REPLACE INTO frequency (freq, freq_name) VALUES ('Y','Anuual'),('S','Semiannual'),('Q','Quarterly'),('M','Monthly'),('D','Daily')"
    #stopifnot(trySendQuery(conn, q_str))
    tryBulkInsertOrReplace(conn, "frequency", c("freq", "freq_name"), 
    data.frame(enQuote(c("Y","S","Q","M","D")),enQuote(c("Anuual","Semiannual","Quarterly","Monthly","Daily"))) )
    logger.info("Populated FREQUENCY table")
    
    ########################################
    # Create FQL table
    ########################################
    #q_str <- "CREATE TABLE IF NOT EXISTS fql (fql VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE, description TEXT(100), unit FLOAT, freq CHAR(1), note TEXT(200), category_id INTEGER, FOREIGN KEY(category_id) REFERENCES category(category_id) ON DELETE NO ACTION ON UPDATE CASCADE, FOREIGN KEY(freq) REFERENCES frequency(freq) ON DELETE NO ACTION ON UPDATE CASCADE )"
    #stopifnot(!trySendQuery(conn, q_str))
    specs <- c("fql VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
               "syntax TEXT(200)",
               "description TEXT(100)", 
               "unit FLOAT", 
               "report_freq CHAR(1)", 
               "category_id VARCHAR(50)", 
               "note TEXT(200)", 
               "FOREIGN KEY(category_id) REFERENCES category(category_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
               "FOREIGN KEY(report_freq) REFERENCES frequency(freq) ON DELETE NO ACTION ON UPDATE CASCADE"
    )
    tryCreateTable(conn, "fql", specs)
    logger.info("Created FQL table")
    
    stopifnot( exists("FQL_MAP", envir=config) )
    fql_map_filename <- get("FQL_MAP", envir=config)
    stopifnot(file.exists(fql_map_filename))
    fql_map <- read.csv(fql_map_filename)
    rownames(fql_map) <- fql_map$fql
    tryBulkInsert(conn, "fql", 
                  c("fql","syntax","description","unit","report_freq","category_id","note"),
                  data.frame(enQuote2(fql_map$fql),
                             enQuote2(fql_map$syntax),
                             enQuote(fql_map$description),
                             fql_map$unit,
                             enQuote(fql_map$report_freq),
                             enQuote(fql_map$category_id),
                             enQuote(fql_map$note)
                  ))

    logger.info("Populated FQL table")
    ########################################
    # Company basics 
    ########################################
    
    fs_company_info_list <- c("FG_COMPANY_NAME",
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
    
    fs_company_meta_colnames <- c("id", 
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
    ########################################
    ########################################
    specs <- c("tablename VARCHAR(41) NOT NULL UNIQUE",
               "factset_id VARCHAR(20) NOT NULL",
               "fql VARCHAR(20) NOT NULL",
               "usd INTEGER",
               "local INTEGER",
               "earliest INTEGER",
               "latest INTEGER",
               "PRIMARY KEY(factset_id, fql)",
               "FOREIGN KEY(factset_id) REFERENCES company(factset_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
               "FOREIGN KEY(fql) REFERENCES fql(fql) ON DELETE NO ACTION ON UPDATE CASCADE")
    tryCreateTable(conn, "catalog", specs)
    
    ########################################
    # Create fql-company tables
    ########################################
    for( fsid in universe ) {
        dbSendQuery(conn, "BEGIN")
        
        tryCatch({
            # Get company meta data
            company <- FF.ExtractDataSnapshot(fsid, paste(fs_company_info_list, collapse=","))
            if( is.empty(company) ){
                stop(paste("Empty company meta data:", fsid))
            }
            if( ncol(company) != length(fs_company_meta_colnames) ) {
                stop(paste("Collapted company meta data:", fsid))
            }
            colnames(company) <- fs_company_meta_colnames 
            
            # Extract country info and register to the country table
            
            # See if the country is already registered
            country_id <- trySelect(conn, "country", c("country_id"), paste("country_id=",enQuote(company$country_id), sep=""))
            if( is.empty(country_id) ){
                tryInsert(conn,
                          "country", 
                          c("country_id", "country", "region", "exchange", "curr", "curr_iso", "market"), 
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
            for( fql in fql_list ){
                tablename <- paste(fql, fsid, sep="-")
                specs <- c("date INTEGER PRIMARY KEY NOT NULL UNIQUE", 
                            "usd FLOAT", 
                            "local FLOAT"
                )
                tryCreateTable(conn, tablename, specs)
                logger.info(paste("Created tseries table:", tablename, specs))
        
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
#####################################
# Constants
#####################################
db_name <- "frontier"
db <- paste("/home/honda/sqlite-db/", db_name, ".sqlite", sep="")
config_file <- paste("/home/honda/mpg/", db_name, ".conf", sep="")
wkdir <- "/home/honda/sqlite-db"
logfile_name <- paste(db_name, ".log", sep="")
do_stdout <- TRUE
#####################################
# Start...
#####################################
started <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")

#####################################
# Load config
#####################################
config <- read_config(config_file) # returns an environment
print(paste("Loaded config:", config_file))
#####################################
# Set up working directory
#####################################
if( !file.exists(wkdir) ){
    print(paste("Directory does not exist.  Creating...:", wkdir))
    dir.create(wkdir)
}
stopifnot( file.exists(wkdir) )

#####################################
# Open Logger
#####################################
logfile <- file.path(wkdir, logfile_name)
logger.init(level=logger.INFO,
            do_stdout=do_stdout,
            logfile=logfile)

print(paste("Log file:", logfile))

#####################################
# Open DB
#####################################
conn <<- dbConnect( SQLite(), db )
logger.warn(paste("Opened SQLite database:", db))

on.exit(function(){ dbDisconnect(conn); logger.warn("Closed db") })
#####################################
# Drop tables
#####################################
#drop_tables(conn)

#####################################
# Bulk init DB
#####################################
createdb(conn, config)

#####################################
# Bulk init DB
#####################################
create_year_summary(conn, "FF_WKCAP", do_drop=FALSE)

#####################################
# Close DB
#####################################
#dbDisconnect(conn)
#logger.warn("Closed db")
#####################################
# Close Logger
#####################################
logger.warn("Good day!")
logger.close()
#####################################
# Finishing up...
#####################################
finished <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
print(paste("started, finished:", started, "-", finished))