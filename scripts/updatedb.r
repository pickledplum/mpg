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
    
    stopifnot( exists("T1", envir=config) )
    t1 <- get("T1", mode="character", envir=config) # "1980-12-12" "now"
    stopifnot(grepl("[[:digit:]]+{4}-[[:digit:]]+{2}-[[:digit:]]+{2}", t1))
    
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
    
    country.row1 <- trySelect(conn, "country", c("*"), c("rowid==1"))
    if( !all(intersect(colnames(country.row1), country.column.name) == country.column.name) ){
        logger.error(paste("Schema mismatch (COUNTRY table): expecting (", paste(counry.column.name, collapse=","), ")"))
        stop()
    }
    logger.info(paste("Sanity checked (COUNTRY table):", paste(country.column.name, collapse=",")))
    
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
    company.row1 <- trySelect(conn, "company", c("*"), c("rowid==1"))
    if( !all(intersect(colnames(company.row1), company.column.name) == company.column.name) ){
        logger.error(paste("Schema mismatch (COMPANY table): expecting (", paste(company.column.name, collapse=","), ")"))
        stop()
    }
    logger.info(paste("Sanity checked (COMPANY table):", paste(company.column.name, collapse=",")))
    
    ########################################
    # Create CATEGORY table
    ########################################
    category.column.name <- c("category_id", 
                              "category_descript")
    category.column.type <- c("VARCHAR(50) PRIMARY KEY NOT NULL UNIQUE", 
                              "TEXT(50) NOT NULL UNIQUE")
    category.specs <- paste(category.column.name, category.column.type)
    category.row1 <- trySelect(conn, "category", c("*"), c("rowid==1"))
    if( !all(intersect(colnames(category.row1), category.column.name) == category.column.name) ){
        logger.error(paste("Schema mismatch (CATEGORY table): expecting (", paste(category.column.name, collapse=","), ")"))
        stop()
    }
    logger.info(paste("Sanity checked (CATEGORY table):", paste(category.column.name, collapse=",")))
    
    ########################################
    # Create FREQUENCY table
    ########################################
    frequency.column.name <- c("freq", 
                               "freq_name")
    
    frequency.column.type <- c("VARCHAR(1) PRIMARY KEY NOT NULL UNIQUE",
                               "VARCHAR(20) UNIQUE"
    )
    frequency.specs <- paste(frequency.column.name, frequency.column.type)
    
    frequency.row1 <- trySelect(conn, "frequency", c("*"), c("rowid==1"))
    if( !all(intersect(colnames(frequency.row1), frequency.column.name) == frequency.column.name) ){
        logger.error(paste("Schema mismatch (FREQUENCY table): expecting (", paste(frequency.column.name, collapse=","), ")"))
        stop()
    }
    logger.info(paste("Sanity checked (FREQUENCY table):", paste(frequency.column.name, collapse=",")))
    
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
    fql.row1 <- trySelect(conn, "fql", c("*"), c("rowid==1"))
    if( !all(intersect(colnames(fql.row1), fql.column.name) == fql.column.name) ){
        logger.error(paste("Schema mismatch (FQL table): expecting (", paste(fql.column.name, collapse=","), ")"))
        stop()
    }
    logger.info(paste("Sanity checked (FQL table):", paste(fql.column.name, collapse=",")))
    
#     stopifnot( exists("FQL_MAP", envir=config) )
#     fql_map_filename <- get("FQL_MAP", envir=config)
#     stopifnot(file.exists(fql_map_filename))
#     fql_map <- read.csv(fql_map_filename)
#     rownames(fql_map) <- fql_map$fql
#     tryBulkInsert(conn, "fql", 
#                   fql.column.name,
#                   data.frame(enQuote2(fql_map$fql),
#                              enQuote2(fql_map$syntax),
#                              enQuote(fql_map$description),
#                              fql_map$unit,
#                              enQuote(fql_map$report_freq),
#                              enQuote(fql_map$category_id),
#                              enQuote(fql_map$note)
#                   ))
#     
#     logger.info(paste("Populated FQL table:", nrow(fql_map)))

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
    catalog.row1 <- trySelect(conn, "catalog", c("*"), c("rowid==1"))
    if( !all(intersect(colnames(catalog.row1), catalog.column.name) == catalog.column.name) ){
        logger.error(paste("Schema mismatch (CATALOG table): expecting (", paste(catalog.column.name, collapse=","), ")"))
        stop()
    }
    logger.info(paste("Sanity checked (CATALOG table):", paste(catalog.column.name, collapse=",")))
    
    
    
    ########################################
    # Create fql-company tables
    ########################################
    tseries.column.name <- c("date", "usd", "local")
    tseries.column.type <- c("INTEGER PRIMARY KEY NOT NULL UNIQUE", 
                             "FLOAT", 
                             "FLOAT")
    tseries.specs <- paste(tseries.column.name, tseries.column.type)        
    for( fsid in universe ) {
        
        tryCatch({
            dbSendQuery(conn, "BEGIN")
            #check if the company is registered in Company table
            company.table.data <- trySelect(conn, "company", company.column.name, paste("factset_id=", enQuote(fsid), sep=""))
            if( !is.empty(company.table.data) ){
                logger.info(paste("Company already registered:", fsid))
            } else {
            
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
            }
            dbSendQuery(conn, "COMMIT")
            logger.info(paste("Commited COUNTRY and COMPANY tables for", fsid))
        }, error=function(msg){
                logger.error(paste("Rolling back:", msg))
                dbSendQuery(conn, "ROLLBACK")
        })

        tryCatch({
            # T series tables
            for( fql in fql_list ){
                dbSendQuery(conn, "BEGIN")
                catalog.entry <- trySelect(conn, 
                                           "catalog", 
                                           catalog.column.name, 
                                           paste("fql=", enQuote(fql), " AND factset_id=", enQuote(fsid), sep=""))
                tablename <- NULL
                is_new_entry <- FALSE
                
                # Create the tseries table
                if( is.empty(catalog.entry) || is.empty(catalog.entry$tablename) ){
                    is_new_entry <- TRUE
                    tablename <- paste(fql, fsid, sep="-")
                    
                    tryCreateTable(conn, tablename, tseries.specs)
                    logger.info(paste("Created tseries table:", tablename, tseries.specs))
                } else {
                    # update fs.t0, fs.t1 and t0, t1
                    is_new_entry <- FALSE
                    # grab the latest date
                    tablename <- catalog.entry$tablename
                    t0 <- unjulianday(catalog.entry$latest + 1)
                    t1 <- Sys.Date()
                }
                
                controls <- get(paste(fs_prefix, fql, sep=""), envir=config)
                curr_list <- c(tolower(default_currency))
                freq_list <- c()
                fs.t0 <- gsub("-", "", t0)
                fs.t1 <- gsub("-", "", t1)
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
                    freq <- "D"
                }
                if( all("M" %in% controls) ) {
                    freq <- "M"
                }
                if( all("Q" %in% controls) ) {
                    freq <- "Q"
                }
                if( all("Y" %in% controls) ) {
                    freq <- "Y"
                }
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
                    stop()
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
                    stop()
                }
                tryCatch({
                    as.Date(filtered_data[[2]])
                }, warning=function(msg){
                    logger.error(paste("Some date string must be mulformed:", filtered_data[[2]]))
                    stop()
                }, error=function(msg){
                    logger.error(paste("Some date string must be mulformed:", filtered_data[[2]]))
                    stop()
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
                #columns <- c("tablename","factset_id","fql","usd","local","earliest","latest")

                if( is_new_entry ){
                    values <- c(enQuote(tablename),
                                enQuote(c(fsid)),
                                enQuote(c(fql)),
                                c( ifelse( "usd" %in% curr_list, 1, 0) ),
                                c( ifelse( "local" %in% curr_list, 1, 0) ),
                                earliest,
                                latest
                    )
                    tryInsertOrReplace(conn, 
                              "catalog", 
                              catalog.column.name, 
                              values)
                    logger.info(paste("Registered to CATALOG table:", fsid, ",", fql))
                } else{
                    values <- c(c( ifelse( "usd" %in% curr_list && catalog.entry$usd==0, 1, 0) ),
                                c( ifelse( "local" %in% curr_list && catalog.entry$local==0, 1, 0) ),
                                earliest,
                                latest)
                    tryUpdate(conn, 
                              "catalog", 
                              "tablename", 
                              enQuote(tablename), 
                              c("usd", "local", "earliest", "latest"), 
                              values)
                    logger.info(paste("Updated the entry in CATALOG table:", fsid, ",", fql))
                }
                dbSendQuery(conn, "COMMIT")
                logger.info(paste("Commited on", fsid))

            }
            
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
db_name <- "mini"
db <- paste("/Users/honda/db/", db_name, ".sqlite", sep="")
config_file <- paste("/Users/honda/Documents/GitHub/mpg/", db_name, ".conf", sep="")
wkdir <- "/Users/honda/db"
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
logger.init(level=logger.DEBUG,
            do_stdout=do_stdout,
            logfile=logfile)

print(paste("Log file:", logfile))

#####################################
# Open DB
#####################################
conn <<- dbConnect( SQLite(), db )
logger.warn(paste("Opened SQLite database:", db))

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
create_year_summary(conn, "FF_WKCAP", do_drop=TRUE)

#####################################
# Close DB
#####################################
dbDisconnect(conn)
logger.warn("Closed db")
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
stop()
