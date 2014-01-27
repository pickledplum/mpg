# Download specified FS parameters
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
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)
MAX_TRIALS <- 5

config_file <- "/home/honda/mpg/dummy/createdb.conf"
config <- read_config(config_file) # returns an environment

db <- get("DB", envir=config)

started <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
logfile <- file.path(wkdir, paste("dummy", ".log",sep=""))
logger.init(level=logger.INFO,
            do_stdout=TRUE,
            logfile=logfile)

print(paste("Log file:", logfile))

conn <<- dbConnect( SQLite(), db )
logger.info(paste("Opened SQLite database:", db))
# on.exit( function(conn) {
# 
#     dbDisconnect(conn)
#     logger.info("Closed db")
#     logger.info("Good bye!")
#     logger.close()
# })

########################################
# Configure FS
########################################
# Query time out.  Default is 120 secs.  120-3600 secs on client side.
FactSet.setConfigurationItem( FactSet.TIMEOUT, 900 )
logger.info(paste("FactSet timetout:", 900, "secs"))

########################################
# Load config
########################################
config <- read_config(config_file) # returns an environment
logger.info(paste("Loaded config:", config_file))

wkdir <- ""
portfolio_ofdb <- ""
universe <- c("character")
prefix <- ""
t0 <- ""
t1 <- ""
default_currency <- ""

stopifnot( exists("WORKING_DIR", envir=config) )
wkdir <- get("WORKING_DIR", mode="character", envir=config) 
if( !file.exists(wkdir) ){
    logger.warn(paste("Directory does not exist.  Creating...:", wkdir))
    dir.create(wkdir)
}
stopifnot( file.exists(wkdir) )

stopifnot( exists("OUTPUT_PREFIX", envir=config) )
prefix <- get("OUTPUT_PREFIX", mode="character", envir=config) # "dummy"
if(is.null(prefix)){
    prefix = "" # to make string concatinations happy
}

stopifnot( exists("T0", envir=config) )
t0 <- get("T0", mode="character", envir=config) # "1980-01-01" NOTE: implement "last"?
stopifnot(grepl("[[:digit:]]+{4}-[[:digit:]]+{2}-[[:digit:]]+{2}", t0))
fs.t0 <- gsub("-", "", t0)

stopifnot( exists("T1", envir=config) )
t1 <- get("T1", mode="character", envir=config) # "1980-12-12" "now"
stopifnot(grepl("[[:digit:]]+{4}-[[:digit:]]+{2}-[[:digit:]]+{2}", t1))
fs.t1 <- gsub("-", "", t1)

stopifnot( exists("DEFAULT_CURRENCY", envir=config) )
default_currency <- get("DEFAULT_CURRENCY", mode="character", envir=config) # USD
stopifnot( toupper(default_currency) %in% c("USD","LOCAL") )

stopifnot( exists("MARKET", envir=config) )
market <- get("MARKET", mode="character", envir=config)
stopifnot( toupper(market) %in% c("MIXED", "EM", "DM", "FM") )

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
logger.info(paste("WORKING_DIR:", wkdir))
logger.info(paste("OUTPUT_PREFIX:", prefix))
logger.info(paste("T0,T1:", t0,",",t1))
logger.info(paste("DEFAULT_CURRENCY:", default_currency))
logger.info(paste("MARKET:", market))
logger.info(paste("INDEX:", finance.index))

########################################
# Extract FactSet parameter strings
# Prefixed with "FACTSET_"
########################################
stopifnot( exists("FACTSET_PREFIX", envir=config) )
fs_prefix <- get("FACTSET_PREFIX", envir=config)
logger.info(paste("FACTSET_PREFIX:", fs_prefix))

fs_prefix_pattern <- paste("^", fs_prefix, sep="")
config_param_list <- grep(fs_prefix_pattern, ls(config), value=TRUE) 
param_list <- gsub(fs_prefix_pattern, "", config_param_list)
logger.info(paste("FACTSET items:", paste(param_list, collapse=",")))

########################################
# Create Country Table
########################################
#q_str <- "CREATE TABLE IF NOT EXiSTS country (country_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, country TEXT(100), region TEST(50), exchange TEXT(50), curr_iso VARCHAR(3), curr TEXT(100), market VARCHAR(25))"
#stopifnot(trySendQuery(conn, q_str))
specs <- c("country_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE", 
           "country TEXT(100)", 
           "region TEST(50)", 
           "exchange TEXT(50)", 
           "curr_iso VARCHAR(3)", 
           "curr TEXT(100)", 
           "market VARCHAR(25)" )
tryCreateTableIfNotExists(conn, "country", specs)
logger.info("Created COUNTRY table")
########################################
# Create COMPANY table
########################################
#q_str <- "CREATE TABLE IF NOT EXISTS company (factset_id VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE, company_name TEXT(100), country_id INTEGER, sector VARCHAR(100),indgrp VARCHAR(100),industry VARCHAR(100),subind VARCHAR(100),FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE)"
#stopifnot(trySendQuery(conn, q_str))
specs <- c("factset_id VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
           "company_name TEXT(100)", 
           "country_id INTEGER", 
           "sector VARCHAR(100)",
           "indgrp VARCHAR(100)",
           "industry VARCHAR(100)",
           "subind VARCHAR(100)",
           "FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE"
)

tryCreateTableIfNotExists(conn, "company", specs)
logger.info("Created COMPANY table")

########################################
# Create CATEGORY table
########################################
#q_str <- "CREATE TABLE IF NOT EXISTS category (category_id VARCHAR(30) PRIMARY KEY NOT NULL UNIQUE, category_descrip TEXT(50) NOT NULL UNIQUE)"
#trySendQuery(conn, q_str)
specs <- c("category_id VARCHAR(50) PRIMARY KEY NOT NULL UNIQUE", 
           "category_descript TEXT(50) NOT NULL UNIQUE"
)
tryCreateTableIfNotExists(conn, "category", specs)
logger.info("Created CATEGORY table")

#q_str <- "INSERT OR REPLACE INTO category (category_id, category_descript) VALUES ('company_fund', 'Company fundamental'), ('price', 'Price'), ('company_info', 'Company info'), ('country_fund', 'Country fundamental')"
#stopifnot(trySendQuery(conn, q_str))
tryInsertOrReplace(conn, "category", c("category_id", "category_descript"), 
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
tryCreateTableIfNotExists(conn, "frequency", specs)
logger.info("Created FREQUENCY table")
#q_str <- "INSERT OR REPLACE INTO frequency (freq, freq_name) VALUES ('Y','Anuual'),('S','Semiannual'),('Q','Quarterly'),('M','Monthly'),('D','Daily')"
#stopifnot(trySendQuery(conn, q_str))
tryInsertOrReplace(conn, "frequency", c("freq", "freq_name"), 
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
tryCreateTableIfNotExists(conn, "fql", specs)
logger.info("Created FQL table")

########################################
# FQL map
########################################
stopifnot( exists("FQL_MAP", envir=config) )
fql_map_filename <- get("FQL_MAP", envir=config)
stopifnot(file.exists(fql_map_filename))
fql_map <- read.csv(fql_map_filename)
rownames(fql_map) <- fql_map$fql
for( i in seq(1, nrow(fql_map) )){
    r <- fql_map[i,]
    fql <- r$fql
    syntax <- r$syntax
    description <- r$description
    unit <- r$unit
    report_freq <- r$report_freq
    category <- r$category
    note <- r$note
    ret <- trySelect(conn, "category", c("category_id"), paste("category_descript", "=", enQuote(category), sep=""))
    category_id <- ret$category_id
    tryInsertOrReplace(conn, "fql", c("fql","syntax","description","unit","report_freq","category_id","note"),
              data.frame(c(enQuote(fql)),
                         c(enQuote2(syntax)),
                         c(enQuote(description)),
                         c(unit),
                         c(enQuote(report_freq)),
                         c(enQuote(category_id)),
                         c(enQuote(note))
                         )
              )

}

tryExtract <- function(param, id, d1, d2, freq, curr) {
    #browser()
    formula <- fql_map[param, ]$syntax
    stopifnot(!is.empty(formula))
    formula <- gsub("<ID>", id, formula)
    formula <- gsub("<d1>",d1,formula)
    formula <- gsub("<d2>",d2,formula)
    formula <- gsub("<freq>",freq,formula)
    formula <- gsub("<curr>",curr,formula)
    logger.info(formula)
    ret <- eval(parse(text=formula))
    if(is.null(ret)) return(data.frame())
    return(ret)
}

########################################
# Company meta data
########################################
# Company basics
#
# company name
# country of operation
# region of operation
# exchange
# currency
# currency 3 letter code
# isin
# SEDOL
# sector
# industry group
# industry
# sub-industry
#
fs_company_info_list <- c("FG_COMPANY_NAME",
  "P_DCOUNTRY",
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
# Create param-company tables
########################################
for( fsid in universe ) {
    # Company meta data
    company <- FF.ExtractDataSnapshot(fsid, paste(fs_company_info_list, collapse=","))
    colnames(company) <- fs_company_meta_colnames 
    country <- trySelect(conn, "country", c("country_id"), paste("country='",company$country, "'",sep=""), MAX_TRIALS)
    if( nrow(country) > 0 ){
        logger.debug(paste("Found the entry in the country table:", company$country))
    } else {
        tryInsertOrReplace(conn, 
                           "country", 
                           c("country", "region", "exchange", "curr", "curr_iso", "market"), 
                           data.frame(c(enQuote(company$country)),
                                      c(enQuote(company$region)),
                                      c(enQuote(company$exchange)),
                                      c(enQuote(company$curr)),
                                      c(enQuote(company$curr_code)),
                                      c(enQuote(market))
                           ),
                           MAX_TRIALS)
        
        logger.info(paste("Registered to the country table:", company$country))

        country <- trySelect(conn, "country", c("country_id"), paste("country='",company$country,"'",sep=""))
    }
    country_id <- "NULL"
    if( nrow(country) > 0 && !is.null(country$country_id) ){
        country_id <- country$country_id
    }
    tryInsertOrReplace(conn, "company", 
                       c("factset_id",
                         "company_name",
                         "country_id",
                         "sector",
                         "indgrp",
                         "industry",
                         "subind"),
                       data.frame(c(enQuote(company$id)),
                                  c(enQuote(company$name)),
                                  c(country_id),
                                  c(enQuote(company$sector)),
                                  c(enQuote(company$indgrp)),
                                  c(enQuote(company$industry)),
                                  c(enQuote(company$subind))
                       )
    )
    logger.info(paste("Registered to the company table:", val_liststr))
    
    # register company
    for( param in param_list ){
        tablename <- paste(param, fsid, sep="-")
        specs <- c("date INTEGER PRIMARY KEY NOT NULL UNIQUE", 
                    "usd FLOAT", 
                    "local FLOAT"
        )
        tryCreateTableIfNotExists(conn, tablename, specs)
        logger.info(paste("Created tseries table:", tablename, specs))

        controls <- get(paste(fs_prefix, param, sep=""), envir=config)
        curr_list <- c(default_currency)
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
            master_data <- NULL
            for( curr in curr_list ){
                fs_str2 <- paste(fs.t0, fs.t1, freq, sep=":")
                data <- NULL

                tryCatch({
                    data <- tryExtract(param, fsid, fs.t0, fs.t1, freq, curr)
                }, error=function(msg){
                    logger.error(msg)
                    next
                })

                if( is.empty(data) ){
                    logger.warn("Empty data.  Skipping...")
                    next
                }
                colnames(data) <- c("id", "date", curr)
                if( is.null(master_data) ){
                    master_data <- data
                } else {
                    master_data <- merge(master_data, data, by=c("id", "date"))
                }                
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
            for( j in seq(1, nrow(filtered_data) ) ){
                julianval <- julian(as.Date(unlist(filtered_data[j,][2])))
                rownames(filtered_data)[j] <- julianval
            }
            for( begin in seq(1, length(val_list), 100) ){
                end <- min(nrow(filtered_data), begin+100-1)
                values
                if( ncol(filtered_data) > 3 ){
                    values <- cbind(rownames(filtered_data[begin:end,]),
                                    filtered_data[begin:end,][c(3,4)])
                } else {
                    values <- cbind(rownames(filtered_data[begin:end,]),
                                    filtered_data[begin:end,][3])
                }
                tryInsertOrReplace(conn, 
                                   tablename, 
                                   c("date", curr_list), 
                                   values
                )
            }

        }
    }
}

dbCommit(conn)
dbDisconnect(conn)
logger.info("Closed db")
logger.info("Good day!")
logger.close()