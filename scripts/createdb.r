# Download specified FS parameters
tryCatch({
    library(tools)
    library(FactSetOnDemand)
    library(xts)
    library(RSQLite)
    
    source("read_config.r")
    source("logger.r")
    source("tryDb.r")
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)
MAX_TRIALS <- 5

db <- "D:/home/honda/sqlite-db/mini.sqlite"

config_file <- "/home/honda/mpg/dummy/createdb.conf"
config <- read_config(config_file) # returns an environment
wkdir <- get("WORKING_DIR", envir=config)

timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
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
q_str <- "CREATE TABLE IF NOT EXiSTS country (country_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, country TEXT(100), region TEST(50), exchange TEXT(50), curr_iso VARCHAR(3), curr TEXT(100), market VARCHAR(25))"
stopifnot(!is.null(trySendQuery(conn, q_str)))
logger.info("Created COUNTRY table")
########################################
# Create COMPANY table
########################################
q_str <- "CREATE TABLE IF NOT EXISTS company (factset_id VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE, company_name TEXT(100), country_id INTEGER, sector_id,indgrp_id,industry_id,subind_id,FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE,FOREIGN KEY(sector_id) REFERENCES sector(sector_id) ON DELETE NO ACTION ON UPDATE CASCADE,FOREIGN KEY(indgrp_id) REFERENCES indgrp(indgrp_id) ON DELETE NO ACTION ON UPDATE CASCADE,FOREIGN KEY(industry_id) REFERENCES industry(industry_id) ON DELETE NO ACTION ON UPDATE CASCADE,FOREIGN KEY(subind_id) REFERENCES subind(subind_id) ON DELETE NO ACTION ON UPDATE CASCADE)"
stopifnot(!is.null(trySendQuery(conn, q_str)))
logger.info("Created COMPANY table")
########################################
# Create (industrial) SECTOR table
########################################
q_str <- "CREATE TABLE IF NOT EXISTS sector (sector_id INTEGER PRIMARY KEY NOT NULL UNIQUE, sector_name TEXT(100) NOT NULL UNIQUE)"
stopifnot(!is.null(trySendQuery(conn, q_str)))
logger.info("Created SECTOR table")
########################################
# Create INDGRP table
########################################
q_str <- "CREATE TABLE IF NOT EXISTS indgrp (indgrp_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, indgrp_name TEXT(100) NOT NULL UNIQUE)"
stopifnot(!is.null(trySendQuery(conn, q_str)))
logger.info("Created INDGRP table")
########################################
# Create INDUSTRY table
########################################
q_str <- "CREATE TABLE IF NOT EXISTS industry (industry_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, industry_name TEXT(100) NOT NULL UNIQUE)"
stopifnot(!is.null(trySendQuery(conn, q_str)))
logger.info("Created INDUSTRY table")
########################################
# Create SUBIND table
########################################
q_str <- "CREATE TABLE IF NOT EXISTS subind (subind_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, subind_name TEXT(100) NOT NULL UNIQUE)"
stopifnot(!is.null(trySendQuery(conn, q_str)))
logger.info("Created SUBIND table")
########################################
# Create CATEGORY table
########################################
q_str <- "CREATE TABLE IF NOT EXISTS category (category_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, category_name TEXT(50) NOT NULL UNIQUE)"
trySendQuery(conn, q_str)
logger.info("Created CATEGORY table")
q_str <- "INSERT OR REPLACE INTO category (category_name) VALUES ('company fundamental'), ('price'), ('company meta'), ('country fundamental')"
stopifnot(!is.null(trySendQuery(conn, q_str)))
########################################
# Create FREQUENCY table
########################################
q_str <- "CREATE TABLE IF NOT EXISTS frequency (freq VARCHAR(1) PRIMARY KEY NOT NULL UNIQUE, freq_name VARCHAR(20) UNIQUE)"
trySendQuery(conn, q_str)
logger.info("Created CATEGORY table")
q_str <- "INSERT OR REPLACE INTO frequency (freq, freq_name) VALUES ('Y','Anuual'),('S','Semiannual'),('Q','Quarterly'),('M','Monthly'),('D','Daily')"
stopifnot(!is.null(trySendQuery(conn, q_str)))
########################################
# Create FACTLET table
########################################
q_str <- "CREATE TABLE IF NOT EXISTS factlet (factlet VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE, description TEXT(100), unit FLOAT, freq CHAR(1), note TEXT(200), category_id INTEGER, FOREIGN KEY(category_id) REFERENCES category(category_id) ON DELETE NO ACTION ON UPDATE CASCADE, FOREIGN KEY(freq) REFERENCES frequency(freq) ON DELETE NO ACTION ON UPDATE CASCADE )"
stopifnot(!is.null(trySendQuery(conn, q_str)))
logger.info("Created FACTLET table")
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
fs_company_meta_header <- c("FG_COMPANY_NAME",
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
    company <- FF.ExtractDataSnapshot(fsid, paste(fs_company_meta_list, collapse=","))
    colnames(company) <- fs_company_meta_colnames
    
    # register the country if not registered
    q_str <- paste("SELECT country_id FROM country WHERE country='", company$country, "'",sep="")
    logger.debug(q_str)  
    country <- tryGetQuery(conn, q_str, MAX_TRIALS) 
    if( nrow(country) > 0 ){
        logger.debug(paste("Found the entry in the country table:", company$country))
    } else {
        value_list <- c(company$country, 
                        company$region, 
                        company$exchange,
                        company$curr,
                        company$curr_code,
                        market
                        )       
        q_str <- paste("INSERT OR REPLACE INTO country (country,region,exchange,curr,curr_iso,market) VALUES",
                       "(",
                       paste("\"", value_list, "\"", sep="", collapse=","),
                       ")",
                       sep="")
        logger.debug(q_str)
        trySendQuery(conn, q_str, MAX_TRIALS)
        logger.info(paste("Registered to the country table:", company$country))
        
        q_str <- paste("SELECT country_id FROM country WHERE country='", company$country, "'",sep="")
        logger.debug(q_str)   
        country <- tryGetQuery(conn, q_str, MAX_TRIALS)
    }
    country_id <- "NULL"
    if( nrow(country) > 0 && !is.null(country$country_id) ){
        country_id <- country$country_id
    }
    # register the sector if not registered
    q_str <- paste("SELECT sector_id FROM sector WHERE sector_name='", company$sector, "'",sep="")
    logger.debug(q_str)  
    sector <- tryGetQuery(conn, q_str, MAX_TRIALS)  
    if( nrow(sector) > 0 ){
        logger.debug(paste("Found the entry in the sector table:", company$sector))
    } else {
        if( !is.na(company$sector) ){      
            q_str <- paste("INSERT OR REPLACE INTO sector (sector_name) VALUES",
                           "('",company$sector,"')",
                           sep="")
            logger.debug(q_str)
            trySendQuery(conn, q_str, MAX_TRIALS)
            logger.info(paste("Registered to the sector table:", company$sector))
            
            q_str <- paste("SELECT sector_id FROM sector WHERE sector_name='", company$sector, "'",sep="")
            logger.debug(q_str)   
            sector <- tryGetQuery(conn, q_str, MAX_TRIALS)
        }
    }
    sector_id <- "NULL"
    if( nrow(sector) > 0 && !is.null(sector$sector_id) ){
        sector_id <- sector$sector_id
    }
    # register the industry group if not registered
    q_str <- paste("SELECT indgrp_id FROM indgrp WHERE indgrp_name='", company$indgrp, "'",sep="")
    logger.debug(q_str)  
    indgrp <- tryGetQuery(conn, q_str, MAX_TRIALS)  
    if( nrow(indgrp) > 0 ){
        logger.debug(paste("Found the entry in the indgrp table:", company$indgrp))
    } else {
        if( !is.na(company$indgrp) ){
            q_str <- paste("INSERT OR REPLACE INTO indgrp (indgrp_name) VALUES",
                           "('",company$indgrp,"')",
                           sep="")
            logger.debug(q_str)
            trySendQuery(conn, q_str, MAX_TRIALS)
            logger.info(paste("Registered to the indgrp table:", company$indgrp))
            
            q_str <- paste("SELECT indgrp_id FROM indgrp WHERE indgrp_name='", company$indgrp, "'",sep="")
            logger.debug(q_str)   
            indgrp <- tryGetQuery(conn, q_str, MAX_TRIALS)
        }
    }   
    indgrp_id <- "NULL"
    if( nrow(indgrp) > 0 && !is.null(indgrp$indgrp_id) ){
        indgrp_id <- indgrp$indgrp_id
    }
    # register the industry if not registered
    q_str <- paste("SELECT industry_id FROM industry WHERE industry_name='", company$industry, "'",sep="")
    logger.debug(q_str)  
    industry <- tryGetQuery(conn, q_str, MAX_TRIALS)  
    if( nrow(industry) > 0 ){
        logger.debug(paste("Found the entry in the industry table:", company$industry))
    } else {
        if( !is.na(company$industry)  ){
            q_str <- paste("INSERT OR REPLACE INTO industry (industry_name) VALUES",
                           "('",company$industry,"')",
                           sep="")
            logger.debug(q_str)
            trySendQuery(conn, q_str, MAX_TRIALS)
            logger.info(paste("Registered to the industry table:", company$industry))
            
            q_str <- paste("SELECT industry_id FROM industry WHERE industry_name='", company$industry, "'",sep="")
            logger.debug(q_str)   
            indgrp <- tryGetQuery(conn, q_str, MAX_TRIALS)
        }
    } 
    iindustry_id <- "NULL"
    if( nrow(industry) > 0 && !is.null(industry$industry_id) ){
        industry_id <- industry$industry_id
    }
    # register the sub-industry if not registered
    q_str <- paste("SELECT subind_id FROM subind WHERE subind_name='", company$subind, "'",sep="")
    logger.debug(q_str)  
    subind <- tryGetQuery(conn, q_str, MAX_TRIALS)  
    if( nrow(subind) > 0 ){
        logger.debug(paste("Found the entry in the subind table:", company$subind))
    } else {
        if( !is.na(company$subind) ){
            q_str <- paste("INSERT OR REPLACE INTO subind (subind_name) VALUES",
                           "('",company$subind,"')",
                           sep="")
            logger.debug(q_str)
            trySendQuery(conn, q_str, MAX_TRIALS)
            logger.info(paste("Registered to the subind table:", company$subind))
            
            q_str <- paste("SELECT subind_id FROM subind WHERE subind_name='", company$subind, "'",sep="")
            logger.debug(q_str)   
            subind <- tryGetQuery(conn, q_str, MAX_TRIALS)
        }
    } 
    subind_id <- "NULL"
    if( nrow(subind) > 0 && !is.null(subind$subind_id) ){
        subind_id <- subind$subind_id
    }
    val_list <- c(paste("\"", company$id, "\"", sep=""), 
                  paste("\"", company$name, "\"", sep=""), 
                  country_id,
                  sector_id,
                  indgrp_id,
                  industry_id,
                  subind_id)
    val_liststr <- paste(val_list, collapse=",")
    q_str <- paste("INSERT OR REPLACE INTO company (factset_id,company_name,country_id,sector_id,indgrp_id,industry_id,subind_id) VALUES (", val_liststr, ")", sep=""); 
    logger.debug(q_str)
    trySendQuery(conn, q_str, MAX_TRIALS)
    logger.info(paste("Registered to the company table:", val_liststr))
    
    # register company
    for( param in param_list ){
        tablename <- paste(param, fsid, sep="-")
        q_str <- paste("CREATE TABLE IF NOT EXISTS \"", tablename, "\" (date INTEGER PRIMARY KEY NOT NULL UNIQUE, usd, local)", sep="")
        logger.debug(q_str)
        trySendQuery(conn, q_str, MAX_TRIALS)
        logger.info(paste("Created table:", tablename))

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
                    if( grepl("P_TOTAL_RETURNC", param) || grepl("^FG_", param) ){
                        fs_str1 <- paste(param, "(", paste(fs.t0,fs.t1,freq,curr,sep=","), ")", sep="")
                        # FF.ExtractFormulaHistory("002826", P_PRICE_AVG(20120101,20121231,D,USD), 20120101:20121231:D)
                        data <- FF.ExtractFormulaHistory(as.character(fsid), fs_str1, fs_str2)
                    } else if( grepl("P_PRICE_AVG", param) ){
                        #FF.ExtractFormulaHistory('004561','P_PRICE_AVG(19800101,20131231,M,USD,4)','19800101:20131231:M')
                        fs_str1 <- paste(param, "(", paste(fs.t0,fs.t1,freq,curr,4, sep=","), ")", sep="")                  
                        data <- FF.ExtractFormulaHistory(as.character(fsid), fs_str1, fs_str2)
                    } else if( grepl("^FF_", param) ){
                        # FF.ExtractFormulaHistory("002826", "FF_ASSETS", "20120101:20121231:D","curr=USD")
                        data <- FF.ExtractFormulaHistory(as.character(fsid),
                                                     param,
                                                     fs_str2,
                                                     paste("curr=",curr,sep=""))
                    } else {
                        logger.warn(paste("I don't know what to do with this FS param:", param))
                    }

                }, error=function(msg){
                    logger.error(msg)
                    next
                })

                if( nrow(data) < 1 ){
                    logger.warn("Empty data.  Skipping...")
                    next
                }
                colnames(data) <- c("id", "date", curr)
                #browser()
                if( is.null(master_data) ){
                    master_data <- data
                } else {
                    master_data <- merge(master_data, data, by=c("id", "date"))
                }                
            }

            val_list <- apply(master_data, 1, function(row){ 
                if(!all(is.na(row[3:length(row)])) ){
                    j <- julian(as.Date(row[2]))
                    paste("(", j, ",", paste(row[3:length(row)], collapse=","), ")", sep="")
                } 
            }
            )

            val_list <- Filter( function(x) {
                if( is.null(x) ){
                    return(FALSE)
                } 
                return(TRUE)
            }, val_list)
            
            if( length(val_list) < 1 ){
                logger.debug("No non-NA data")
                next   
            }

            for( begin in seq(1, length(val_list), 100) ){
                end <- min(length(val_list), begin+100-1)
                val_list[begin:end]

                val_liststr <- paste(val_list[begin:end], collapse=",")
                
                q_str <- paste("INSERT OR REPLACE INTO \"", 
                               tablename, 
                               "\" (date,", paste(curr_list,collapse=","), ") VALUES ", val_liststr, "", sep="")
                logger.debug(q_str)
                trySendQuery(conn, q_str, MAX_TRIALS )
            }

        }
    }
}

dbCommit(conn)
dbDisconnect(conn)
logger.info("Closed db")
logger.info("Good day!")
logger.close()