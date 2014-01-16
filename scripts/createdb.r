# Download specified FS parameters
library(tools)
library(FactSetOnDemand)
library(xts)
source("read_config.r")
source('logger.r"')

db <- "D:/home/honda/sqlite-db/mini.sqlite"

config_file <- "/home/honda/mpg/dummy/createdb.conf"
config <- read_config(config_file) # returns an environment
wkdir <- get("WORKING_DIR", envir=config)
#on.exit(exit_f)

timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
logfile <- file.path(wkdir, paste("dummy", ".log",sep=""))
logger.init(level=logger.INFO,
            do_stdout=TRUE,
            logfile=logfile)
)
print(paste("Log file:", logfile))

conn <<- dbConnect( SQLite(), db )
logger.info(paste("Opened SQLite database:", db))
on.exit( function(conn) {
    dbDisconnect(conn)
})

########################################
# Configure FS
########################################
# Query time out.  Default is 120 secs.  120-3600 secs on client side.
FactSet.setConfigurationItem( FactSet.TIMEOUT, 900 )
logger.info("paste(FactSet timetout:", 900, "secs"))

########################################
# Load config
########################################
config <- read_config(config_file) # returns an environment
log.debug(paste("Loaded config:", config_file))

wkdir <- ""
portfolio_ofdb <- ""
universe <- c("character")
prefix <- ""
t0 <- ""
t1 <- ""
default_currency <- ""

stopifnot( exists("wkdir", envir=config) )
wkdir <- get("wkdir", mode="character", envir=config) 
stopifnot( file.exists(wkdir) )

stopifnot( exists("OUTPUT_PREFIX", envir=config) )
prefix <- get("OUTPUT_PREFIX", mode="character", envir=config) # "dummy"
if(is.null(prefix)){
    prefix = "" # to make string concatinations happy
}

stopifnot( exists("T0", envir=config) )
t0 <- get("T0", mode="character", envir=config) # "1980-01-01" NOTE: implement "last"?
stopifnot(grepl("[[:digit:]]+{4}-[[:digit:]]+{2}-[[:digit:]]+{2}", t0))

stopifnot( exists("T1", envir=config) )
t1 <- get("T1", mode="character", envir=config) # "1980-12-12" "now"
stopifnot(grepl("[[:digit:]]+{4}-[[:digit:]]+{2}-[[:digit:]]+{2}", t1))

stopifnot( exists("DEFAULT_CURRENCY", envir=config) )
default_currency <- get("DEFAULT_CURRENCY", mode="character", envir=config) # USD
stopifnot( toupper(default_currency) %in% c("USD","LOCAL") )

stopifnot( exists("MARKET", envir=config) )
finance.market <- get("MARKET", mode="character", envir=config)
stopifnot( toupper(default_currency) %in% c("MIXED", "EM", "DM", "FM") )

stopifnot( exists("INDEX", envir=config) )
finance.index <- get("INDEX", mode="character", envir=config)

if( exists("PORTFOLIO_OFDB", envir=config) ){
    portfolio_ofdb <- get("PORTFOLIO_OFDB", mode="character", envir=config) # "PERSONAL:HONDA_MSCI_ACWI_ETF"
    stopifnot( !is.null(portfolio_ofdb) && portfolio_ofdb != "" )
    data <- FF.ExtractOFDBUniverse(portfolio_ofdb, "0O")
    universe <- data$Id
    
} else if( exists("UNIVERSE", envir=config) ){
    universe_file <- get("UNIVERSE", mode="character", envir=config) # "list_of_constituents.txt"
    stopifnot( !is.null(universe_file) && universe_file != "" )
    stopifnot( file.exists(universe_file))
    unicon <- file(universe_file, open="r", blocking=FALSE)
    temp <- read.table(unicon, colClasses=c("character"), header=TRUE, strip.white=TRUE, blank.lines.skip=TRUE, comment.char="#")
    universe <-temp[[1]]
    close(unicon)
    
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
logger.info(paste("PREFIX:", prefix))
logger.info(paste("T0,T1:", t0,",",t1))
logger.info(paste("DEFAULT_CURRENCY:", default_currency))

########################################
# Extract FactSet parameter strings
# Prefixed with "FACTSET_"
########################################
stopifnot( exists("FACTSET_PREFIX", envir=config) )
fs_prefix <- get("FACTSET_PREFIX", envir=config)
logger.info(paste("FACTSET_PREFIX:", fs_prefix))

fs_prefix_pattern <- paste("^", fs_prefix, sep="")
config_param_list <- grep(fs_prefix_pattern, ls(config), value=TRUE)
config_param_list <- config_param_list[-c(which(config_param_list=="FACTSET_PREFIX"))]   
param_list <- gsub(fs_prefix_pattern, "", config_param_list)
logger.info(paste("FACTSET items:", paste(param_list, collapse=",")))

########################################
# Create Country Table
########################################
q_str <- "CREATE TABLE IF NOT EXiSTS country (country_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, country TEXT(100), region TEST(50), exchange TEXT(50), currency_code VARCHAR(3), currency TEXT(100))"
tryCatch({
    dbSendQuery(conn, q_str)
}, error = function(msg){
    stop(msg)
}
)
logger.info("Created country table")
########################################
# Download & Create Tables
########################################
q_str <- "CREATE TABLE IF NOT EXISTS company (factset_id VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE, company_name TEXT(100), country_id INTEGER, FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE)"
tryCatch({
    dbSendQuery(conn, q_str)
}, error = function(msg){
    stop(msg)
}
)
logger.info("Created company table")
for( fsid in universe ) {
    
    
    #######################################
    #######################################
    # DEBUG FROM HERE!!! 1/15/2015
    #######################################
    #######################################
    # get company basics
    #
    # company name
    # country of operation
    # region of operation
    # exchange
    # currency
    # currency 3 letter code
    # isin
    # SEDOL
    comp.info <- FF.ExtractDataSnapshot(fsid, "FG_COMPANY_NAME,P_DCOUNTRY,P_DCOUNTRY(REG),P_EXCHANGE,P_CURRENCY,P_CURRENCY_CODE,FE_COMPANY_INFO(ISIN),FE_COMPANY_INFO(SEDOL)")
    company.name <- comp.info[[3]]
    company.country <- comp.info[[4]]
    company.region <- comp.info[[5]]
    company.exchange <- comp.info[[6]]
    company.curr <- comp.info[[7]]
    company.curr_code <- comp.info[[8]]
    company.isin <- comp.info[[9]]
    company.sedol <- company.info[[10]]
    company.market <- "prefix"
    # register the country if not registered
    q_str <- paste("SELECT country_id FROM country WHERE country='", company.country, "'",sep="")
    logger.debug(q_str)
    tryCatch({
        res <- dbGetQuery(conn, q_str)
        
    }, error=function(e){
         logger.warn(e)
         next
     }
    )
    if( nrow(res) > 0 ){
        logger.debug(paste("Already registered in the country table:", country)
        next
    }
    value_list <- c(company.country, 
                    company.region, 
                    company.exchange,
                    company.curr,
                    company.curr_code,
                    company.isin,
                    company.sedol,
                    finance.market)       
    q_str <- paste("INSERT OR REPLACE INTO country (country,region,exchange,curr,curr_iso,isin,sedol,market) VALUES",
                   "(",
                   paste("\"", value_list, "\"", sep="", collapse=","),
                   ")",
                   sep="")
    logger.debug(q_str)
    tryCatch({
        dbSendQuery(conn, q_str)
                         
        }, error=function(e){ 
            logger.error(e)
            next
        }
    )
    
    # register company
    for( param in param_list ){
        # create a subdirectory for this param
        output_dir <- file.path(wkdir, param)
        if( !file.exists(output_dir) ){
            dir.create(output_dir, showWarnings=TRUE, recursive=FALSE, mode="0775")
        }
        
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

            output_filename <- file.path(output_dir, 
                                         paste(
                                             paste(param, as.character(fsid), sep="-"),
                                             ".csv", sep=""))
            log.info(output_filename)
            master_data = NULL
            #browser()
            for( curr in curr_list ){
                tryCatch({
                    if( grepl("^P_", param) ){
                        fs_str <- paste(param, "(", paste(t0,t1,freq,curr,sep=","),")",sep="")
                        str <- paste(paste("FF.ExtractFormulaHistory(\"",as.character(fsid),"\"",sep=""), ",", fs_str,")",sep="")
                        # FF.ExtractFormulaHistory("002826", P_PRICE_AVG(20120101,20121231,D,USD))
                        data <- FF.ExtractFormulaHistory(as.character(fsid), fs_str)
                        
                    } else if( grepl("^FF_", param) || grepl("^FG_", param)){
                        # FF.ExtractFormulaHistory("002826", "P_PRICE_AVG", "20120101:20121231:D","curr=USD"))
                        data <- FF.ExtractFormulaHistory(as.character(fsid),
                                                     param,
                                                     paste(t0,t1,freq,sep=":"),
                                                     paste("curr=",curr,sep=""))
                    } else {
                        print(paste("I don't know what to do with this FS param:", param))
                    }

                }, error=function(msg){
                    print(paste("ERROR!!!", msg))
                    next
                })
                if( nrow(data) < 1 ){
                    print("Empty data.  Skipping...")
                    next
                }
                non_nan <- !is.na(data[3])
                tseries <- as.data.frame(cbind(data[2][non_nan], data[3][non_nan]))
                colnames(tseries) <- c(paste("nr",nrow(tseries),sep=""), curr)

                if( !is.null(master_data) ){
                    master_data <- merge(x=master_data, 
                                     y=tseries, 
                                     by.x=colnames(tseries)[1],
                                     by.y=colnames(master_data)[1],
                                     all=TRUE)
                }
                else{
                    master_data <- tseries
                }
            }
            
            tryCatch({
                fout <- file(output_filename, "w", blocking=FALSE)
                colnames(master_data) <- c(paste("nr",nrow(master_data),sep=""),
                                           colnames(master_data)[2:ncol(master_data)])
                write.table(master_data, fout,
                            quote=FALSE, 
                            row.names=FALSE, 
                            col.names=TRUE,
                            sep=",")
                
                
            }, error=function(msg){
                print(paste("ERROR!!!", msg))
                return(NULL)
            }, finally=function(fout){
                close(fout)
            })
            close(fout)
        }
    }
}

