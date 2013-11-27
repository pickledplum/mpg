#####################################################################
#
# Driver for get_fs_tseries()
# 
# Download historical data from the FactSet server.
#
# Input file: A text file containg <name>=<value> pairs, where 
# <name> is a FactSet formula that can be called via the 
# "FF.ExtractFormulaHistory" FQL statement.
# <value> is a letter indicating the frequency of data being pulled.
# Valid letters are "D","M","Q","Y" for daily, monthly, quarterly and yearly
# prespectively.
# 
# Output files: A csv for each FactSet formula for each CURRENCYency.
# Each csv will contain a (n+1) by (m+1) matrix, 
# where n is the number of data entries pulled (e.g. #of days for daily data,
# #of moths for monthly data).
#
# The first row contains the company IDs.
# The first column contains the time stamps.
#
# NOTES:
# For each FS formula per CURRENCYency, a temporary file is created.
# The temp file stores on-going transaction results in case of
# a disaster.  The user can inspect the contents and may pick
# up from where it is left.  The temp file is created in the
# same directory as the final result files are stored: OUTPUT_DIR.
#
# $Id$
#
#####################################################################
library(xts)
library(FactSetOnDemand)
#//////////////////////////////////////////////
# Adjustable parameters
#//////////////////////////////////////////////
DEBUG = FALSE
source("get_fs_tseries.r")
source("read_config.r")

download_fs_tseries <- function(config_file){
    # Java heap
    options(java.parameters="-Xmx1000m")
    
    # Query time out.  Default is 120 secs.  120-3600 secs on client side.
    FactSet.setConfigurationItem( FactSet.TIMEOUT, 900 )
    
    # config parameters
    config <- read_config(config_file)
    
    # moving to local variables.  throws error if the accessed parameter does not exist in the file.
    PREFIX <- config$PREFIX
    OUTPUT_DIR <- config$OUTPUT_DIR
    PORTFOLIO_OFDB <- config$PORTFOLIO_OFDB
    T0 <- config$T0
    T1 <- config$T1
    PARAMS_FILE <- config$PARAMS_FILE
    CURRENCY = config$CURRENCY
    MAX_COMPANIES <- config$MAX_COMPANIES
    
    # use 100-500 for monthly, 25-50 for daily.
    #Use lower bound for big universe (e.g. DM, EM)
    QUERY_SIZE <- config$QUERY_SIZE  
    
    #//////////////////////////////////////////////
    
    # Load the universe from FS portfolio
    df <- FF.ExtractOFDBUniverse(PORTFOLIO_OFDB, "0O")
    all_ids <- df$Id
    
    # Read the FS function list that comes with frequency
    stmt_list <- read.table(PARAMS_FILE, sep="=", comment.char="#", strip.white=TRUE, as.is=TRUE)
    rownames(stmt_list) <- stmt_list[,1] # fs function names
    stmt_list[,1] <- c()
    
    if( DEBUG ) T0 <- "20100101"
    if( DEBUG ) T1 <- "20100103"
    if( DEBUG ) all_ids <- head(all_ids,10001)
    if( DEBUG ) stmt_list <- head(stmt_list,1)
    
    print(paste("Input file:", PARAMS_FILE))
    print(paste("Output dir:", OUTPUT_DIR))
    if( !file.exists(OUTPUT_DIR) ) dir.create(OUTPUT_DIR) #create the output dir if not exists.
    print(paste("FactSet portfolio:", PORTFOLIO_OFDB))
    print(paste("Prefix:", PREFIX))
    print(paste("Time frame:", T0, "-", T1))
    print(paste("Currency:", CURRENCY))
    print(paste("Num of company ids:", length(all_ids)))
    print(paste(cat(head(all_ids,10)), "..."))
    print(paste("num of companies per query: ", QUERY_SIZE))
    print(paste("Max companies per file:", MAX_COMPANIES))
    
    ########################
    # Company Basic Info
    ########################
    comp.info <- FF.ExtractDataSnapshot(all_ids, "FG_COMPANY_NAME(),P_DCOUNTRY()")
    rownames(comp.info) <- comp.info$Id
    comp.info$Id <- c()
    comp.info$Date <- c()
    info_file <- file.path(OUTPUT_DIR,paste(PREFIX, "company-info.txt",sep="-"))
    write.csv(comp.info, info_file)
    print(paste("Company basic info (id, name, domicile country) are written in: ", info_file))
    
    ########################
    # Time series
    ########################
    
    # Iterate over FS statements
    for( stmt in rownames(stmt_list) ) {
        freq <- stmt_list[stmt,1]

        for( company_index0 in seq(1, length(all_ids), MAX_COMPANIES )) {
            company_index1 <- min(length(all_ids), company_index0 + MAX_COMPANIES - 1)
            ids <- all_ids[company_index0:company_index1]
            
            writeLines("")
            output_filename <- file.path(OUTPUT_DIR, 
                                         paste(PREFIX, 
                                               stmt, 
                                               freq, 
                                               CURRENCY,
                                               company_index0,
                                               paste(company_index1, ".csv", sep=""), 
                                               sep="-"))         
            if( file.exists(output_filename) ){
                # Warn them
                secs <- 5
                print(paste("File already exists:", output_filename ))
                print(paste("Terminate the program in", secs, "seconds, or the file will be overwritten."))
                Sys.sleep(secs) # give the user a chance to interrupt execution
                print(paste(output_filename, " is overwritten."))
            }
            
            started <- proc.time()
            
            # Get the results in xts
            a_tseries <- get_fs_tseries(stmt, ids, T0, T1, freq, CURRENCY, QUERY_SIZE, OUTPUT_DIR, PREFIX)
            
            # Save in csv
            write.zoo(a_tseries, output_filename, sep=",")
            
            finished <- proc.time()
                    
            if( DEBUG ) {
                writeLines("")
                print("------- See if the live data structure looks good --------------------")
                if( length( a_tseries ) > 0 ) {
                    print(tail(a_tseries)) 
                } else {
                    print("Some jobs failed.  Failed companies are: ")
                    print(a_tseries)
                }
                print("--------------------------------------------------------")
            }
            writeLines("")
            print(paste("Output stored in: ", output_filename, sep=""))
            msg <- paste("Elapsed time for market=<", PREFIX, "> param=<", stmt, "> (", CURRENCY, "): ", (finished-started)["elapsed"], "s", sep="")
            print(msg)
        }
    }
    print("Good bye...")
}



download_fs_tseries("D:/home/honda/mpg/frontier/download_fs_tseries-usd.conf")
download_fs_tseries("D:/home/honda/mpg/frontier/download_fs_tseries-local.conf")
download_fs_tseries("D:/home/honda/mpg/frontier/download_fs_tseries-fundamentals-usd.conf")
download_fs_tseries("D:/home/honda/mpg/developed/download_fs_tseries-usd.conf")
download_fs_tseries("D:/home/honda/mpg/developed/download_fs_tseries-local.conf")
download_fs_tseries("D:/home/honda/mpg/developed/download_fs_tseries-fundamentals-usd.conf")
download_fs_tseries("D:/home/honda/mpg/emerging/download_fs_tseries-usd.conf")
download_fs_tseries("D:/home/honda/mpg/emerging/download_fs_tseries-local.conf")
download_fs_tseries("D:/home/honda/mpg/emerging/download_fs_tseries-fundamentals-usd.conf")