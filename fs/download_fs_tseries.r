#####################################################################
#
# Download historical data from the FactSet server.
#
# The function takes a path to a configuration file that
# defines a number of parameters to control queries.
# 
#
#
# <<<Config File>>>
# The file contains name-value pairs.  The name in this case is a parameter name that controls
# the behavior this program.
# 
# <Config Paramters>
#
# CURRENCY : String - A three letter FactSet currency code or "local" for local currency.  e.g "USD", "EUR"
# OUTPUT_DIR : String - Directory path to which all outputs are stored. 
# PORTFOLIO_OFDB : String - FactSet portfolio name
# PREFIX : String - A sting to be prefixed to all output file names.
# T0 : String - Staring date.  A double quoted integer.  
#               A relative date from T1, or an eight digit number in YYYYMMDD format for a specific date.
# T1 : String - Ending date.  A double quoted integer.
#               0 for most recent, or a eight digit number in YYYYMMDD format for a specific date.
# PARAMS_FILE : String - Path to a file listing FactSet paramters to be pulled.
#               See below for details.
# QUERY_SIZE : Integer - The maximum number of companies per query.  e.g. 100 to 500 for monthly, 
#               25 to 50 for daily
# MAX_COMPANIES : Integer - The maximum number of companies per output file.  
#               Consider 10000 to be max for a file, or R will blow up.
#
# <Example>
# CURRENCY=USD
# OUTPUT_DIR = "D:/home/honda/mpg/frontier/fs_output"
# PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_FM_BY_EX"
# PREFIX = "ex-fm"
# T0 = "19900101"
# T1 = "0"
# QUERY_SIZE = 50
# MAX_COMPANIES = 10000
#
# <<<Parameter File>
# 
# The file contains name-value pairs, where 
# the name in this case is a FactSet formula that can be called via the 
# "FF.ExtractFormulaHistory" FQL statement.
# <value> is a letter indicating the frequency of data being pulled.
# Valid letters are "D","M","Q","Y" for daily, monthly, quarterly and yearly
# prespectively.
#
#
# <<<Return>>>
# 
# The program essentially generates a m by n matrix where m is the number of dates
# and n is the number of companies.  The matrix will be divided into pieces
# according to MAX_COMPANIES and saved in a file.  For example, if the total number
# of constituents in a universe defined by PORTFOLIO_OFDB is 10000
# and MAX_COMPANIES is set to 1000, the matrix will be divided into 10 sub matrices
# and the first file will contain the first 1000 company data.
# 
#
# NOTE:
# Setting "DEBUG" to TRUE in the code will trancate the universe and 
# print out extra log messages on screen.
#
# $Id:$
#
#####################################################################
library(xts)
library(FactSetOnDemand)

#//////////////////////////////////////////////
# CPU Architecture 
#//////////////////////////////////////////////
arch <- 32

#//////////////////////////////////////////////
# Adjustable parameters
#//////////////////////////////////////////////
DEBUG = FALSE
source("get_fs_tseries.r")
source("read_config.r")
if( arch == 32 ){
    max_heap <- 2047 #MB
} else if( arch == 64 ){
    max_heap <- 2097151 # MB
} else{
    max_heap <- 2047
}

# Use max heap.  Limit when appropriate
max_heap <- min(memory.limit(), max_heap)
if( memory.size(NA) < max_heap ) memory.size(max_heap)

download_fs_tseries <- function(config_file){

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
    
    if( DEBUG ) T0 <- "20000101"
    if( DEBUG ) T1 <- "20131231"
    if( DEBUG ) all_ids <- head(all_ids,10)
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
            a_tseries <- get_fs_tseries(stmt, ids, T0, T1, freq, 
                                        CURRENCY, QUERY_SIZE, OUTPUT_DIR, PREFIX,
                                        output_filename)
            
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
            gc()
            rm(a_tseries)
            gc()
        }
    }
    print("Good bye...")
}


#download_fs_tseries("D:/home/honda/mpg/dummy/download_fs_tseries.conf")


#download_fs_tseries("D:/home/honda/mpg/developed/download_fs_tseries-usd.conf")
#download_fs_tseries("D:/home/honda/mpg/developed/download_fs_tseries-local.conf")
#download_fs_tseries("D:/home/honda/mpg/developed/download_fs_tseries-fundamentals-usd.conf")

#download_fs_tseries("D:/home/honda/mpg/frontier/download_fs_tseries-cap-usd.conf")


########################
download_fs_tseries("D:/home/honda/mpg/emerging/download_fs_tseries-fundamentals-usd.conf")
download_fs_tseries("D:/home/honda/mpg/emerging/download_fs_tseries-local.conf")
download_fs_tseries("D:/home/honda/mpg/emerging/download_fs_tseries-cap-usd.conf")

download_fs_tseries("D:/home/honda/mpg/acwi/download_fs_tseries-cap-usd.conf")