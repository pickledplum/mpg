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
# Output files: A csv for each FactSet formula for each currency.
# Each csv will contain a (n+1) by (m+1) matrix, 
# where n is the number of data entries pulled (e.g. #of days for daily data,
# #of moths for monthly data).
#
# The first row contains the company IDs.
# The first column contains the time stamps.
#
# NOTES:
# For each FS formula per currency, a temporary file is created.
# The temp file stores on-going transaction results in case of
# a disaster.  The user can inspect the contents and may pick
# up from where it is left.  The temp file is created in the
# same directory as the final result files are stored: OUTPUT_PATH.
#
# $Id$
#
#####################################################################

#//////////////////////////////////////////////
# Adjustable parameters
#//////////////////////////////////////////////
DEBUG = FALSE
universe = "LARGE" # SMALL, MEDIUM, LARGE
markets <- c("ACWI") # "FM", "ACWI", DM", "EM"
# Currencies
currencies = c("USD", "local")

# use 100-500 for monthly, 25-50 for daily.
#Use lower bound for big universe (e.g. DM, EM)
binsize <- 50  

# Function to execute on exit (normal or abnormal)
on.exit(function(log){ close(log)}, add=FALSE)

# Java heap
options(java.parameters="-Xmx1000m")

# Query time out.  Default is 120 secs.  120-3600 secs on client side.
FactSet.setConfigurationItem( FactSet.TIMEOUT, 900 )

for( market in markets ) {
    # Where the output files and temp files are stored.
    OUTPUT_PATH=""
    # FS portfolio that defines the universe.
    FS_PORTFOLIO_OFDB=""
    # File prefix used for output files
    OUTPUT_FILE_PREFIX=""
    # <name>=<value> list, where <name> is a FS formula and <value> is the data frequency.
    # See the doc on top of this file.
    FS_FUNC_LIST_FILEPATH = ""
    if( market == "ACWI" ) {
        OUTPUT_PATH = "R:/temp/honda/mpg/acwi/fs_output"
        FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_MSCI_ACWI_ETF"
        OUTPUT_FILE_PREFIX = "etf-acwi-"
        t0 <- "19800101"
        FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/acwi/fs-sorted-params.txt"
    }
    if( universe == "LARGE") {
        if( market == "DM" ){
            OUTPUT_PATH = "R:/temp/honda/mpg/developed/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_DM_BY_EX"
            OUTPUT_FILE_PREFIX = "ex_dm-"
            t0 <- "19800101"
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/developed/fs-sorted-params.txt"
        } else if( market == "EM" ){
            OUTPUT_PATH = "R:/temp/honda/mpg/emerging/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_EM_BY_EX"
            OUTPUT_FILE_PREFIX = "ex-em-"
            t0 <- "19900101"
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/emerging/fs-sorted-params.txt"
        } else if( market == "FM") {
            OUTPUT_PATH = "R:/temp/honda/mpg/frontier/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_FM_BY_EX"  #4636
            OUTPUT_FILE_PREFIX = "ex-fm-"
            t0 <- "19900101"
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/frontier/fs-sorted-params.txt"
        }
    } else if( universe == "MEDIUM" ){
        if( market == "DM" ){
            OUTPUT_PATH = "R:/temp/honda/mpg/developed/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_MSCI_DEVELOPED_ETF"
            OUTPUT_FILE_PREFIX = "etf_dm-"
            t0 <- "19800101"
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/developed/fs-sorted-params.txt"
        } else if( market == "EM" ){
            OUTPUT_PATH = "R:/temp/honda/mpg/emerging/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_EM"
            OUTPUT_FILE_PREFIX = "all-em-"
            t0 <- "19900101"
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/emerging/fs-sorted-params.txt"
        } else if( market == "FM") {
            OUTPUT_PATH = "R:/temp/honda/mpg/frontier/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_FRONTIERS"  
            OUTPUT_FILE_PREFIX = "all-fm-"
            t0 <- "19900101"
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/frontier/fs-sorted-params.txt"
        }
    } else if( universe == "SMALL" ){
        if( market == "DM" ){
            OUTPUT_PATH = "R:/temp/honda/mpg/developed/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_MSCI_DEVELOPED_ETF"
            OUTPUT_FILE_PREFIX = "etf_dm-"
            t0 <- "19800101"
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/developed/fs-sorted-params.txt"
        } else if( market == "EM" ){
            OUTPUT_PATH = "R:/temp/honda/mpg/emerging/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_EM_ETF"
            OUTPUT_FILE_PREFIX = "etf-em-"
            t0 <- "19900101"
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/emerging/fs-sorted-params.txt"
        } else if( market == "FM") {
            OUTPUT_PATH = "R:/temp/honda/mpg/frontier/fs_output"
            FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_FM_ETF"  
            OUTPUT_FILE_PREFIX = "etf-fm-"
            t0 <- "19900101"  
            FS_FUNC_LIST_FILEPATH = "R:/temp/honda/mpg/frontier/fs-sorted-params.txt"
        }
    
    } else {
        print(paste("Invalid univeser size: ", universe))
        return(1)
    }
    # History range.  YYYYMMDD for specific date, 0 for most recent
    t1 = "0"
    
    #//////////////////////////////////////////////
    
    # Load the universe from FS portfolio
    df <- FF.ExtractOFDBUniverse(FS_PORTFOLIO_OFDB, "0O")
    ids <- df$Id
    
    # Read the FS function list that comes with frequency
    stmt_list <- read.table(FS_FUNC_LIST_FILEPATH, sep="=", comment.char="#", strip.white=TRUE, as.is=TRUE)
    rownames(stmt_list) <- stmt_list[,1] # fs function names
    stmt_list[,1] <- c()
    
    if( DEBUG ) t0 = "20100101"
    if( DEBUG ) ids <- head(ids,3)
    if( DEBUG ) stmt_list <- head(stmt_list,2)
    
    print(paste("Input file:", FS_FUNC_LIST_FILEPATH))
    print(paste("Output path:", OUTPUT_PATH))
    if( !file.exists(OUTPUT_PATH) ) dir.create(OUTPUT_PATH) #create the output dir if not exists.
    print(paste("FactSet portfolio:", FS_PORTFOLIO_OFDB))
    print(paste("Output file prefix:", OUTPUT_FILE_PREFIX))
    print(paste("Time frame:", t0, "-", t1))
    print(paste("Currencies:"))
    str(currencies, give.attr=FALSE, give.length=FALSE, give.head=FALSE)
    print(paste("Num of company IDs:", length(ids)))
    print(paste(cat(head(ids,10)), "..."))
    
    ########################
    # Company Basic Info
    ########################
    comp.info <- FF.ExtractDataSnapshot(ids, "FG_COMPANY_NAME(),P_DCOUNTRY()")
    rownames(comp.info) <- comp.info$Id
    comp.info$Id <- c()
    comp.info$Date <- c()
    info_file <- file.path(OUTPUT_PATH,paste(OUTPUT_FILE_PREFIX, "company-info.txt",sep=""))
    write.csv(comp.info, info_file)
    print(paste("Company basic info (id, name, domicile country) are written in: ", info_file))
    
    ########################
    # Time series
    ########################
    # Iterate over currencies
    for( curr in currencies ) {
    
        # Iterate over FS statements
        for( stmt in rownames(stmt_list) ) {
            freq <- stmt_list[stmt,1]
    
            writeLines("")
            output_filepath <- file.path(OUTPUT_PATH, 
                                         paste(OUTPUT_FILE_PREFIX, 
                                               stmt, 
                                               "-", 
                                               freq, 
                                               "-",
                                               curr, 
                                               ".csv", 
                                               sep=""))         
            log <- file(file.path(OUTPUT_PATH, paste(OUTPUT_FILE_PREFIX, "log.log", sep="")), open="a")
            
            started <- proc.time()
            
            # Get the results in xts
            a_tseries <- get_fs_tseries(stmt, ids, t0, t1, freq, curr, binsize)
            
            # Save in csv
            write.zoo(a_tseries, output_filepath, sep=",")
            
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
            print(paste("Output stored in: ", output_filepath, sep=""))
            msg <- paste("Elapsed time for market=<", market, "> param=<", stmt, "> (", curr, "): ", (finished-started)["elapsed"], "s", sep="")
            print(msg)
            writeLines(msg, log)
            close(log)
        }
    }
}
print("Good bye...")

#
# Recover the whole or partial results from the temporary file.
#
# recover_from_tempfile <- function(temp_filepath) {
#     tmp <- read.table(temp_filepath,header=FALSE,sep=",",colClasses=c("character"))
#     colnames(tmp) <- tmp[1,]
#     tmp <- subset(tmp, ID!="ID")
#     rownames(tmp) <- tmp[,1]
#     tmp$ID <- c()
#     tmp <- t(tmp)
#     ncols <- ncol(tmp)
#     nrows <- nrow(tmp)
#     print(paste("matrix",nrows,"x",ncols))
#     tmp <- matrix(as.numeric(tmp),nrow=nrows,ncol=ncols,dimnames=list(rownames(tmp),colnames(tmp)))
#     return(as.xts(tmp))
# }
#####################################################################################
#
# Download a t-series from the FactSet server. 
#
# One company's entire historical data in one currency will be pulled at once
# using FF.ExtractFormulaHistory facility,
# and written/appended to a temporary file in case of a disaster.
#
# Inputs
# - stmt: FactSet formula name. e.g. "P_TOTAL_RETURNC" for compound price return
# - ids:  list of company IDs.  FactSet IDs, ticker symbols, etc.
# - t0:   start date.  "YYYYMMDD" for specific data, 0 for most recent, 
#         -1 for the prior date/month/year
# - t1:   end date.  t1 >= t0
# - freq: frequency of data. 
#         Valid values are "D","M","Q","Y" for daily, monthly, quarterly and yearly, respectively.
# - curr: Three letter currency code or "local" for local currency.  
#         e.g "USD", "local", "EUR"
#
# Outputs: 
# An xts object containg the time series.
# The essential cotent of the xts is a (n+1) by (m+1) matrix, 
# where n is the number of data entries (e.g. #of dates for daily data)
# The first row contains the company IDs.
# The first column contains the time stamps.
# The first row contains the company IDs.
#
# Temporary file:
# Each FS transaction result is written to a temporary file in the same directory
# as the final output file is stored.  The orientation of the matrix in the temp file
# is transposed.
# 
# $Id$
#
#####################################################################################
get_fs_tseries <- function(stmt, ids, t0, t1, freq, curr, binsize) {
    #temp_filepath <- file.path(OUTPUT_PATH, paste(OUTPUT_FILE_PREFIX, stmt, "-", freq, "-", curr, ".csv.tmp", sep=""))
    
    # On exit (normal or abnormal), tell the user where to find the temporary file.
    #on.exit(function(temp_filepath){ print(paste("Whole/partial results is found in", temp_filepath))}, add=FALSE)
    
    # Collect company IDs of which FQL transactions failed.
    failed_ids = c(character(0)) 
    # Flag to short cut when error occured.
    failed <- FALSE
    # TRUE after the column lables are written to the output file.
    wrote.header <- FALSE
    # Dataframe to collect t-series by companies.
    tseries <- xts()
    
    # Iterate over company ids, a chunk by chunk.  Pull the entire history (t0 - t1) for each company.
    for( begin in seq(1,length(ids), binsize) ) {
        end <- min(begin+binsize-1, length(ids))
        
        print(paste("(",begin,"-",end,")", "/", length(ids), ") ", "pulling ", freq, " ", stmt, " data for ", ids[begin], " (", curr, ")...", sep=""))
        a_bin_data <- xts()
        tryCatch( {
            a_bin_data <- get_a_bin_data(stmt, ids[begin:end], t0, t1, freq, curr)
        }, error = function(msg) { 
            print(paste("Error!", msg))
            failed <<- TRUE
            failed_ids <<- c(failed_ids,ids[begin:end]) 
        }
        )
        if( failed ) {
            print(paste("Skipping", ids[begin], "..."))
            print(failed_ids)
            failed <- FALSE
            next
        }
        
        # Write the column lables to the output file
        # for the first successful transaction
        # NOTE: Could not have write.table(col.names=TRUE) write the header in
        #       a desireable format...

        if( !wrote.header ){
            # Save the number of date entries for subsequent error checking.
            num.entries <- length(a_bin_data)
            # Initialize the final data structure with the first entry
            #tseries <- a_bin_data
            # Write to the file, header first, erasing the old file if it already exists.
            temp_fo <- file(temp_filepath, open="w")
            write.table(t(c("ID",colnames(t(a_bin_data)))), 
                        file=temp_fo, 
                        sep=",", 
                        row.names=FALSE, 
                        col.names=FALSE)
            wrote.header <- TRUE
            write.table(t(a_bin_data), 
                        file=temp_fo, 
                        append=TRUE, 
                        sep=",", 
                        row.names=TRUE, 
                        col.names=FALSE);
        }
        else{
            # Merge the new company's data to the final t-series
            #tseries <- merge(tseries, a_bin_data)
            # Append to the file as well.
            temp_fo <- file(temp_filepath, open="a")
            write.table(t(a_bin_data), 
                        file=temp_fo, 
                        append=TRUE, 
                        sep=",", 
                        row.names=TRUE, col.names=FALSE);
        } 
        close(temp_fo)
    }
    if( length(failed_ids) > 0 ) {
        cat("!!! Failed transactions: ", failed_ids)
        write(failed_ids, file.path(OUTPUT_PATH, paste(OUTPUT_FILE_PREFIX, stmt, "-", freq, "-", curr, ".failed.txt", sep="")))
    }
    return(tseries)
}

# Get the daily price returns from day t0 to t1 for the companies.
# Used FQL: P_TOTAL_RETURNC()
# Return a time series (xts).  
# The data column are labeled by the company ID.
get_a_bin_data <- function(stmt, ids, t0, t1, freq, currency) {
    # for testing transaction failure
    #if( currency == "USD") stop(id)
    master <- xts()
    data <- FF.ExtractFormulaHistory(ids,stmt,paste(t0,":",t1, ":",freq), paste("curr=",curr,sep=""))
    # break the array of data by company IDs
    is.first <- TRUE
    for( id in ids ){
        flags <- data$Id==id
        a_comp <- data.frame(data[flags,])
        
        rownames(a_comp) <- as.Date(a_comp[,2])
        a_comp$Id <- c()
        a_comp$Date <- c()
        colnames(a_comp) <- c(id)
        
        x_comp <- as.xts(a_comp)
        if( is.first ){
            master <- x_comp
            is.first <- FALSE
        } else{
            master <- merge(master, x_comp)
        }
    }
    
    return(master)
}

