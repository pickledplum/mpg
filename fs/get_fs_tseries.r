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
source("recover_from_tempfile.r")
get_fs_tseries <- function(stmt, ids, t0, t1, freq, curr, querysize, output_dir, output_prefix) {
    temp_filename <- file.path(output_dir, paste(output_prefix, 
                                                 stmt, 
                                                 freq, 
                                                 paste(curr, ".tmp.csv", sep=""), 
                                                 sep="-"))

    # On exit (normal or abnormal), tell the user where to find the temporary file.
    #on.exit(function(temp_filename){ print(paste("Whole/partial results is found in", temp_filename))}, add=FALSE)
    
    # Collect company IDs of which FQL transactions failed.
    failed_ids = c(character(0)) 
    # Flag to short cut when error occured.
    failed <- FALSE
    # TRUE after the column lables are written to the output file.
    wrote.header <- FALSE
    # Dataframe to collect t-series by companies.
    tseries <- xts()
    
    # Iterate over company ids, a chunk by chunk.  Pull the entire history (t0 - t1) for each company.
    for( begin in seq(1,length(ids), querysize) ) {
        end <- min(begin+querysize-1, length(ids))
        
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
            temp_fo <- file(temp_filename, open="w")
            write.table(t(c(num.entries,colnames(t(a_bin_data)))), 
                        file=temp_fo, 
                        sep=",", 
                        row.names=FALSE, 
                        col.names=FALSE,
                        quote=FALSE)
            wrote.header <- TRUE
            write.table(t(a_bin_data), 
                        file=temp_fo, 
                        append=TRUE, 
                        sep=",", 
                        row.names=TRUE, 
                        col.names=FALSE,
                        quote=FALSE);
        }
        else{
            # Merge the new company's data to the final t-series
            #tseries <- merge(tseries, a_bin_data)
            # Append to the file as well.
            temp_fo <- file(temp_filename, open="a")
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
        write(failed_ids, file.path(output_dir, paste(output_prefix, stmt, "-", freq, "-", curr, ".failed.txt", sep="")))
    }
    data <- recover_from_tempfile(temp_filename)
    return(data)
}

# Get the daily price returns from day t0 to t1 for the companies.
# Used FQL: P_TOTAL_RETURNC()
# Return a time series (xts).  
# The data column are labeled by the company ID.
get_a_bin_data <- function(stmt, ids, t0, t1, freq, curr) {
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
