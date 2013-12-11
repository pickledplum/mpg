#####################################################################################
#
# Download a t-series from the FactSet server. 
#
# One company's entire historical data in one currency will be pulled at once
# using FF.ExtractFormulaHistory facility,
# and written/appended to a temporary file in case of a disaster.
#
# Inputs
# - stmt: FactSet formula. e.g. "P_TOTAL_RETURNC" for compound price return
# - ids:  list of company IDs.  FactSet IDs, ticker symbols, etc.
# - t0:   start date.  "YYYYMMDD" for specific data, 0 for most recent, 
#         -1 for the prior date/month/year
# - t1:   end date.  t1 >= t0
# - freq: frequency of data. 
#         Valid values are "D","M","Q","Y" for daily, monthly, quarterly and yearly, respectively.
# - curr: Three letter currency code or "local" for local currency.  
#         e.g "USD", "local", "EUR"
# - querysize: the max number of companies per query.
# - output_dir: Output directory.  Temp files are stored here.
# - output_prefix: prefix to temporary file names.
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
get_fs_tseries <- function(stmt, ids, t0, t1, freq, curr, querysize, output_dir, output_prefix, output_filename ){
    on.exit(function(temp_fo, error_fo){ close(temp_fo, error_fo)})
    
    temp_filename <- file.path(output_dir, paste(output_prefix, 
                                                 stmt, 
                                                 freq, 
                                                 paste(curr, ".tmp.csv", sep=""), 
                                                 sep="-"))
    temp_filename <- paste(output_filename, ".tmp", sep="")

    # On exit (normal or abnormal), tell the user where to find the temporary file.
    #on.exit(function(temp_filename){ print(paste("Whole/partial results is found in", temp_filename))}, add=FALSE)
    
    # Number of failed IDs
    num_failed_ids <- 0
    # Flag to short cut when error occured.
    failed <- FALSE
    # stores the ids of which transactions are failed.
    failed_ids_filename <- file.path(output_dir, 
                                     paste(output_prefix, "-", stmt, "-", freq, "-", curr, ".failed.txt", sep=""))
    if( file.exists(failed_ids_filename) ){
        file.remove(failed_ids_filename)
    }
    # TRUE after the column lables are written to the output file.
    wrote.header <- FALSE
    # Store the array of dates
    dates <- list()
    
    # Iterate over company ids, a chunk by chunk.  Pull the entire history (t0 - t1) for each company.
    for( begin in seq(1,length(ids), querysize) ) {
        end <- min(begin+querysize-1, length(ids))
        
        print(paste("(",begin,"-",end,")", "/", length(ids), ") ", "pulling ", freq, " ", stmt, " data for ", ids[begin], " (", curr, ")...", sep=""))
        
        # Query.  A matris is returned when successeful.  NULL if not.
        a_bin_data <- get_a_bin_data(stmt, ids[begin:end], t0, t1, freq, curr)
        
        # Take care of failure case.  Fill the slot with some invalid value.
        if( length(a_bin_data) < 1 ) {
            failed_ids <- ids[begin:end]
            print(paste("Query failed.  Skipping", failed_ids[1], "to", failed_ids[length(failed_ids)], "..."))
            # log IDs in file
            error_fo <- file(failed_ids_filename, "a")
            writeLines(paste(failed_ids, collapse=","), error_fo )
            close(error_fo)
            if( !wrote.header ) {
                warning("Fatal error occured at the first query. Likely network diruption. We are doomed..., bailing out.")
                stop()
            }
            else{
                # filling with some rediculous number
                bogus <- -999999
                print(paste("Filling with", length(dates), bogus, "..."))
                a_bin_data <- matrix(bogus, length(failed_ids), length(dates))
                rownames(a_bin_data) <- failed_ids
                colnames(a_bin_data) <- dates
            }
        }
        
        # Write the column lables to the output file
        # for the first successful transaction
        # NOTE: Could not have write.table(col.names=TRUE) write the header in
        #       a desireable format...
        
        if( !wrote.header ){
            # Save the number of date entries for subsequent error checking.
            total_num_ids <- length(ids)
            # Write to the file, header first, erasing the old file if it already exists.
            temp_fo <- file(temp_filename, open="w")
            # The header begins with the total number of companies, followed by
            # dates.
            dates <- colnames(a_bin_data)
            writeLines(paste(c(total_num_ids, dates), collapse=","), temp_fo)
            wrote.header <- TRUE
            write.table(a_bin_data, 
                        file=temp_fo, 
                        append=TRUE, 
                        sep=",", 
                        row.names=TRUE, 
                        col.names=FALSE,
                        quote=FALSE)
            close(temp_fo)
        }
        else{
            # Append to the file as well.
            write.table(a_bin_data, 
                        file=temp_filename, 
                        append=TRUE, 
                        sep=",", 
                        row.names=TRUE, 
                        col.names=FALSE,
                        quote=FALSE);
        } 
        
    }
    if( num_failed_ids > 0 ) {
        
        cat("!!! Failed transactions are logged in: ", failed_ids_filename)
    }
    data <- recover_from_tempfile(temp_filename)
    return(data)
}

# Query FF database, and returnt the result as an m by n matrix, where
# m is the number of IDs and n is the number of dates.
# The matrix's row names are set to IDs and the column names to dates in YYYY-MM-DD format.
get_a_bin_data <- function(stmt, ids, t0, t1, freq, curr) {
    
    # FF returns a data.frame looking like this:
    #
    #        Id       Date              val
    #  1 605860 2012-12-31              NA
    #  2 605860 2013-01-02              NA
    #  3 605860 2013-01-03              NA
    #  4 605860 2013-01-04              NA
    #  5 605860 2013-01-07              NA
    #  6 605860 2013-01-08              NA
    #  .....
    # 64 605910 2013-12-31      -0.7692277
    # 65 605910 2013-01-02       1.8087864
    # 66 605910 2013-01-03       2.2842526
    # 67 605910 2013-01-04      -0.7444143
    # 68 605910 2013-01-07       0.5000114
    # 69 605910 2013-01-08       0.2487421
    #
    # For file drop, we will make a matrix that is essentially the transpose of XTS time series.
    #
    # Id        2012-12-31  2012-01-02  2012-02-03 ...
    # 605860            NA          NA          NA ...
    # ...
    # 605910    -0.7692277   1.8087864   2.2842526 ...
    #
    #
    tryCatch({
        data <- FF.ExtractFormulaHistory(ids,stmt,paste(t0,":",t1, ":",freq), paste("curr=",curr,sep=""))
    }, error=function(msg){
        print(paste("ERROR!!!", msg))
        return(NULL)
    })
    if( length(data) < 1 ){
        print("ERROR!  Empty data.  Skipping...")
        return(NULL)
    }
    y0 <- 0
    y1 <- 0
    # Yearly data points are made on a company specific date.  So, standardize
    if( freq == "Y" ){
        if( as.integer(t1) <= 0 ){
            t1 <- as.character(Sys.Date() - as.integer(t1))
            t1 <- paste(unlist(strsplit(t1, "-", fixed=TRUE)), collapse="")
        }
        y0 <- as.integer(substr(t0,1,4))
        y1 <- as.integer(substr(t1,1,4))
        dates <- paste(as.character(seq(y0, y1, by=1)), "-12-31", sep="")
    }
    else{
        y0 <- as.integer(substr(t0,1,4))
        y1 <- as.integer(substr(t1,1,4))
        dates <- as.character(unique(unlist(data$Date)))
    }
    m <- matrix(double(), length(ids), length(dates))
    rownames(m) <- ids
    colnames(m) <- dates

    i <- 1
    for( id in ids ){
        flags <- data$Id==id
        a_comp <- data[flags,]
        vals <- unlist(a_comp[,3])
        if( length(vals) != length(dates) ){
            if( freq=="Y"){
                # the company may not have existed in certains years before or after.
                # So, align whatever we got to the standarlized years.
                m[i, ] <- array(double(), dim=length(dates))
                k <- 1
                for( timestamp in as.character(a_comp$Date) ){
                    year <- as.integer(substr(timestamp,1,4))
                    year_col <- year - y0 + 1
                    m[i,year_col] <- vals[k]
                    k <- k+1
                }
                
            }
            else{
                print(paste("Warning! FS returned data for", id, "has", length(vals), "but the others have", length(dates)))
                print(paste(vals, collapse=","))
                return(NULL)
            }
        }
        else{
            tryCatch({
                m[i,] <- unlist(a_comp[,3])
            }, error=function(msg){
                print(msg)
                print("Warning! The length of replacement is not a multiple of m[i,].")
                return(NULL)
            })
        }
        i <- i+1
    }
    return(m)
}
