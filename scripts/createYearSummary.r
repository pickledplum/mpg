#Create table summarizing the market values by year
tryCatch({
    library(tools)
    library(xts)
    library(RSQLite)
    
    source("logger.r")
    source("tryDb.r")
    source("assert.r")
    source("tryExtract.r")
    source("is.empty.r")
    source("julianday.r")
    source("extractDirFromPath.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)
#' Create the yearly summary of the valuation factor across all companies in the database
#' @param meta_conn Connection to the meta database
#' @param fql The FQL parameter for the valuation factor of interest
#' @return A table named "yearly_<FQL>" will be created in the meta database, where <FQL> is like "FF_WKCAP".
#' @see The "FQL" table in the meta db for available FactSet parameters (valuation factors)
#' 
createYearSummary <- function(meta_conn, fql, do_drop=FALSE ){
    
    dbinfo <- dbGetInfo(meta_conn)
    dbdir <- extractDirFromPath(dbinfo$dbname)
    logger.debug(paste("Database:", dbinfo$dbname))
            
    ########################################
    # Collect all the market values by year
    ########################################

    # collect table names from CATALOG table
    #SELECT factset_id, tablename, usd, earliest, latest FROM catalog
    print(dbGetQuery(meta_conn, "SELECT * FROM sqlite_master"))
    dbname_list <- tryGetQuery(meta_conn, "SELECT DISTINCT dbname FROM catalog")
    
    # Lookup table for open connections to the sub dbs.
    dbhash <- new.env(hash=TRUE)
    
    catalog_ret <- trySelect(meta_conn, 
                           "catalog", 
                            c("factset_id", "dbname", "tablename", "usd", "earliest", "latest"),
                            c(paste("fql=", enQuote(fql),sep="")),
                           TRUE)
    
    ########################################
    # Create summary table
    ########################################
    summary_tablename <- paste("yearly_", fql, sep="")
    years <- seq(1980, 2014)
    yearends <- seq(as.Date("1980-12-31"), as.Date("2014-12-31"), by="year")
    years_spec <- paste(enQuote(years), "FLOAT")
    specs <- c("factset_id VARCHAR(20)",
               years_spec)
    
    tryDrop(meta_conn, summary_tablename)
    tryCreateTableIfNotExists(meta_conn, summary_tablename, specs)
    logger.info(paste("Created table:", summary_tablename))
    stack <- c()
    julian_yearends <- julianday(yearends)
    for( i in seq(1, nrow(catalog_ret)) ){
        attributes <- catalog_ret[i,]
        fsid <- attributes$factset_id
        dbname <- attributes$dbname
        tablename <- attributes$tablename
        has_value <- attributes$usd
        julianday_earliest_data_on <- attributes$earliest
        julianday_latest_data_on <- attributes$latest
    
        earliest_date <- unjulianday(julianday_earliest_data_on)
        m <- regexpr("^[0-9]+{4}", earliest_date)
        earliest_year <- as.numeric(regmatches(earliest_date, m))
        earliest_year0101 <- as.Date(paste(earliest_year, "-01-01", sep=""))
        earliest_year1231 <- as.Date(paste(earliest_year, "-12-31", sep=""))
        
        latest_date <- unjulianday(julianday_latest_data_on)
        m <- regexpr("^[0-9]+{4}", latest_date)
        latest_year <- as.numeric(regmatches(latest_date, m))
        latest_year0101 <- as.Date(paste(latest_year, "-01-01", sep=""))
        latest_year1231 <- as.Date(paste(latest_year, "-12-31", sep=""))
        
        julian_yearstarts <- julianday(seq(earliest_year0101, latest_year0101, by="year"))
        julian_yearends <- julianday(seq(earliest_year1231, latest_year1231, by="year"))
        
        ###################################################
        # Get the open connection if there is already.
        # If not, open and register with the hashtable.
        ###################################################
        if( exists(dbname, envir=dbhash ) ){
            tseries_dbconn <- get(dbname, envir=dbhash)
        } else{
            dbpath <- file.path(dbdir, dbname)
            tseries_dbconn <- dbConnect(SQLite(), dbpath)
            assign(dbname, tseries_dbconn, envir=dbhash)
        }
        
        q_str <- ""
        for( k in seq(1, length(julian_yearstarts) ) ){
            if( k > 1 ){
                q_str <- paste(q_str, "UNION")
            }
            q0_str <- paste("SELECT date(max(date)) as date, usd",
                            "FROM", enQuote(tablename), 
                            "WHERE date BETWEEN", julian_yearstarts[k], "AND", julian_yearends[k])
            logger.debug(q0_str)
            q_str <- paste(q_str, q0_str)
        }
        r <- tryGetQuery(tseries_dbconn, q_str)

        valid_years <- year(r$date)
        valid_vals <- r$usd

        toremove <- which(is.na(valid_vals))
        if( !is.empty(toremove) ){
            valid_years <- valid_years[-toremove]
            valid_vals <- valid_vals[-toremove]
        }
        if(is.empty(valid_vals) ){
            logger.warn(paste("Empty table:", tablename))
            next
        }
        
        tryInsertOrReplace(meta_conn, 
                  summary_tablename, 
                  c("factset_id", valid_years),
                  c(enQuote(fsid), valid_vals)
        )
        
        for( pending_result in dbListResults(tseries_dbconn) ){
            dbClearResult(pending_result)
        }
    }
    
    for( pending_result in dbListResults(meta_conn) ){
        dbClearResult(pending_result)
    }
    for( dbname in ls(dbhash) ){
        tseries_dbconn <- get(dbname, envir=dbhash)
        dbDisconnect(tseries_dbconn)
    }
    
    dbDisconnect(meta_conn)
    
    logger.info(paste("Populated table:", summary_tablename))
    return(0)
}
year <- function( date_list ){
    years <- c()
    for( date in date_list ){
        if( class(date) == "Date" ){
            date <- as.character(date)
        }
        tokens <- strsplit(date, "-")
        years <- c(years, tokens[[1]][1])
    }
    return(years)
    
}
