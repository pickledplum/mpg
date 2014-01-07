# Populate company table
#
# ISIN charvar(25)
# Name Text(100)
# Country charvar(20)
# Region charvar(20)
# Exchange charvar(20)
# Market charvar(10)
#
library(RSQLite)
#library(RSQLite.extfuns)
FILL_COUNTRY <- FALSE
FILL_COMPANY <- FALSE
FILL_DATA <- TRUE
WHICH_FREQ <- "M"

db <- "D:/home/honda/sqlite-db/historical.sqlite3"

# Populate company data table
company_file_list <- c("D:/home/honda/mpg/acwi/fs_output/etf-acwi-company-info.txt",
                       "D:/home/honda/mpg/frontier/fs_output/ex-fm-company-info.txt",
                       "D:/home/honda/mpg/emerging/fs_output/ex-em-company-info.txt",
                       "D:/home/honda/mpg/developed/fs_output/ex-dm-company-info.txt" 
                       )
data_dir_list <- c("D:/home/honda/mpg/acwi/fs_output",
                   "D:/home/honda/mpg/frontier/fs_output",
                   "D:/home/honda/mpg/emerging/fs_output",
                   "D:/home/honda/mpg/developed/fs_output"
                   )

data_prefix_list <- c("etf-acwi-",
                      "ex-fm-",
                      "ex-em-",
                      "ex-dm-")
#
if( FILL_COUNTRY ) {
    conn <- dbConnect( SQLite(), db)
    for( company_filename in company_file_list ){
        
        data <- read.table(company_filename, header=TRUE, sep=",")
        ids <- as.character(data[[1]])
        companies <- as.character(data[[2]])
        countries <- as.character(data[[3]])
        regions <- as.character(data[[4]])
        exchanges <- as.character(data[[5]])
        currencies <- as.character(data[[6]])
        currency_codes <- as.character(data[[7]])
        
        n <- length(ids)
        for( i in seq(1,n,1) ){
            id <- ids[i]
            company <- companies[i]
            country <- countries[i]
            region <- regions[i]
            exchange <- exchanges[i]
            currency <- currencies[i]
            currency_code <- currency_codes[i]
            if( country == "" ){
                print("Corrupt data, skipping...")
                next
            }
            # country table
            q_str <- paste("SELECT country_id FROM country WHERE name=\"",country, "\"", sep="")
            print(q_str)
            res <- dbSendQuery(conn, q_str)
            d <- fetch(res, n=1)
            dbClearResult(res)

            if( nrow(d) < 1 ){
                value_list <- c(country, region, exchange,currency,currency_code)       
                value_str <- paste("\"", value_list, "\"", sep="", collapse=",")
                tryCatch(dbSendQuery(conn,
                            paste("INSERT INTO country(name,region,exchange,currency,currency_iso) VALUES(",value_str,")",sep="")
                ), error=function(e){ print(paste("Already exist:", country))}
                )
                dbCommit(conn)
                q_str <- paste("SELECT country_id FROM country WHERE name=\"",country, "\"", sep="")
                print(q_str)
                res <- dbSendQuery(conn, q_str)
                d <- fetch(res, n=1)
                dbClearResult(res)
            }
        }
    }
    dbDisconnect(conn)
}
if( FILL_COMPANY ){
    conn <- dbConnect( SQLite(), db)
    for( company_filename in company_file_list ){
        
        data <- read.table(company_filename, header=TRUE, sep=",")
        ids <- as.character(data[[1]])
        companies <- as.character(data[[2]])
        countries <- as.character(data[[3]])
        regions <- as.character(data[[4]])
        exchanges <- as.character(data[[5]])
        currencies <- as.character(data[[6]])
        currency_codes <- as.character(data[[7]])
        
        n <- length(ids)
        for( i in seq(1,n,1) ){
            id <- ids[i]
            company <- companies[i]
            country <- countries[i]
            region <- regions[i]
            exchange <- exchanges[i]
            currency <- currencies[i]
            currency_code <- currency_codes[i]
            if( country == "" ){
                print(paste("No country specificiation.  Skipping", company, "..."))
                next
            }
            q_str <- paste("SELECT country_id FROM country WHERE name=\"",country, "\"", sep="")
            print(q_str)
            res <- dbSendQuery(conn, q_str)
            d <- fetch(res, n=1)
            dbClearResult(res)

            country_code <- as.integer(d[1])
            #browser()
            if( nrow(d) < 1 ){
                stop()
            }
    
            value_list <- c(id, company)#, country_code)
            value_str <- paste("\"", value_list, "\"",sep="", collapse=",")
            value_str <- paste(value_str, country_code, sep=",")
            q_str <- paste("INSERT INTO company(isin,name,country_id) VALUES(",value_str,")",sep="")
            print(q_str)
            tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
            dbCommit(conn)
        }
    }
    dbDisconnect(conn)
}
if( FILL_DATA ){
    
    hash <- new.env(hash=TRUE)
    for( i in 1:length(data_dir_list)){
        assign( data_dir_list[i], data_prefix_list[i], hash)
    }
    ls(hash)
    
    conn <- dbConnect( SQLite(), db)
    table_list <- dbGetQuery(conn, "SELECT name FROM sqlite_master WHERE type='table'")
    table_list <- table_list[[1]]
    existing_table_list <- c(character())
    for( data_dir in data_dir_list[2] ){
        prefix <- hash[[data_dir]]
        regx <- paste("^",prefix,sep="")
        filename_list <- list.files(data_dir, full.names=FALSE, pattern=regx)
        errorlog <- file(file.path(data_dir, "error.log"), "w", blocking=FALSE)

        #filename_list <- c("ex-fm-FF_ASSETS_CURR-M-USD.csv",
        #                   "ex-fm-P_TOTAL_RETURNC-D-local.csv", 
        #                   "ex-fm-P_TOTAL_RETURNC-D-USD.csv"
        #                   )
        for( filename in filename_list ){
            if( length(grep(".csv$", filename )) < 1 ) {
                next
            }
            writeLines(paste("<", filename, ">", sep=""), errorlog)
            
            print(paste("filename:", filename))
            meat <- sub(regx, "", filename)
            meat <- sub(".csv$", "", meat)
            tokens <- unlist(strsplit(meat, "-"))
            param <- tokens[1]
            freq <- tokens[2]
            if( freq != WHICH_FREQ ){
                next
            }
            curr <- tokens[3]
            stem_tablename <- paste(param,freq,sep="-")
            
            fin <- file(file.path(data_dir, filename), "r", blocking=FALSE)
            line <- readLines(fin,n=1)
            header <- unlist(strsplit(line, ","))
            tokens <- strsplit(header, ",")
            if( length(tokens[1]) < 1 ) {
                
                print(paste("Bad file:", filename))
                writeLines(paste("BAD FILE:", filename), errorlog )
                nrows <- as.integer(tokens[2])
                company_id_list <- unlist(header[3:length(header)])
            } else{
                nrows <- as.integer(tokens[1])
                company_id_list <- unlist(header[2:length(header)])
            }
            ncols <- length(company_id_list)

            block_size <- 500
            for( start in seq(1, nrows, block_size)) {
                end <- min(start + block_size - 1, nrows)
                actual_size <- end-start+1
                m <- matrix(nrow=actual_size, ncol=ncols)
                dates <- matrix(nrow=actual_size, ncol=1)
                for( i in seq(1:actual_size)){
                    line <- readLines(fin, n=1)
                    tokens <- unlist(strsplit(line, ","))
                    date <- tokens[1]
                    
                    if( length(grep("/", date)) > 0) {
                        d <- as.character(as.Date(date, format="%m/%d/%Y"))
                    } else{
                        d <- as.character(as.Date(date))
                    }   
                    dates[i,1] <- d
                    
                    m[i,] <- tokens[2:length(tokens)]
                }

                rownames(m) <- dates[,1]
                colnames(m) <- company_id_list
    
                for( company_id in company_id_list) {
                    tablename <- paste(company_id, stem_tablename, sep="-")
                    #if( tablename != 'B3C9VK-FF_EBIT_OPER-M'){
                    #    next
                    #}
                    is_first <- TRUE
                    values <- ""
                    for( date in rownames(m) ){
                        val <- m[date,company_id]
                        if( val == "NA" && is_first ){
                            next
                        }
                        
                        if( is_first ){
                            is_first <- FALSE
                            q_str <- paste("CREATE TABLE IF NOT EXISTS '", tablename, "'(date DATE PRIMARY_KEY NOT NULL UNIQUE, USD REAL, local REAL)", sep="")
                            print(q_str)
                            tryCatch(dbSendQuery(conn, q_str), 
                                     error=function(e){
                                         print(e)
                                         writeLines(q_str, errorlog)
                                     }
                            )
                        } 
                        
                        if( WHICH_FREQ == "D" ){
                            if( val != "NA" ){
                            
                                q_str <- paste("SELECT * FROM '", tablename, "' WHERE Date='", date, "'", sep="")
                                print(q_str)
                                result <- data.frame()
                                tryCatch({result <- dbGetQuery(conn, q_str)},
                                         error=function(e){
                                             print(e)
                                             writeLines(q_str, errorlog)
                                         }
                                )
                                if( nrow(result) > 0 ){
                                    usd_val <- ifelse(!is.na(result$USD), result$USD, "NULL")
    
                                    local_val <- ifelse(!is.na(result$local), result$local, "NULL")
                                } else{  
                                    usd_val <- "NULL"
                                    local_val <- "NULL"
                                }
                                if( curr == "USD" ){
                                    usd_val <- ifelse(!is.na(val), as.numeric(val), "NULL")
                                } else{
                                    local_val <- ifelse(!is.na(val), as.numeric(val), "NULL")
                                }
                                q_str <- paste("INSERT OR REPLACE INTO '", tablename, "' (Date, USD, local) VALUES ",
                                               paste("(date('", date, "'), ", usd_val, ",", local_val, ")",sep=""), sep="")
                                
                                print(q_str)
                                tryCatch(dbSendQuery(conn,q_str), 
                                         error=function(e){
                                             print(e)
                                             writeLines(q_str, errorlog)
                                         }
                                )
                            }
                            
                        } else { # "M"
                            usd_val <- ifelse(val!="NA", as.numeric(val), "NULL")
                            tuple <- paste("(date('", date, "'),", usd_val, ")", sep="")
                            values <- ifelse(nchar(values)==0, tuple, paste(values, ",", tuple, sep=""))
                        }
                    }
                    if( WHICH_FREQ == "D" ){
                        
                    } else {
                        if( nchar(values) < 1 ){
                            next
                        }
                        q_str <- paste("INSERT OR REPLACE INTO '", tablename, "' (Date, USD) VALUES ",
                                       values, sep="")       
                        print(q_str)
                        tryCatch(dbSendQuery(conn, q_str), 
                                 error=function(e){
                                     print(e)
                                     writeLines(q_str, errorlog)
                                 }
                        )
                    }
                    dbCommit(conn)
                }
                close(fin)
                all_vars = ls()
                vars_to_remove = all_vars[-c(which(all_vars=="filename_list"),
                                             which(all_vars=="errorlog"),
                                             which(all_vars=="regx"),
                                             which(all_vars=="prefix"),
                                             which(all_vars=="data_dir"),
                                             which(all_vars=="WHICH_FREQ"),
                                             which(all_vars=="conn"),
                                             which(all_vars=="hash"))]
                rm(list=vars_to_remove)
#                 remove(m)
#                 remove(dates)
#                 remove(company_id_list)
            }
            
        }

        close(errorlog)
    }
    dbDisconnect(conn)
}

