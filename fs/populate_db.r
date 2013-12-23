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

db <- "D:/home/honda/sqlite-db/security.sqlite3"

# Populate company data table
company_file_list <- c("D:/home/honda/mpg/acwi/fs_output/etf-acwi-company-info.txt",
                       "D:/home/honda/mpg/developed/fs_output/ex-dm-company-info.txt",
                       "D:/home/honda/mpg/emerging/fs_output/ex-em-company-info.txt",
                       "D:/home/honda/mpg/frontier/fs_output/ex-fm-company-info.txt")

data_dir_list <- c("D:/home/honda/mpg/acwi/fs_output",
                       "D:/home/honda/mpg/developed/fs_output",
                       "D:/home/honda/mpg/emerging/fs_output",
                       "D:/home/honda/mpg/frontier/fs_output")

data_prefix_list <- c("etf-acwi-",
                      "ex-fm-",
                      "ex-em-",
                      "ex-dm-")

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
    for( data_dir in data_dir_list[1:1] ){
        prefix <- hash[[data_dir]]
        regx <- paste("^",prefix,sep="")
        filename_list <- list.files(data_dir, full.names=FALSE, pattern=regx)
        for( filename in filename_list ){
            if( length(grep(".csv$", filename )) < 1 ) {
                next
            }
            meat <- sub(regx, "", filename)
            meat <- sub(".csv$", "", meat)
            tokens <- unlist(strsplit(meat, "-"))
            param <- tokens[1]
            freq <- tokens[2]
            curr <- tokens[3]
            stem_tablename <- paste(param,freq,sep="-")
            
            fin <- file(file.path(data_dir, filename), "r", blocking=FALSE)
            header <- read.table(fin, sep=",", as.is=FALSE, nrows=1, quote="", colClasses="character", strip.white=TRUE)
            company_id_list <- unlist(header[2:length(header)])
            close(fin)
            # Remove leading X from all-numeric company ID.
            # The "X" has been attached by R when collecting data from FS database
            for( company_id in company_id_list[1:1]) {
                company_id <- sub("^\"", "", company_id)   
                company_id <- sub("\"$", "", company_id)
                #print(company_id)
                if( length(grep("^X[0-9]", company_id)) > 0 ){
                    print(paste("----- Gotcha!", company_id))
                    company_id <- sub("^X", "", company_id)
                }

                tablename <- paste(company_id, stem_tablename, sep="-")
                # create <COMPANY_ID>-<PARAM>-<FREQ> table if not exists
                q_str <- paste("CREATE TABLE IF NOT EXISTS '", tablename, "'(date DATE PRIMARY_KEY NOT NULL UNIQUE, usd REAL, local REAL)", sep="")
                print(q_str)
                tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
            }
            dbCommit(conn)
        }
    }
    dbDisconnect(conn)
}

