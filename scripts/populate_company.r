# Populate company, country tables
library(RSQLite)
source("logger.r")
FILL_COUNTRY <- TRUE
FILL_COMPANY <- TRUE

populate_company <- function(conn, company_file_list, market_designations){

    hash <- new.env(hash=TRUE)
    for( i in seq(1,length(market_designations))){
        
        x <- market_designations[i]
        y <- company_file_list[i]
        assign(x,y,envir=hash)
    }
    if( FILL_COUNTRY ) {
        q_str <- paste("CREATE TABLE IF NOT EXISTS country (country_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, country VARCHAR(15), region VARCHAR(20),exchange VARCHAR(25),currency VARCHAR(25),currency_iso VARCHAR(3),market VARCHAR(10))")
        dbSendQuery(conn, q_str)
        for( market_designation in market_designations[1] ){
            company_filename <- get(market_designation, hash)
            data <- read.table(company_filename, header=TRUE, sep=",")
            ids <- as.character(data[[1]])
            companies <- as.character(data[[3]])
            countries <- as.character(data[[4]])
            regions <- as.character(data[[5]])
            exchanges <- as.character(data[[6]])
            currencies <- as.character(data[[7]])
            currency_codes <- as.character(data[[8]])
    
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
                q_str <- paste("SELECT * FROM country WHERE country='", country, "'",sep="")
                print(q_str)
                tryCatch({
                        res <- dbGetQuery(conn, q_str)
     
                    },
                    error=function(e){
                        print(e)
                        next
                    }
                )
                if( nrow(res) > 0 ){
                    print("already registered")
                    next
                }
                value_list <- c(country, region, exchange,currency,currency_code,market_designation)       
                value_str <- paste("\"", value_list, "\"", sep="", collapse=",")
                tryCatch(dbSendQuery(conn,
                                     paste("INSERT OR REPLACE INTO country (country,region,exchange,currency,currency_iso,market) VALUES(",value_str,")",sep="")
                ), error=function(e){ print(e); print(paste("Already exist:", country))}
                )
                q_str <- paste("SELECT country_id FROM country WHERE country=\"",country, "\"", sep="")
                print(q_str)
                res <- dbSendQuery(conn, q_str)
                d <- fetch(res, n=1)
                dbClearResult(res)
            }
        }
    }
    if( FILL_COMPANY ){
        
        q_str <- paste("CREATE TABLE IF NOT EXISTS company (isin VARCHAR(15) PRIMARY KEY NOT NULL UNIQUE, company TEXT(100), country_id INTEGER, FOREIGN KEY(country_id) REFERENCES country(country_id))")
        dbSendQuery(conn, q_str)
        for( market_designation in market_designations[1] ){
            company_filename <- get(market_designation, hash)
            data <- read.table(company_filename, header=TRUE, sep=",")
            ids <- as.character(data[[1]])
            companies <- as.character(data[[3]])
            countries <- as.character(data[[4]])
            regions <- as.character(data[[5]])
            exchanges <- as.character(data[[6]])
            currencies <- as.character(data[[7]])
            currency_codes <- as.character(data[[8]])
            
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
                q_str <- paste("SELECT country_id FROM country WHERE country=\"",country, "\"", sep="")
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
                q_str <- paste("INSERT INTO company(isin,company,country_id) VALUES(",value_str,")",sep="")
                print(q_str)
                tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
            }
        }
    }
}