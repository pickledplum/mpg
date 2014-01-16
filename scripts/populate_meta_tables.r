# Populate company, country tables
library(RSQLite)
source("logger.r")

populate_meta_tables <- function(conn, meta_data_source, 
                                 do_create_country=TRUE,
                                 do_create_company=TRUE,
                                 do_create_index=TRUE ){
    company_file_list <- meta_data_source$company_info 
    market_designations <- meta_data_source$market
    index_designations <- meta_data_source$index
    print(index_designations)
    company_by_market <- new.env(hash=TRUE)
    for( i in seq(1,length(market_designations))){
        
        x <- market_designations[i]
        y <- company_file_list[i]
        assign(x,y,envir=company_by_market)
    }
    company_by_index <- new.env(hash=TRUE)
    for( i in seq(1,length(index_designations))){
        
        x <- index_designations[i]
        y <- company_file_list[i]
        print(paste("x=",x,", y=",y))
        assign(x,y,envir=company_by_index)
    }
    if( do_create_country ) {
        q_str <- paste("CREATE TABLE IF NOT EXISTS country (country_id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, country VARCHAR(15), region VARCHAR(20),exchange VARCHAR(25),currency VARCHAR(25),currency_iso VARCHAR(3),market VARCHAR(10))")
        dbSendQuery(conn, q_str)
        for( market_designation in market_designations ){
            company_filename <- get(market_designation, company_by_market)
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
            }
        }
    }
    if( do_create_company ){
        
        q_str <- paste("CREATE TABLE IF NOT EXISTS company (isin VARCHAR(15) PRIMARY KEY NOT NULL UNIQUE, company TEXT(100), country_id INTEGER, FOREIGN KEY(country_id) REFERENCES country(country_id))")
        dbSendQuery(conn, q_str)
        for( market_designation in market_designations ){
            company_filename <- get(market_designation, company_by_market)
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
                q_str <- paste("INSERT INTO company (isin,company,country_id) VALUES(",value_str,")",sep="")
                print(q_str)
                tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
            }
        }
    }
    if( do_create_index ){

        fin <- file("index_list.txt", "r", blocking=FALSE)
        index_info <- read.csv(fin)
        index_list <- index_info$index
        index_descriptions <- index_info$descript
        index_type <- index_info$type
        q_str <- paste("CREATE TABLE IF NOT EXISTS financial_index (index_id VARCHAR(25) PRIMARY KEY NOT NULL UNIQUE, index_descrip TEXT(100))")
        tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
        

        # creating (financial)index table, listing various indices or ETFs.
        for( i in seq(1, length(index_list)) ){
            
            index_name <- index_list[i]
            index_descrip <- index_descriptions[i]
            constituents_tablename <- paste(tolower(index_name))
            
            q_str <- paste("INSERT OR REPLACE INTO financial_index (index_id, index_descrip) VALUES (",
                           paste(paste("\"", index_name, "\"", sep=""),
                                 paste("\"", index_descrip, "\"", sep=""),
                                 sep=","),
                           
                           ")", sep="")
            print(q_str)
            tryCatch(dbSendQuery(conn, q_str), 
                     error=function(e){
                         print(e)
                        }
            )
        }
        dbCommit(conn)
        
        # index designation
        q_str <- "CREATE TABLE IF NOT EXISTS index_designation (isin, index_id, PRIMARY KEY(isin, index_id), FOREIGN KEY(isin) REFERENCES company(isin), FOREIGN KEY(index_id) REFERENCES financial_index(index_id))"
        print(q_str)    
        tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
        
        for( index_designation in index_designations ){
            company_filename <- get(index_designation, company_by_index)
            data <- read.table(company_filename, header=TRUE, sep=",")
            isins <- as.character(data[[1]]) # isin
            companies <- as.character(data[[3]]) # company name
            for( i in seq(1, length(isins)) ){
                isin <- isins[i]
                company <- companies[i]
                q_str <- paste("SELECT * FROM company WHERE isin=\"", isin, "\"", sep="")
                print(q_str)
                tryCatch({
                        q <- dbGetQuery(conn, q_str)
                        #browser()
                        if( nrow(q) < 1 ){
                            # no such a company
                            print(paste("Forgot to register company?:", isin))
                            next
                        }
                        q_str <- paste("INSERT OR REPLACE INTO index_designation (isin, index_id) VALUES (",
                                       paste(paste("\"",isin,"\"",sep=""), 
                                             paste("\"",index_designation,"\"",sep=""),
                                             sep=","),
                                        ")",
                                       sep="")
                        print(q_str)
                        dbSendQuery(conn, q_str)
                    
                    }, error=function(e){print(e)}
                )
            }
        }
    }
    
}

