#'
#' Define tables in META db
#'
#'
source("tryDb.r")
source("logger.r")
source("dropTables.r")
source("is.empty.r")
source("tryExtract.r")
#'
#' Create CATALOG table
#'
create.table.catalog <- function(conn) {
    #' Define CATALOG table
    catalog.attrs <- c( "dbname     VARCHAR(30)",
                        "tablename  VARCHAR(41)",
                        "factset_id VARCHAR(20) NOT NULL",
                        "fql        VARCHAR(20) NOT NULL",
                        "usd        BOOL",
                        "local      BOOL",
                        "earliest   INTEGER",
                        "latest     INTEGER")
    
    catalog.constraints <- c("PRIMARY KEY(factset_id, fql)",
                             "FOREIGN KEY(factset_id) REFERENCES company(factset_id) ON DELETE SET DEFAULT ON UPDATE CASCADE", 
                             "FOREIGN KEY(fql) REFERENCES fql(fql) ON DELETE SET DEFAULT ON UPDATE CASCADE")
    
    catalog.specs <- c(catalog.attrs, catalog.constraints)
    tryCreateTable(conn, "catalog", catalog.specs)
    logger.info(paste("Created CATALOG table:", paste(catalog.specs, collapse=",")))
}
#'
#'Create & fill CATEGORY table
#'
create.table.category <- function(conn){

    category.attrs <- c("category_id          VARCHAR(50) PRIMARY KEY NOT NULL UNIQUE", 
                        "category_description TEXT(50)")
    category.specs <- paste(category.attrs)
    tryCreateTableIfNotExists(conn, "category", category.specs)
    logger.info("Created CATEGORY table")

    category.category_id <- c("company_fund", 
                              "price", 
                              "company_info", 
                              "country_fund")
    category.category_descript <- c("company fundamental",
                                    "price",
                                    "company info",
                                    "country fundamental")
    tryBulkInsertOrReplace(conn, 
                           "category", 
                           c("category_id", "category_description"), 
                           data.frame(enQuote(category.category_id), 
                                      enQuote(category.category_descript)))
    
    logger.info(paste("Created & filled CATEGORY table:", paste(category.specs, collapse=",")))
}
#'
#' Create COMPANY table
#' 
create.table.company <- function(conn) {
    company.attrs <- c("factset_id  VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE",
                       "company     TEXT(100)",
                       "sector      TEXT(100)",
                       "industry    TEXT(100)",
                       "subind      TEXT(100)",
                       "country_id  CHAR(3)",
                       "exchange_id VARCHAR(5)",
                       "sedol       VARCHAR(20)",
                       "isin        VARCHAR(20)"
    )
    company.constraints <- c("FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE SET DEFAULT ON UPDATE CASCADE", 
                             "FOREIGN KEY(exchange_id) REFERENCES exchange(exchange_id) ON DELETE SET DEFAULT ON UPDATE CASCADE"
    )
    company.specs <- c(company.attrs, company.constraints)
    tryCreateTable(conn, "company", company.specs)
    logger.info(paste("Created COMPANY table:", paste(company.specs, collapse=",")))
}

#'
#' Create COUNTRY table
#' 
create.table.country <- function(conn) {
    country.attrs <- c("country_id  CHAR(3)  PRIMARY KEY NOT NULL UNIQUE",
                       "country     TEXT(100)",
                       "region      TEXT(100)",
                       "curr_id     CHAR(3)",
                       "market_id   VARCHAR(5)"
    )
    country.constraints <- c("FOREIGN KEY(curr_id) REFERENCES currency(curr_id) ON DELETE SET DEFAULT ON UPDATE CASCADE", 
                             "FOREIGN KEY(market_id) REFERENCES market(market_id) ON DELETE SET DEFAULT ON UPDATE CASCADE"
    )
    country.specs <- c(country.attrs, country.constraints)
    tryCreateTable(conn, "country", country.specs)
    logger.info(paste("Created COUNTRY table:", paste(country.specs, collapse=",")))
}
create.table.currency <- function(conn){
    currency.attrs <- c("curr_id CHAR(3) PRIMARY KEY NOT NULL UNIQUE",
                        "currency TEXT(50)"
    )
    currency.specs <- currency.attrs
    tryCreateTable(conn, "currency", currency.specs)
    logger.info(paste("Created CURRENCY table:", paste(currency.specs, collapse=",")))
}
create.table.exchange <- function(conn){
    exchange.attrs <- c("exchange_id VARCHAR(5) PRIMARY KEY NOT NULL UNIQUE",
                        "exchange               TEXT(100)",
                        "country_id             CHAR(3)",
                        "curr_id                CHAR(3)"
    )
    exchange.constraints <- c("FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE SET DEFAULT ON UPDATE CASCADE",
                              "FOREIGN KEY(curr_id) REFERENCES currency(curr_id) ON DELETE SET DEFAULT ON UPDATE CASCADE"
    )
    exchange.specs <- c(exchange.attrs, exchange.constraints)
    tryCreateTable(conn, "exchange", exchange.specs)
    logger.info(paste("Created EXCHANGE table:", paste(exchange.specs, collapse=",")))
}
create.table.frequency <- function(conn) {
    frequency.attrs <- c("freq VARCHAR(1)  PRIMARY KEY NOT NULL UNIQUE", 
                         "freq_description VARCHAR(20)"
    )
    
    frequency.specs <- frequency.attrs
    
    tryCreateTableIfNotExists(conn, "frequency", frequency.specs)
    logger.info("Created FREQUENCY table")
    
    tryBulkInsertOrReplace(conn, "frequency", 
                           c("freq", "freq_description"), 
                           data.frame(enQuote(c("Y","S","Q","M","D")),
                                      enQuote(c("Anuual","Semiannual","Quarterly","Monthly","Daily"))) )
    logger.info(paste("Created & filled FREQUENCY table:", paste(frequency.specs, collapse=",")))
}
create.table.market <- function(conn) {
    market.attrs <- c("market_id VARCHAR(5) PRIMARY KEY NOT NULL UNIQUE", 
                      "market               TEXT(50)"
    )
    
    market.specs <- market.attrs
    
    tryCreateTableIfNotExists(conn, "market", market.specs)
    logger.info("Created market table")
    
    tryBulkInsertOrReplace(conn, "market", 
                           c("market_id", "market"), 
                           data.frame(enQuote(c("DM","EM","FM","WORLD")),
                                      enQuote(c("developed","emerging","frontier","world"))) )
    logger.info(paste("Created & filled MARKET table:", paste(market.specs, collapse=",")))
}
create.table.fql <- function(conn, fql_map_csv){

    ########################################
    # Create FQL table
    ########################################
    fql.column.name <- c("fql", 
                         "syntax", 
                         "description", 
                         "unit", 
                         "freq",
                         "category_id", 
                         "note")
    fql.column.type <- c("VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
                         "TEXT(200)",
                         "TEXT(100)", 
                         "FLOAT", 
                         "CHAR(1)", 
                         "VARCHAR(50)",
                         "VARCHAR(200)")
    fql.column.constraint <-  c("FOREIGN KEY(category_id) REFERENCES category(category_id) ON DELETE SET DEFAULT ON UPDATE CASCADE", 
                                "FOREIGN KEY(freq) REFERENCES frequency(freq) ON DELETE SET DEFAULT ON UPDATE CASCADE")
    
    fql.specs <- c(paste(fql.column.name, fql.column.type), fql.column.constraint)
    tryCreateTableIfNotExists(conn, "fql", fql.specs)
    logger.info(paste("Created FQL table:", paste(fql.specs, collapse=",")))
    
    stopifnot(file.exists(fql_map_csv))
    fql_map <- read.csv(fql_map_csv)
    rownames(fql_map) <- fql_map$fql
    tryBulkInsertOrReplace(conn, "fql", 
                           fql.column.name,
                           data.frame(enQuote2(fql_map$fql),
                                      enQuote2(fql_map$syntax),
                                      enQuote(fql_map$description),
                                      fql_map$unit,
                                      enQuote(fql_map$freq),
                                      enQuote(fql_map$category_id),
                                      enQuote(fql_map$note)
                           ))
    
    logger.info(paste("Populated FQL table:", nrow(fql_map)))
}

fill.table.country <- function(target.conn, country_code_table) {
    
    country_code_table[[1]] <- enQuote(country_code_table[[1]])
    country_code_table[[2]] <- enQuote(country_code_table[[2]])
    country_code_table[[3]] <- enQuote(country_code_table[[3]])
    tryBulkInsertOrReplace(target.conn, "country", c("country_id", "country", "market_id"), country_code_table[c(1,2,3)], 1)
    
}
fill.table.currency <- function(target.conn, currency_code_table) {
    currency_code_table[[1]] <- enQuote(currency_code_table[[1]])
    currency_code_table[[2]] <- enQuote(currency_code_table[[2]])
    tryBulkInsertOrReplace(target.conn, "currency", c("curr_id", "currency"), currency_code_table[c(1,2)], 1)
    
}

merge.table.catalog <- function(target.conn, source.conn) {
   # simply tranfer from source to target
    columns <- c("dbname",
                 "tablename",
                 "factset_id",
                 "fql",
                 "usd",
                 "local",
                 "earliest",
                 "latest"
    )
    
    cursor <- trySendQuery(source.conn, paste("SELECT", paste(columns, collapse=","), "FROM catalog"))
    BINSIZE <- 100
    while( TRUE ){
        data <- fetch(cursor,BINSIZE)
        if( is.empty(data) ) {
            break
        }
        for( j in seq(1, ncol(data)) ) {
            data[[j]] <- enQuote(data[[j]])
        }
        tryBulkInsertOrReplace(target.conn, "catalog", columns, data)
    }
    dbClearResult(cursor)
}
#' Download company, regional, industry, exchange, etc. data
#' and create a new table in the target db.
#' @return The name of the temporary table in the target db.
download.company.info <- function(target.conn, source.conn) {
    
    tablename <- "temp"
    # simply tranfer from source to target
    source.columns <- c("factset_id",
                        "company_name"
    )
    target.columns <- c("factset_id",
                        "company",
                        "sector",
                        "industry",
                        "subind",
                        "country_id",
                        "exchange_id",
                        "sedol",
                        "isin"
    )
    cursor <- trySendQuery(source.conn, paste("SELECT", paste(source.columns, collapse=","), "FROM company"))
    BINSIZE = 100
    while( TRUE ){
        company_data <- fetch(cursor,BINSIZE)
        if( is.empty(company_data) ) {
            break
        }

        extra_master <- NULL
        items <- c(
                "FG_COMPANY_NAME",                   #1
                "FF_MAJOR_IND_NAME",                 #2
                "FF_MAJOR_IND",                      #3
                "FF_MAJOR_SUBIND_NAME",              #4
                "FF_MAJOR_SUBIND",                   #5
                "FG_FACTSET_SECTOR",                 #6
                "FG_FACTSET_SECTORN",                #7
                "P_EXCOUNTRY(NAME)",                 #8
                "P_EXCOUNTRY(ISO3)",                 #9
                "P_EXCOUNTRY(REG)",                  #10
                "P_CURRENCY(NAME)", # ex curr        #11
                "P_CURRENCY(ISO)",  # ex curr code   #12
                "P_DCOUNTRY(NAME)",                  #13
                "P_DCOUNTRY(ISO3)",                  #14
                "P_DCOUNTRY(REG)",                   #15
                "FF_CURN_LOC", # local curr          #16
                "FF_CURN_ISO", # local curr code     #17
                "P_EXCHANGE(NAME,FACTSET)",          #18
                "P_EXCHANGE(CODE,FACTSET)",          #19
                "FE_COMPANY_INFO(SEDOL)",            #20
                "FE_COMPANY_INFO(ISIN)"              #21
            )
        extra <- trySnapshot(company_data[[1]], items)
        dbWriteTable(target.conn, tablename, extra, append=TRUE)
        
    }
    dbClearResult(cursor)
    return(tablename)
}
merge.table.company <- function(target.conn, source.table) {
    
    items <- c("Id AS factset_id",
      "fg_company_name AS company",
      "ff_major_ind_name AS industry",
      "ff_major_subind_name AS subind",
      "fg_factset_sector AS sector",
      "p_dcountry_1 AS country_id",
      "p_exchange_1 AS exchange_id",
      "fe_company_info AS sedol",
      "fe_company_info_1 AS isin"
    )
    data <- tryGetQuery(target.conn, paste("SELECT DISTINCT", paste(items, collapse=","), "FROM", source.table))
    for(i in seq(1, ncol(data))){
        data[[i]] <- enQuote(data[[i]])
    }
    BINSIZE <- 100
    for( begin in seq(1, nrow(data), BINSIZE) ) {
        end <- min(begin+BINSIZE-1, nrow(data))
        
        tryBulkInsertOrReplace(target.conn,
                      "company2",
                      c("factset_id",
                        "company",
                        "industry",
                        "subind",
                        "sector",
                        "country_id",
                        "exchange_id",
                        "sedol",
                        "isin"
                      ),
                      data[c(seq(begin, end)),]
        )
    }
}

fill.table.exchange <- function(target.conn, source.table){
    items <- c("p_exchange_1 AS exchange_id",
                "p_exchange AS exchange",
                "p_excountry_1 AS country_id",
                "p_currency_1 AS curr_id"
    )

    ret <- tryGetQuery(target.conn, paste("SELECT DISTINCT", paste(items, collapse=","), "FROM", enQuote(source.table), "GROUP BY exchange_id"))
    ret[[1]] <- enQuote(ret[[1]])
    ret[[2]] <- enQuote(ret[[2]])
    ret[[3]] <- enQuote(ret[[3]])
    ret[[4]] <- enQuote(ret[[4]])
    tryBulkInsert(target.conn, 
                  "exchange", 
                  c("exchange_id", "exchange", "country_id", "curr_id"),
                  ret)

}
update.table.country <- function(target.conn, source.table) {
    items <- c("p_dcountry_1 AS country_id",
               "p_dcountry_2 AS region",
               "ff_curn_iso AS curr_id"
    )
    data <- tryGetQuery(target.conn, paste("SELECT DISTINCT", paste(items, collapse=","), "FROM", source.table))

    for( i in seq(1, nrow(data)) ){
        entry <- data[i,]
        country_id <- entry[1]
        region <- entry[2]
        curr_id <- entry[3]
        q_str <- "UPDATE country SET region=<REGION>, curr_id=<CURR_ID> WHERE country_id=<COUNTRY_ID>"
        q_str <- gsub("<REGION>", enQuote(region), q_str)
        q_str <- gsub("<CURR_ID>", enQuote(curr_id), q_str)
        q_str <- gsub("<COUNTRY_ID>", enQuote(country_id), q_str)
        trySendQuery(target.conn, q_str)
    }
    
    items <- c("p_excountry_1 AS country_id",
               "p_excountry_2 AS region",
               "p_currency_1 AS curr_id"
    )
    data <- tryGetQuery(target.conn, paste("SELECT DISTINCT", paste(items, collapse=","), "FROM", source.table))
    
        for( i in seq(1, nrow(data)) ){
            entry <- data[i,]
            country_id <- entry[1]
            region <- entry[2]
            curr_id <- entry[3]
            q_str <- "UPDATE country SET region=<REGION>, curr_id=<CURR_ID> WHERE country_id=<COUNTRY_ID>"
            q_str <- gsub("<REGION>", enQuote(region), q_str)
            q_str <- gsub("<CURR_ID>", enQuote(curr_id), q_str)
            q_str <- gsub("<COUNTRY_ID>", enQuote(country_id), q_str)
            trySendQuery(target.conn, q_str)
        }
}

target.dbdir <- "/home/honda/sqlite-db/new"
target.dbname <- "meta.sqlite"
source.dbdir <- "/home/honda/sqlite-db/developed"
source.dbname <- "developed.sqlite"
fql.map.file <- "/home/honda/mpg/fql_map.csv"

if( !file.exists(target.dbdir) ){
    dir.create(target.dbdir, recursive=TRUE)
}
#####################################
# Open Logger
#####################################
logger.init(level=logger.DEBUG,
            do_stdout=TRUE)

target.conn <- dbConnect(SQLite(), file.path(target.dbdir, target.dbname))
source.conn <- dbConnect(SQLite(), file.path(source.dbdir, source.dbname))

temp_tablename <- "temp"
if(FALSE){
    dropTables(target.conn)
    
    create.table.catalog(target.conn)
    create.table.category(target.conn) # filled
    create.table.company(target.conn)
    create.table.country(target.conn)
    create.table.frequency(target.conn)  # filled
    create.table.fql(target.conn, fql_map_csv=fql.map.file) #filled
    create.table.currency(target.conn)
    create.table.market(target.conn) # filled
    create.table.exchange(target.conn)
    
    curr_table <- read.csv("/home/honda/mpg/curr_code.csv")
    fill.table.currency(target.conn, curr_table)

    country_table <- read.csv("/home/honda/mpg/country_code.csv")
    fill.table.country(target.conn, country_table)

    merge.table.catalog(target.conn, source.conn)
    
    # temp table containing bulk company info
    temp_tablename <- download.company.info(target.conn, source.conn)
    fill.table.exchange(target.conn, temp_tablename)
    update.table.country(target.conn, temp_tablename)
}
if(TRUE){
    
    
    merge.table.company(target.conn, temp_tablename)

    
}

dbDisconnect(target.conn)
dbDisconnect(source.conn)

