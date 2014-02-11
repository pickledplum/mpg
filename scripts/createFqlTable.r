source("logger.r")
source("tryDb.r")

createFqlTable <- function(conn, fql_map_filename){
    ########################################
    # Create FQL table
    ########################################
    fql.column.name <- c("fql", 
                         "syntax", 
                         "description", 
                         "unit", 
                         "report_freq",
                         "category_id", 
                         "note")
    fql.column.type <- c("VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
                         "TEXT(200)",
                         "TEXT(100)", 
                         "FLOAT", 
                         "CHAR(1)", 
                         "VARCHAR(50)",
                         "VARCHAR(200)")
    fql.column.constraint <-  c("FOREIGN KEY(category_id) REFERENCES category(category_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
                                "FOREIGN KEY(report_freq) REFERENCES frequency(freq) ON DELETE NO ACTION ON UPDATE CASCADE")
    
    fql.specs <- c(paste(fql.column.name, fql.column.type), fql.column.constraint)
    tryCreateTableIfNotExists(conn, "fql", fql.specs)
    logger.info(paste("Created FQL table:", paste(fql.specs, collapse=",")))

    stopifnot(file.exists(fql_map_filename))
    fql_map <- read.csv(fql_map_filename)
    rownames(fql_map) <- fql_map$fql
    tryBulkInsertOrReplace(conn, "fql", 
                           fql.column.name,
                           data.frame(enQuote2(fql_map$fql),
                                      enQuote2(fql_map$syntax),
                                      enQuote(fql_map$description),
                                      fql_map$unit,
                                      enQuote(fql_map$report_freq),
                                      enQuote(fql_map$category_id),
                                      enQuote(fql_map$note)
                           ))
    
    logger.info(paste("Populated FQL table:", nrow(fql_map)))
}