source("tryDb.r")
source("logger.r")
createCategoryTable <- function(conn){
    ########################################
    # Create CATEGORY table
    ########################################
    category.column.name <- c("category_id", 
                              "category_descript")
    category.column.type <- c("VARCHAR(50) PRIMARY KEY NOT NULL UNIQUE", 
                              "TEXT(50) NOT NULL UNIQUE")
    category.specs <- paste(category.column.name, category.column.type)
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
    tryBulkInsertOrReplace(conn, "category", category.column.name, 
                           data.frame(enQuote(category.category_id), 
                                      enQuote(category.category_descript)))
    logger.info(paste("Created CATEGORY table:", paste(category.specs, collapse=",")))
}
