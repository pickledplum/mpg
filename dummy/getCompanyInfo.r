
tryCatch({
    source("tryDb.r")
    source("assert.r")
    source("is.empty.r")
    source("julianday.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
)
#' Get company info
#'
#' @param universe: list of factset ids
#' @return A data frame containing the table of company info.
#' 
#' <frames>
#' 1. "factset_id"   
#' 2. "company"      
#' 3. "dcountry"     : domicile country
#' 4. "dregion"      : domicile region
#' 5. "dcurr_id"     : local currency
#' 6. "market_id"    : market designation 
#' 7. "sector"       
#' 8. "industry"    
#' 9. "exchange"     
#' 10. "exchange_id"  
#' 11. "excurr_id"   : trading currency
#' 12."excountry_id" : trading country
#' 
getCompanyInfo <- function(conn, universe) {
    templ <- 
        "SELECT factset_id, company,dcountry,dregion,dcurr_id,market_id,sector,industry,exchange,exchange_id,excurr_id,excountry_id
             FROM  
              (SELECT * FROM
                (SELECT country_id AS dcountry_id, country_name AS dcountry, region AS dregion, curr_id AS dcurr_id, market_id
                        FROM country ORDER BY dcountry_id) AS T0          
                 JOIN
                 (SELECT factset_id, company, domicile_country_id AS dcountry_id, sector, industry, subind, exchange_id, isin, sedol
                        FROM company ORDER BY exchange_id) AS T1          
                 USING (dcountry_id)     
               )
              JOIN
              (SELECT exchange_id, exchange_name AS exchange, curr_id AS excurr_id, country_id AS excountry_id
                      FROM exchange ORDER BY exchange_id) AS T2
              USING(exchange_id)
    "
    tmpl <- gsub("\n", "", templ)
    master_data <- NULL
    for( fsid in universe ){
        q_str <- paste(templ, " WHERE factset_id=", enQuote(fsid), " ORDER BY factset_id", sep="")
        data <- tryGetQuery(conn, q_str)
        if( is.null(master_data) ){
            master_data <- data
        } else {
            master_data <- rbind(master_data, data)
        }
    }
    return(master_data)
}
#' Get all the avaiable FQL parameters
#' @param fsid FactSet ID
#' @return A data frame containing table of FQL params and their related info available to the company.
#' 
#' <frames>
#' 1. fql : FQL param
#' 2. syntax: FQL syntax to extract "fql" values
#' 3. description: description
#' 4. unit: unit of values
#' 5. report_freq: FactSet reporting frequency
#' 6. category_id: type of FQL param ("company_fund", "company_meta", "price", "country_fund")
#' 7. note: some notes if any
#' 
getAvailableFQLs <- function(conn, fsid){
    q_str <- "SELECT fql, syntax, description, unit, report_freq, category_id, note FROM catalog JOIN fql USING(fql) WHERE factset_id=FACTSET_ID"
    q_str <- gsub("FACTSET_ID", enQuote(fsid), q_str)
    data <- tryGetQuery(conn, q_str)
    return(data)
}

#' Get the list of all possible industry groups
getIndustryGroups <- function(conn) {
    l <- trySelect(conn, "company", c("indgrp"), is_distinct=TRUE)
    return(l$indgrp)
}
#' Get the list of all possible industries
getIndustories <- function(conn){
    l <- trySelect(conn, "company", c("industry"), is_distinct=TRUE)
    return(l$industry)
}
#' Get the list of all possible sub-industries
getSubIndustries <- function(conn){
    l <- trySelect(conn, "company", c("subind"), is_distinct=TRUE)
    return(l$subind)
}
#' Get the list of all possible sectors
getSectors <- function(conn){
    l <- trySelect(conn, "company", c("sector"), is_distinct=TRUE)
    return(l$sector)
}
