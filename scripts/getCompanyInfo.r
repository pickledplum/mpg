
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
#' 1. factset_id : FactSet ID
#' 2. company_name: Company name
#' 3. country_id: 2-letter ISO country ID
#' 4. sector: Sector name
#' 5. indgrp: Industry group name
#' 6. industry: Industry name
#' 7. subind: Sub-industry name
#' 
getCompanyInfo <- function(conn, universe) {
    master_data <- NULL
    for( fsid in universe ){
        q_str <- "SELECT * FROM company JOIN country using (country_id) WHERE factset_id='FACTSET_ID'"
        q_str <- gsub("FACTSET_ID", fsid, q_str)
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
