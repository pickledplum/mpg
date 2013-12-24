library(RSQLite)


limitByAsOf <- function(conn, d1, d2, company_list, formula, freq, curr, lo=NULL, up=NULL) {

    table_list <- dbGetQuery(conn, "SELECT name FROM sqlite_master WHERE type='table'")
    table_list <- table_list[[1]]
    
    pattern <- "[a-zA-Z_]+"
    struct_matches <- gregexpr(pattern, formula)
    params <- unique(unlist(regmatches(formula, struct_matches)))
    #print(params)
    
    m <- matrix(nrow=length(company_list), ncol=length(params))
    rownames(m) <- company_list
    colnames(m) <- params
    universe <- c(character())
    for( company_id in company_list ){
        expr <- formula
        for( param in params ){
            table_name <- paste(company_id,param,freq,sep="-")
            #print(table_name)
            l <- grep(table_name, table_list)

            if( length(l) < 1 ){
                # no such table
                next
            }
            #browser()
            q_str <- paste("SELECT ", curr, " FROM '", table_name, "' WHERE Date >='", d1, "' AND Date <='", d2, "'", sep="")
            print(q_str)
            data <- dbGetQuery(conn, q_str)
            #print(data)
            m[company_id,param] <- data[[1]]
            expr <- gsub(param, m[company_id, param], expr)
        }

        val <- eval(parse(text=expr))
        print(val)
        passed <- TRUE
        if( length(lo) >= 1 ) {
            if( val < lo ){
                passed <- FALSE
            }
        }
        if( passed && length(up) >= 1 ){
            if( val > up ){
                passed <- FALSE
            }
        }
        if( passed ) {
            universe <- c(universe, company_id)
        }
    }
    print(m)
    return(universe)
}
limitByYearlyAsOf <- function(conn, year, company_list, formula, curr, lo=NULL, up=NULL) {

    return(limitByAsOf(conn, paste(year, "-01-01", sep=""), paste(year, "-12-31", sep=""),
                           company_list, formula, "Y", curr, lo, up))
}
limitByMonthlyAsOf <- function(conn, year, month, company_list, formula, curr, lo=NULL, up=NULL) {
    d1 <- paste(year,month,"01",sep='-')
    q_str <- paste("SELECT date('", d1, "','start of month','+1 month','-1 day')", sep="")
    print(q_str)
    
    tmp <- dbGetQuery(conn, q_str)

    d2 <- unlist(tmp)[1]
    print(d2)
    return(limitByAsOf(conn, d1, d2,
                             company_list, formula, "M", curr, lo, up))
}
db <- "D:/home/honda/sqlite-db/security.sqlite3"
conn <- dbConnect( SQLite(), db)

universe <- c("282410", "2826", "4561")
#universe <- limitByAsOf(conn, "2012-12-31", universe, 
#            "FF_WKCAP*1.0*FF_WKCAP", "Y", "USD", 100000)
#universe <- limitByYearlyAsOf(conn, 2012, universe, 
#                        "FF_WKCAP*1.0*FF_WKCAP", "USD", 1000000)
universe <- limitByMonthlyAsOf(conn, "2012", "02", universe, 
                              "FF_DEBT", "USD", NULL, 25000)
print(universe)

dbCommit(conn)
dbDisconnect(conn)