library(RSQLite)

drop_tables <- function(conn, exclude=c()){
    
    table_list <- dbGetQuery(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name<>'sqlite_sequence'")
    if( is.empty(table_list ) ){
        return
    }
    table_list <- as.array(table_list[[1]])
    
    apply(table_list, 1, function(x){
        if( x %in% exclude){
            return(NULL)
        }
        if( is.empty(x) ){
            return(NULL)
        }
        q_str <- paste("DROP TABLE IF EXISTS '", x, "'", sep="")
        print(q_str)
        tryCatch(dbSendQuery(conn, q_str), 
                 error=function(e){print(e)}
                 )
    })
}