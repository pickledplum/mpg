library(RSQLite)

drop_tables <- function(conn, exclude=c("company", "country")){
    
    table_list <- dbGetQuery(conn, "SELECT name FROM sqlite_master WHERE type='table'")
    table_list <- as.array(table_list[[1]])
    
    apply(table_list, 1, function(x){
        if( x %in% exclude){
        return(NULL)
        }
        q_str <- paste("DROP TABLE IF EXISTS '", x, "'", sep="")
        print(q_str)
        tryCatch(dbSendQuery(conn, q_str), 
                 error=function(e){print(e)}
                 )
    })
}