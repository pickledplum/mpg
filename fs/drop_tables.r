library(RSQLite)
db <- "D:/home/honda/sqlite-db/financial.sqlite3"
conn <- dbConnect( SQLite(), db)

table_list <- dbGetQuery(conn, "SELECT name FROM sqlite_master WHERE type='table'")
table_list <- as.array(table_list[[1]])

apply(table_list, 1, function(x){
    if( x %in% c("company", "country") ){
    return(NULL)
    }
    q_str <- paste("DROP TABLE if exists '", x, "'", sep="")
    print(q_str)
    tryCatch(dbSendQuery(conn, q_str), 
             error=function(e){print(e)}
             )
})


dbCommit(conn)
dbDisconnect(conn)