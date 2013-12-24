library(RSQLite)
db <- "D:/home/honda/sqlite-db/security.sqlite3"
conn <- dbConnect( SQLite(), db)

table_list <- dbGetQuery(conn, "SELECT name FROM sqlite_master WHERE type='table'")
table_list <- table_list[[1]]
l <- grep("-local$", table_list, value=TRUE)
for( tablename in l ){
    q_str <- paste("DROP TABLE if exists '", tablename, "'", sep="")
    print(q_str)
    tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
}
dbCommit(conn)
dbDisconnect(conn)