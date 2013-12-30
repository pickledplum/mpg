library(RSQLite)
db <- "D:/home/honda/sqlite-db/historical.sqlite3"
conn <- dbConnect( SQLite(), db)

table_list <- dbGetQuery(conn, "SELECT name FROM sqlite_master WHERE type='table'")
table_list <- table_list[[1]]
print(table_list)
l <- grep("-Y$", table_list, value=TRUE)
for( tablename in l ){
    q_str <- paste("DROP TABLE if exists '", tablename, "'", sep="")
    print(q_str)
    tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
}
l <- grep("-M$", table_list, value=TRUE)
for( tablename in l ){
    q_str <- paste("DROP TABLE if exists '", tablename, "'", sep="")
    print(q_str)
    tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
}
l <- grep("-D$", table_list, value=TRUE)
for( tablename in l ){
    q_str <- paste("DROP TABLE if exists '", tablename, "'", sep="")
    print(q_str)
    tryCatch(dbSendQuery(conn, q_str), error=function(e){print(e)})
}

dbCommit(conn)

table_list <- dbGetQuery(conn, "SELECT name FROM sqlite_master WHERE type='table'")
table_list <- table_list[[1]]
print(table_list)
dbDisconnect(conn)