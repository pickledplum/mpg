library(RSQLite)

source("test.assert.r")
source("tryDb.r")

# 
db <- "D:/home/honda/sqlite-db/mini.sqlite"
conn <<- dbConnect( SQLite(), db )
q_str <- "DROP TABLE IF EXISTS dummy"
trySendQuery(conn, q_str)
specs <- c("id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE",
           "val INTEGER")
q_str <- "CREATE TABLE IF NOT EXISTS dummy (id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE, val INTEGER)"
tryCreateIfNotExistsTable(conn, "dummy", specs)
q_str <- "INSERT OR REPLACE INTO dummy (id, val) VALUES (1,30),(2,40),(3,50)"

tryInsert(conn, "dummy", c("id","val"), data.frame(c(1,2,3),c(10,20,30)))

expected <- data.frame(c(2,3), c(20,30))
ret <- trySelect(conn, "dummy", c("id","val"), "val>=20")
assert.equal(ret, expected)

tryInsert(conn, "dummy", c("id","val"), data.frame(c(4,5), c(40,50)))

tryInsertOrUpdate(conn, "dummy", "id", c("id","val"), data.frame(c(4,5,6), c(400,50,60)))

dbDisconnect(conn)