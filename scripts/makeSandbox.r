source("julianday.r")
source("tryDb.r")
source("enQuote.r")

conn <- dbConnect(SQLite(), "R:/temp/honda/sqlite-db/sandbox")

monthly <- julianday(as.Date(
    c("2010-01-01",
          "2010-02-01",
          "2010-03-01",
          "2010-04-01",
          "2010-05-01",
          "2010-06-01",
          "2010-07-01",
          "2010-08-01",
          "2010-09-01",
          "2010-10-01",
          "2010-11-01",
          "2010-12-01",
          "2011-01-01")
))
daily <- julianday(as.Date(
    c("2010-01-01","2010-01-15",        
           "2010-02-01", "2010-02-15",
           "2010-03-01", "2010-03-15",
           "2010-04-01", "2010-04-15",
           "2010-05-01", "2010-05-15",
           "2010-06-01","2010-06-15",
           "2010-07-01","2010-07-15",
             "2010-08-01", "2010-08-15",
             "2010-09-01", "2010-09-15",
             "2010-10-01", "2010-10-15",
             "2010-11-01","2010-11-15",
             "2010-12-01", "2010-12-15",
             "2011-01-01","2011-01-15s")
))

ones <- rep(1,length(monthly))



ndays <- length(daily)
nmonths <- length(monthly)
tryBulkInsertOrReplace(conn, 
                       "bulk_P_TOTAL_RETURNC", 
                       c("date", "AMAZON", "GOOG", "IBM"), 
                       data.frame(enQuote(daily),
                                  rep(1, ndays),
                                  as.integer(seq(0,ndays-1, 1)),
                                  rep(-1,ndays))
)

tryBulkInsertOrReplace(conn, 
                       "bulk_FF_BPS", 
                       c("date","AMAZON","GOOG","IBM"), 
                       data.frame(enQuote(monthly),
                                  ones*3,
                                  ones*2,
                                  ones*1))

tryBulkInsertOrReplace(conn, 
                       "bulk_P_PRICE_AVG", 
                       c("date","AMAZON","GOOG","IBM"), 
                       data.frame(enQuote(monthly),
                                  as.integer(seq(nmonths-1,0, -1)),
                                  ones,
                                  as.integer(seq(0, nmonths-1, 1))))
          

dbDisconnect(conn)