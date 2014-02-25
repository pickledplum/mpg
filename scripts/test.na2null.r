source("na2null.r")
source("assert.r")

expected = "INSERT OR REPLACE INTO 'P_PRICE_AVG-B3DCPD' ('date','usd','local') VALUES (1,2,NA),(3,4,NA),(5,6,NA),(7, 8,NA)"
actual = "INSERT OR REPLACE INTO 'P_PRICE_AVG-B3DCPD' ('date','usd','local') VALUES (1,2,NA),(3,4,NA),(5,6,NA),(7, 8,NA)"
assert.equal(actual, expected)

expected = "NULL,1,2,NULL,3,NULL"
actual = na2null(paste(c(NA,1,2,NA,3,NA), collapse=","))
assert.equal(actual, expected)