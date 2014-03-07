
timestamps <- c("2010-01-01",
                "2010-03-01",
                "2010-06-01",
                "2010-09-01",
                "2010-12-01",
                "2011-01-01",
                "2011-03-01",
                "2011-06-01",
                "2011-09-01",
                "2011-12-01")

# in per cent
values <- c(10,10,10,10,10,1,10,10,1,1)

data <- data.frame(values)
rownames(data) <- timestamps
returns <- as.xts(data)

ret <- computeVolatility(c("2010-12","2011-12"), returns)
print(ret)
