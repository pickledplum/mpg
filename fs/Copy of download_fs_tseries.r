#####################################################################
#
# Driver for get_fs_tseries()
# 
# Download historical data from the FactSet server.
#
# Input file: A text file containg <name>=<value> pairs, where 
# <name> is a FactSet formula that can be called via the 
# "FF.ExtractFormulaHistory" FQL statement.
# <value> is a letter indicating the frequency of data being pulled.
# Valid letters are "D","M","Q","Y" for daily, monthly, quarterly and yearly
# prespectively.
# 
# Output files: A csv for each FactSet formula for each currency.
# Each csv will contain a (n+1) by (m+1) matrix, 
# where n is the number of data entries pulled (e.g. #of days for daily data,
# #of moths for monthly data).
#
# The first row contains the company IDs.
# The first column contains the time stamps.
#
# NOTES:
# For each FS formula per currency, a temporary file is created.
# The temp file stores on-going transaction results in case of
# a disaster.  The user can inspect the contents and may pick
# up from where it is left.  The temp file is created in the
# same directory as the final result files are stored: OUTPUT_PATH.
#
# $Id$
#
#####################################################################

#//////////////////////////////////////////////
# Adjustable parameters
#//////////////////////////////////////////////
DEBUG = FALSE
universe = "LARGE" # SMALL, MEDIUM, LARGE
markets <- c("DM") # "FM", "ACWI", DM", "EM"
# Currencies
currencies = c("USD")#, "local")
binsize <- 100  # use 100-300 for monthly, 25-50 for daily, be conservative

# close log file upon exit 
on.exit(function(log, a_tseries, output_filepath){ 
  close(log)
  write.zoo(a_tseries, output_filepath, sep=",")
  }, add=FALSE)

# query time out.  default is 120 secs.  120 to 3600
FactSet.setConfigurationItem(FactSet.TIMEOUT, 900)
# java heap size
options(java.parameters="-Xmx1000m")

for( market in markets ) {
  # Where the output files and temp files are stored.
  OUTPUT_PATH=""
  # FS portfolio that defines the universe.
  FS_PORTFOLIO_OFDB=""
  # File prefix used for output files
  OUTPUT_FILE_PREFIX=""
  # <name>=<value> list, where <name> is a FS formula and <value> is the data frequency.
  # See the doc on top of this file.
  FS_FUNC_LIST_FILEPATH = ""
  
  if( market == "ACWI" ) {
      OUTPUT_PATH = "Q:/temp/honda/mpg/acwi/fs_output"
      FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_MSCI_ACWI_ETF"
      OUTPUT_FILE_PREFIX = "etf-acwi-"
      t0 <- "19800101"
      # <name>=<value> list, where <name> is a FS formula and <value> is the data frequency.
      # See the doc on top of this file.
      FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/acwi/fs-sorted-params.txt"
  }
  if( universe == "LARGE") {
      if( market == "DM" ){
          OUTPUT_PATH = "Q:/temp/honda/mpg/developed/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_DM_BY_EX"
          OUTPUT_FILE_PREFIX = "ex_dm-"
          t0 <- "19800101"
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/developed/fs-sorted-params.txt"
      } else if( market == "EM" ){
          OUTPUT_PATH = "Q:/temp/honda/mpg/emerging/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_EM_BY_EX"
          OUTPUT_FILE_PREFIX = "ex-em-"
          t0 <- "19900101"
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/emerging/fs-sorted-params.txt"
      } else if( market == "FM") {
          OUTPUT_PATH = "Q:/temp/honda/mpg/frontier/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_FM_BY_EX"  #4636
          OUTPUT_FILE_PREFIX = "ex-fm-"
          t0 <- "19900101"
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/frontier/fs-sorted-params.txt"
      }
  } else if( universe == "MEDIUM" ){
      if( market == "DM" ){
          OUTPUT_PATH = "Q:/temp/honda/mpg/developed/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_MSCI_DEVELOPED_ETF"
          OUTPUT_FILE_PREFIX = "etf_dm-"
          t0 <- "19800101"
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/developed/fs-sorted-params.txt"
      } else if( market == "EM" ){
          OUTPUT_PATH = "Q:/temp/honda/mpg/emerging/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_EM"
          OUTPUT_FILE_PREFIX = "all-em-"
          t0 <- "19900101"
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/emerging/fs-sorted-params.txt"
      } else if( market == "FM") {
          OUTPUT_PATH = "Q:/temp/honda/mpg/frontier/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_FRONTIERS"  
          OUTPUT_FILE_PREFIX = "all-fm-"
          t0 <- "19900101"
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/frontier/fs-sorted-params.txt"
      }
  } else if( universe == "SMALL" ){
      if( market == "DM" ){
          OUTPUT_PATH = "Q:/temp/honda/mpg/developed/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_MSCI_DEVELOPED_ETF"
          OUTPUT_FILE_PREFIX = "etf_dm-"
          t0 <- "19800101"
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/developed/fs-sorted-params.txt"
      } else if( market == "EM" ){
          OUTPUT_PATH = "Q:/temp/honda/mpg/emerging/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_EM_ETF"
          OUTPUT_FILE_PREFIX = "etf-em-"
          t0 <- "19900101"
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/emerging/fs-sorted-params.txt"
      } else if( market == "FM") {
          OUTPUT_PATH = "Q:/temp/honda/mpg/frontier/fs_output"
          FS_PORTFOLIO_OFDB = "PERSONAL:HONDA_ALL_FM_ETF"  
          OUTPUT_FILE_PREFIX = "etf-fm-"
          t0 <- "19900101"  
          FS_FUNC_LIST_FILEPATH <- "Q:/temp/honda/mpg/frontier/fs-sorted-params.txt"
      }
  
  } else {
      print(paste("Invalid univeser size: ", universe))
      return(1)
  }
  # History range.  YYYYMMDD for specific date, 0 for most recent
  t1 = "0"
  
  #//////////////////////////////////////////////
  
  # The actual body of function.  This file must be in the current working dir.
  source("get_fs_tseries.r")
  
  # Load the universe from FS portfolio
  ##### FIXMETODO i uncommented two lines directly below here cause the file 
  ##### wouldn't run since it didn't know what ids was.
  df <- FF.ExtractOFDBUniverse(FS_PORTFOLIO_OFDB, "0O")
  ids <- df$Id
  print("Loaded the universe")
  # Read the FS function list that comes with frequency
  stmt_list <- as.matrix(
      read.table(FS_FUNC_LIST_FILEPATH, sep="=", comment.char="#", strip.white=TRUE, as.is=TRUE),
      ncol=2)
  rownames(stmt_list) <- stmt_list[,1] # fs function names
  stmt_list <- as.list(stmt_list[,2]) # frequency (D,M,Q,Y)
  
  if( DEBUG ) t0 = "20100101"
  if( DEBUG ) ids <- head(ids,3)
  if( DEBUG ) stmt_list <- head(stmt_list,2)
  
  print(paste("Input file:", FS_FUNC_LIST_FILEPATH))
  print(paste("Output path:", OUTPUT_PATH))
  if( !file.exists(OUTPUT_PATH) ) dir.create(OUTPUT_PATH) #create the output dir if not exists.
  print(paste("FactSet portfolio:", FS_PORTFOLIO_OFDB))
  print(paste("Output file prefix:", OUTPUT_FILE_PREFIX))
  print(paste("Time frame:", t0, "-", t1))
  print(paste("Currencies:"))
  str(currencies, give.attr=FALSE, give.length=FALSE, give.head=FALSE)
  print(paste("Num of company IDs:", length(ids)))
  print(paste(cat(head(ids,10)), "..."))
  
  ########################
  # Company Basic Info
  ########################
  comp.info <- FF.ExtractDataSnapshot(ids, "FG_COMPANY_NAME(),P_DCOUNTRY()")
  rownames(comp.info) <- comp.info$Id
  comp.info$Id <- c()
  comp.info$Date <- c()
  info_file <- file.path(OUTPUT_PATH,paste(OUTPUT_FILE_PREFIX, "company-info.txt",sep=""))
  write.csv(comp.info, info_file)
  print(paste("Company basic info (id, name, domicile country) are written in: ", info_file))
  
  ########################
  # Time series
  ########################
  # Iterate over currencies
  for( curr in currencies ) {
  
      # Iterate over FS statements
      for( stmt in labels(stmt_list) ) {
          freq <- stmt_list[stmt]
  
          # breaking down company list into smaller chunks, 
          # or Rstudio chokes up for DM, which has more than 31000 companies.
          # Rstudio can allocate upto 2.36MB for an array (a continuous memory block).
          byby <- 10000 #length(ids) #10000
          for( begin in seq(1, length(ids), by=byby) ) {
              
              end <- min( begin+byby-1, length(ids))
              writeLines("")
              output_filepath <- file.path(OUTPUT_PATH, 
                                         paste(OUTPUT_FILE_PREFIX, 
                                               stmt, 
                                               "-", 
                                               freq, 
                                               "-", 
                                               curr,
                                               "-",
                                               begin,
                                               "-",
                                               end,
                                               ".csv", 
                                               sep=""))         
              # open log file every time for appending an entry
              # so that you can inspect in the meantime.
              log <- file(file.path(OUTPUT_PATH, paste(OUTPUT_FILE_PREFIX, "log.log", sep="")), open="a")
            
              started <- proc.time()

              # Get the results in xts.  This could be big, so keep this in
              # the inner most loop.
              a_tseries <- get_fs_tseries(stmt, ids[begin:end], t0, t1, freq, curr, binsize)
              
              # Save in csv
              write.zoo(a_tseries, output_filepath, sep=",")
              
              finished <- proc.time()
                      
              if( DEBUG ) {
                  writeLines("")
                  print("------- See if the live data structure looks good --------------------")
                  if( length( a_tseries ) > 0 ) {
                      print(tail(a_tseries)) 
                  } else {
                      print("Some jobs failed.  Failed companies are: ")
                      print(a_tseries)
                  }
                  print("--------------------------------------------------------")
              }
              writeLines("")
              print(paste("Output stored in: ", output_filepath, sep=""))
              msg <- paste("Elapsed time for market=<", market, "> param=<", stmt, "> (", curr, "): ", (finished-started)["elapsed"], "s", sep="")
              print(msg)
              writeLines(msg, log)
              # close log file every time so that you can inspect in the meantime.
              close(log)
          }
      }
    }
}
print("Good bye...")

#
# Recover the whole or partial results from the temporary file.
#
# recover_from_tempfile <- function(temp_filepath) {
#     tmp <- read.table(temp_filepath,header=FALSE,sep=",",colClasses=c("character"))
#     colnames(tmp) <- tmp[1,]
#     tmp <- subset(tmp, ID!="ID")
#     rownames(tmp) <- tmp[,1]
#     tmp$ID <- c()
#     tmp <- t(tmp)
#     ncols <- ncol(tmp)
#     nrows <- nrow(tmp)
#     print(paste("matrix",nrows,"x",ncols))
#     tmp <- matrix(as.numeric(tmp),nrow=nrows,ncol=ncols,dimnames=list(rownames(tmp),colnames(tmp)))
#     return(as.xts(tmp))
# }