# Read a config file, which is in the form of named list,
# and return an environement (i.e. hashtable)
config <- read_config( table_file ){
    table <- read.table(table_file, sep="=", comment.char="#", strip.white=TRUE, as.is=TRUE)
    rownames(table) <- table[,1]
    env <- new.env(hash=TRUE)
    for( para in rownames(table) ) {
        assign(para, table[para,2], envir=env)
        
    }
    return(env)
}
