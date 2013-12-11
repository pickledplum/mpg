# Remove leading "x"

source_dir <- "Z:/Backups/MPG-Production/fs-outputs/em"
target_dir <- "D:/home/honda/mpg/emerging/fs_output"

source_names <- list.files(source_dir, pattern=paste(".+.csv$", sep=""), full.names=FALSE)

for( filename in source_names ){
    print(paste("opening", filename, "..."))
    conn <- file(file.path(source_dir, filename), "r")
    tokens <- read.table(conn, sep=",", nrows=1, colClasses="character")
    cnt <- 0
    new_header <- ""
    for( token in tokens ){
        if( length( grep("^[xX][:digit:]+", token) ) > 0 ) {
            new_column_name <- sub("x", "", token)
            new_header <- paste(new_header, new_column_name, sep=",")
            cnt <- cnt + 1
        } else {
            new_header <- paste(new_header, token, ",")
        }
    }
    if( cnt == 0 ){
        # don't need to do anything
        print("No ID had leading x.  Nothing to do...")
        close(conn)
    } else{
        temp <- tempfile()
        temp_conn <- file(temp, "w")
        writeLines(new_header, temp_conn)
        while(TRUE) {
            line <- readLines(conn,1)
            if( length(line) < 1) break
            
            writeLines(line, temp_conn)
        }
        close(temp_conn)
        close(conn)
        if( file.rename( temp, file.path(target_dir, filename ) ) ) {
            print(paste("Wrote", target_dir, "/", filename))
        } else {
            print("Failed to rename")
        }
    }
    
}
