# Remove leading "x"

source_dir <- "Z:/Backups/MPG-Production/fs-outputs/acwi"
target_dir <- "D:/home/honda/mpg/acwi/fs_output"
source_dir_list <- c("Z:/Backups/MPG-Production/fs-outputs/acwi",
                   "Z:/Backups/MPG-Production/fs-outputs/fm",
                   "Z:/Backups/MPG-Production/fs-outputs/em",
                   "Z:/Backups/MPG-Production/fs-outputs/dm")

target_dir_list <- c("D:/home/honda/mpg/acwi/fs_output",
                     "D:/home/honda/mpg/frontier/fs_output",
                     "D:/home/honda/mpg/emerging/fs_output",
                     "D:/home/honda/mpg/developed/fs_output"
)

hash <- new.env(hash=TRUE)
for( i in 1:length(source_dir_list)){
    assign( source_dir_list[i], target_dir_list[i], hash)
}
ls(hash)

for( source_dir in source_dir_list ){
    target_dir <- hash[[source_dir]]

    source_names <- list.files(source_dir, pattern=paste(".csv$", sep=""), full.names=FALSE)
    
    for( filename in source_names ){
        print(paste("opening", filename, "..."))
        conn <- file(file.path(source_dir, filename), "r")
        header <- read.table(conn, sep=",", nrows=1, colClasses="character",strip.white=TRUE)
        tokens <- unlist(header)
        cnt <- 0
        new_header <- ""
        for( token in tokens ){
            #print(paste("token: ", token))
            if( length( grep("^X[0-9]", token) ) > 0 ) {
                #print(paste("--- Gotcha", token))
                new_column_name <- sub("^X", "", token)
                new_header <- paste(new_header, new_column_name, sep=",")
                cnt <- cnt + 1
            } else {
                new_header <- paste(new_header, token, sep=",")
            }
        }
        if( cnt == 0 ){
            # don't need to do anything
            print("----- No ID had leading X.  Nothing to do...")
            file.copy(file.path(source_dir, filename), file.path(target_dir, filename))
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
}