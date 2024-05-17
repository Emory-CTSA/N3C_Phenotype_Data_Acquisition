
#break up the single SQL file into individual statements and output file names
parse_sql <- function(sqlFile) {
  sql <- ""
  output_file_tag <- "OUTPUT_FILE:"
  inrows <- unlist(strsplit(sqlFile, "\n"))
  statements <- list()
  outputs <- list()
  statementnum <- 0
  output_file <- ""

  for (i in 1:length(inrows)) {
    sql = paste(sql, inrows[i], sep = "\n")
    if (regexpr("OUTPUT_FILE", inrows[i]) != -1) {
      output_file <- sub("--OUTPUT_FILE: ", "", inrows[i])
    }
    if (regexpr(";", inrows[i]) != -1) {
      statementnum <- statementnum + 1
      statements[[statementnum]] = sql
      outputs[[statementnum]] = output_file
      sql <- ""
    }
  }
  arr <- mapply(c, outputs, statements)
  dt <- as.data.table(mapply(c, outputs, statements), colnames=c("outputs","statements"))
}

fileRecent <- function(filePath, daysToCheck) {
  fileDT <- file.mtime(filePath)
  retval <- ifelse(difftime(Sys.time(), fileDT, units = "days") <= daysToCheck,TRUE,FALSE)
  return(retval)
}

identifyDateColumn <- function(sql) {
  locstart <- regexpr('[A-Za-z_]*_DATE', sql)
  # print(locstart)
  fieldname <- sapply(regmatches(sql, locstart, invert=NA),`[`,2)
  return(fieldname)
}

buildDateRanges <- function(dateRangeStart, dateRangeEnd, monthsToIncrement) {
  if (!is.numeric(monthsToIncrement) || monthsToIncrement < 1) {
    return(data.table(periodStart=format(dateRangeStart,'%Y-%m-%d'), periodEnd=format(dateRangeEnd,'%Y-%m-%d'), fnAppend=""))
  } else {
    ds = dateRangeStart
    drEnd = dateRangeEnd
    periodStart = c()
    periodEnd = c()
    fnAppend = c()
    while (ds < drEnd) {
      periodStart = c(periodStart,format(ds,'%Y-%m-%d'))
      de <- ds %m+% months(monthsToIncrement)
      if (de > drEnd) de <- drEnd
      periodEnd = c(periodEnd, format(de, '%Y-%m-%d'))
      fnAppend = c(fnAppend,paste0(format(ds,"%Y%m%d-"),format(de,"%Y%m%d")))
      ds <- de
    }
    return(data.table(periodStart, periodEnd, fnAppend))
  }
}

pause <- function() {
  print("entering debugging mode with browser(). Type c to continue or Q to quit")
  browser()
}

dt.append <- function(x1, x2) {
  obj <- deparse(substitute(x1)) # get the actual object name as a string
  #print(obj)
  # get(obj,pos=1)
  assign(obj, value = data.table::rbindlist(list(x1, x2))) #, envir = .GlobalEnv)
  pause()
  
}

glueLocal <- function(mainDataOutputFolder, table="MEASUREMENT") { # windows specific
  files <- fs::dir_ls(paste0(mainDataOutputFolder,"toglob/"),regexp=paste0(table,"*"))
  append <- FALSE
  for (filename in files) {
    dta <- fread(filename)
    fwrite(dta,paste0(mainDataOutputFolder,table,"_glob.csv"),sep="|",append=append)
    append <- TRUE
  }
}

runExtraction  <- function(connectionDetails,
                           sqlFilePath,
                           cdmDatabaseSchema,
                           resultsDatabaseSchema,
                           outputFolder = paste0(getwd(), "/output/"),
                           useAndromeda = FALSE,
                           checkFileDates = FALSE,
                           ...
                           )
{
  daysToCheck <- 0
  dotdotdots <- names(list(...))
  if (checkFileDates) {
    if ("daysBetweenSubmissions" %in% dotdotdots) {if (!is.na(as.numeric(daysBetweenSubmissions))) daysToCheck <- as.numeric(daysBetweenSubmissions)}
    if ("dataLatencyNumDays" %in% dotdotdots) {if (!is.na(as.numeric(dataLatencyNumDays))) daysToCheck <- daysToCheck - as.numeric(dataLatencyNumDays)} 
    if (daysToCheck < 0) daysToCheck <- 0
  }
  if ("monthsToIncrement" %in% dotdotdots) {ifelse(is.integer(monthsToIncrement), monthsToIncrement, NA)} else {
    monthsToIncrement <- NA
  }
  # workaround to avoid scientific notation
  # save current scipen value
  scipen_val <- getOption("scipen")
  # temporarily change scipen setting (restored at end of f())
  options(scipen=999)

  # create output dir if it doesn't already exist
  if (!file.exists(file.path(outputFolder)))
    dir.create(file.path(outputFolder), recursive = TRUE)

  if (!file.exists(paste0(outputFolder,"DATAFILES/toglob")))
    dir.create(paste0(outputFolder,"DATAFILES/toglob"), recursive = TRUE)

  # load source sql file
  src_sql <- SqlRender::readSql(sqlFilePath)
  
  # replace parameters with values
  src_sql <- SqlRender::render(sql = src_sql,
                               warnOnMissingParameters = FALSE,
                               cdmDatabaseSchema = cdmDatabaseSchema,
                               resultsDatabaseSchema = resultsDatabaseSchema,
                               ...)

  nonDF <- c("MANIFEST.csv","DATA_COUNTS.csv","EXTRACT_VALIDATION.csv","DATA_COUNTS_APPEND.csv")
  # split script into chunks (to produce separate output files)
  allSQL <- parse_sql(src_sql)
  sqlsDT <- data.table::transpose(as.data.table(allSQL))
  setnames(sqlsDT,c("fileName","sql"))
  sqlsDT[,fileName:=gsub(pattern = "\r", x = fileName, replacement = "")]
  sqlsDT[,subFolder:=ifelse(fileName %in% nonDF,"","DATAFILES/")][,outputFolder:=paste0(outputFolder,subFolder)]
  # sqlsDT[,fullPathName:=paste0(outputFolder,"DATAFILES/",fileName)][fileName %in% nonDF,fullPathName:=paste0(outputFolder,fileName)]
  sqlsDT[,fullPathName:=paste0(outputFolder,fileName)]
  sqlsDT[,process:=TRUE][file.exists(fullPathName),process:=!fileRecent(fullPathName,daysToCheck)]
  sqlsDT[,dateColumn:=identifyDateColumn(sql)][,daterangeJoin:=grepl("{daterange_clause}",sql,fixed=TRUE)]
  setindex(sqlsDT,fileName)
  innerJoinDT <- sqlsDT[daterangeJoin==TRUE,.(fileName,sql,dateColumn)][,cj:=TRUE]
  drExpandDT <- buildDateRanges(drStart, drEnd, monthsToIncrement)[,cj:=TRUE]
  cjDT <- innerJoinDT[drExpandDT,allow.cartesian=TRUE, on=c('cj')][,cj:=NULL]
  cjDT[,newFileName:=gsub(pattern="\\.csv",replacement="",fileName,ignore.case=TRUE)][,newFileName:=paste0(newFileName,fnAppend,".csv")]
  cjDT[,drcReplace:=paste0("(",dateColumn," >= '",periodStart,"' AND ",dateColumn," < '",periodEnd,"')")]
  cjDT[,newSql:=mapply(function(x,y) {gsub("{daterange_clause}",y,x,fixed=TRUE)}, sql, drcReplace)]
  cjDT <- cjDT[,.(fileName,newFileName,newSql)]
  setkey(sqlsDT,fileName)
  setkey(cjDT,fileName)
  mergedDT <- cjDT[sqlsDT,,nomatch=NA]
  mergedDT[!is.na(newSql),sql:=newSql]
  mergedDT[!is.na(newFileName),outputFolder:=paste0(outputFolder,"toglob/")]
  mergedDT[is.na(newFileName),newFileName:=fileName)]
  # check if each sub-file for e.g. Measurement exists
  mergedDT[,fullPathName:=paste0(outputFolder,newFileName)][file.exists(fullPathName),process:=!fileRecent(fullPathName,daysToCheck)]
  processDT <- mergedDT[process==TRUE,]
  print(processDT)
  pause()
      # establish database connection
  conn <- DatabaseConnector::connect(connectionDetails)

  for (i in seq(1,processDT[,.N])) { # go line-by-line
    # oneRow <- as.list(mergedDT[i,.(sql,newFileName,outputFolder)])
    print(paste("Begin processing file",processDT[i,newFileName]))
    # browser()
    iterresult <- tryCatch({
      if(useAndromeda && processDT[i,newFileName] != "EXTRACT_VALIDATION.csv"){
        executeChunkAndromeda(conn = conn,
                              sql = processDT[i,sql],
                              fileName = processDT[i,newFileName],
                              outputFolder = processDT[i,outputFolder])
      } else {
        # num_result_rows <- executeChunk(conn = conn,
        processDT[i,numResults:=executeChunk(conn = conn,
                                        sql = processDT[i,sql],
                                        fileName = processDT[i,newFileName],
                                        outputFolder = processDT[i,outputFolder])]
        # throw error if dup PKs found
        if(processDT[i,newFileName] == 'EXTRACT_VALIDATION.csv' && processDT[i,numResults] > 0){
          stop("Duplicate primary keys. See EXTRACT_VALIDATION.csv")
        }
      }
    }, error = function(cond) { 
      print(paste("Error encountered:",cond))
      # stop() # don't quit - just go to next file, unless dup primary keys
    }, warning = function(cond) {
      print(paste("Warning only:",cond))
    }, finally = {
      print(paste("Finished processing file",processDT[i,newFileName]))
    })
  }
  # Disconnect from database
  DatabaseConnector::disconnect(conn)

  # restore original scipen value
  options(scipen=scipen_val)

}

#iterate through query list
# for (i in seq(from = 1, to = length(allSQL), by = 2)) {
#   fileNm <- allSQL[i]
# 
#   # check for and remove return from file name
#   fileNm <- gsub(pattern = "\r", x = fileNm, replacement = "")
# 
#   sql <- allSQL[i+1]
# 
#   # TODO: replace this hacky approach to writing these two tables to the root output folder
#   output_path <- outputFolder
#   if(fileNm != "MANIFEST.csv" && fileNm != "DATA_COUNTS.csv" && fileNm != "EXTRACT_VALIDATION.csv" && fileNm != "DATA_COUNTS_APPEND.csv"){
#     output_path <- paste0(outputFolder, "DATAFILES/")
#   }
#   pathFile = paste0(output_path, fileNm)
#   if(file.exists(pathFile)) {
#     fileDT <- file.mtime(pathFile)
#     if(difftime(Sys.time(), fileDT, units = "days") <= daysToCheck) next # have we generated this file lately? if so, skip
#   }
#   iterperiods <- data.table(1)[,V1:=NULL][.0]
#   if (any(grepl("{daterange_clause}",sql))) {
#     fnames <- list()
#     element <- list()
#     if (is.na(monthsToIncrement)) {
#       fnames <- c(fileNm)
#       element <- c("(1 == 1)")
#     } else {
#       ds <- drStart
#       locstart <- regexpr('[A-Za-z_]*_DATE', sql)
#       if (locstart[1] > -1) {
#         fieldname <- regmatches(sql, locstart)
#       while (ds < drEnd) {
#         de <- ds %m+% months(monthsToIncrement)
#         if (de > drEnd) de <- drEnd
#         fn <- paste0(format(ds,"%Y%m%d-"),format(de,"%Y%m%d_"),fileNm) 
#         tempstr <- paste0("(",fieldname," >= '",format(ds,"%Y-%m-%d"),"' AND ",fieldname," < '",format(de,"%Y-%m-%d"),"')")
#         fnames <- c(fnames, fn)
#         element <- c(element, tempstr)
#         ds <- de
#       }
#     } else {
#       fnames <- c(fileNm)
#       element <- c("(1 == 1)")
#     }
#   }
#   iterperiods <- data.table(fileName=fnames, clause=element)
# }
# if (length(iterperiods)>0) {
#   modSQL <- gsub("{daterange_clause}",iterperiods[,clause],sql)
# } else modSQL <- sql



executeChunk <- function(conn,
                         sql,
                         fileName,
                         outputFolder){



  result <- DatabaseConnector::querySql(conn, sql)

  # workaround to append row counts from optional tables (VISIT_DETAIL, NOTE, NOTE_NLP) to DATA_COUNTS.csv on separate executions
  if(fileName == "DATA_COUNTS_APPEND.csv"){
    write.table(result, file = paste0(outputFolder, "DATA_COUNTS.csv" ), sep = "|", row.names = FALSE, na="", append = TRUE, col.names = FALSE)
    return(nrow(result))
  }

  # everything but appends to DATA_COUNTS table
  else{
    write.table(result, file = paste0(outputFolder, fileName ), sep = "|", row.names = FALSE, na="")
    return(nrow(result))
  }



}



executeChunkAndromeda <- function(conn,
                                 sql,
                                 fileName,
                                 outputFolder){



  andr <- Andromeda::andromeda()
  DatabaseConnector::querySqlToAndromeda(connection = conn
                                         ,sql = sql
                                         ,andromeda = andr
                                         ,andromedaTableName = "tmp")

  write.table(andr$tmp, file = paste0(outputFolder, fileName ), sep = "|", row.names = FALSE, na="")

  Andromeda::close(andr)


}


