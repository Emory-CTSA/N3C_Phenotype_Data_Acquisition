#break up the single SQL file into individual statements and output file names
parse_sql <- function (sqlFile) { # replacement for N3C's default parse_sql to allow for function definitions and blocks containing semicolons
  sql <- ""
  output_file_tag <- "OUTPUT_FILE:"
  inrows <- unlist(strsplit(sqlFile, "\n"))
  statements <- list()
  outputs <- list()
  statementnum <- 0
  output_file <- ""
  opendoubledollar = FALSE
  # best practice would be to verify that $$ is not within a quote or a comment block, etc
  # some sql dialects don't use $$, but use DECLARE and/or BEGIN followed by END to mark the block
  # also a $$ can typically be labelled, but we are only supporting the basics and not allowing that syntax
  for (i in 1:length(inrows)) {
    sql = paste(sql, inrows[i], sep = "\n")
    ddpos <- regexpr("$$", inrows[i], fixed=TRUE)
    if (!opendoubledollar) {
      if (ddpos > -1) {
        opendoubledollar <- TRUE # (!opendoubledollar)
        next # start $$ block; other tokens on this line don't matter
      }
      if (regexpr("OUTPUT_FILE", inrows[i]) != -1) {
        output_file <- sub("--OUTPUT_FILE: ", "", inrows[i])
      }
    } else {
      if (ddpos > -1) { # look for closing $$
        opendoubledollar <- FALSE # (!opendoubledollar)
      } else {
        next # still an open $$ block
      }
    }
    scposarr <- unlist(gregexpr(";", inrows[i]))
    scpos <- scposarr[length(scposarr)]
    if ((scpos > ddpos) | (i==length(inrows))) { # check if the last semicolon is after the closing $$
      statementnum <- statementnum + 1
      statements[[statementnum]] = sql
      outputs[[statementnum]] = output_file
      sql <- ""
    }
  }
  arr <- mapply(c, outputs, statements)
  dt <- as.data.table(mapply(c, outputs, statements), colnames = c("outputs", "statements"))
}

is.wholenumber <- function(x, tol = .Machine$double.eps^0.5) { return(abs(x - round(x)) < tol) }

.createErrorReport <- function(dbms, message, sql, fileName, prependDate=FALSE, mode="w") { # added function parameters vs the function provided in DatabaseConnector
  report <- c("DBMS:\n", dbms, "\n\nError:\n", message, "\n\nSQL:\n", sql, "\n\n", .systemInfo())
  if (prependDate) {
    fn <- paste0(format(Sys.Date(),'%Y%m%d'),'_',fileName)
  }  else {
    fn <- fileName
  }
  fileConn <- file(fn, open=mode)
  writeChar(report, fileConn, eos = NULL)
  close(fileConn)
  abort(paste("Error executing SQL:",
              message,
              paste("An error report has been created at", fn),
              sep = "\n"
  ), call. = FALSE)
}

executeSql <- function(connection, # replaces fn (added function parameters) from DatabaseConnector
                       sql,
                       profile = FALSE,
                       progressBar = !as.logical(Sys.getenv("TESTTHAT", unset = FALSE)),
                       reportOverallTime = TRUE,
                       errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                       runAsBatch = FALSE,
                       noSplit = FALSE) {
  library("DatabaseConnector")
  if (inherits(connection, "DatabaseConnectorJdbcConnection") && rJava::is.jnull(connection@jConnection)) {
    abort("Connection is closed")
  }
  
  startTime <- Sys.time()
  dbms <- dbms(connection)
  
  if (inherits(connection, "DatabaseConnectorJdbcConnection") &&
      dbms == "redshift" &&
      rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")) {
    # Turn off autocommit for RedShift to avoid this issue:
    # https://github.com/OHDSI/DatabaseConnector/issues/90
    DatabaseConnector:::trySettingAutoCommit(connection, FALSE)
    on.exit(DatabaseConnector:::trySettingAutoCommit(connection, TRUE))
  }
  
  batched <- runAsBatch && supportsBatchUpdates(connection)
  if (noSplit) { 
    sqlStatements <- sql 
  } else {
    sqlStatements <- SqlRender::splitSql(sql)
  }
  rowsAffected <- c()
  if (batched) {
    batchSize <- 1000
    for (start in seq(1, length(sqlStatements), by = batchSize)) {
      end <- min(start + batchSize - 1, length(sqlStatements))
      
      statement <- rJava::.jcall(connection@jConnection, "Ljava/sql/Statement;", "createStatement")
      batchSql <- c()
      for (i in start:end) {
        sqlStatement <- sqlStatements[i]
        batchSql <- c(batchSql, sqlStatement)
        rJava::.jcall(statement, "V", "addBatch", as.character(sqlStatement), check = FALSE)
      }
      if (profile) {
        SqlRender::writeSql(paste(batchSql, collapse = "\n\n"), sprintf("statements_%s_%s.sql", start, end))
      }
      DatabaseConnector:::logTrace(paste("Executing SQL:", truncateSql(batchSql)))
      tryCatch(
        {
          startQuery <- Sys.time()
          rowsAffected <- c(rowsAffected, rJava::.jcall(statement, "[J", "executeLargeBatch"))
          delta <- Sys.time() - startQuery
          if (profile) {
            rlang::inform(paste("Statements", start, "through", end, "took", delta, attr(delta, "units")))
          }
          DatabaseConnector:::logTrace(paste("Statements took", delta, attr(delta, "units")))
        },
        error = function(err) {
          .createErrorReport(dbms, err$message, paste(batchSql, collapse = "\n\n"), errorReportFile, prependDate=TRUE, mode="a")
        },
        finally = {
          rJava::.jcall(statement, "V", "close")
        }
      )
    }
  } else {
    if (progressBar) {
      pb <- txtProgressBar(style = 3)
    }
    
    for (i in 1:length(sqlStatements)) {
      sqlStatement <- sqlStatements[i]
      if (profile) {
        fileConn <- file(paste("statement_", i, ".sql", sep = ""))
        writeChar(sqlStatement, fileConn, eos = NULL)
        close(fileConn)
      }
      tryCatch(
        {
          startQuery <- Sys.time()
          rowsAffected <- c(rowsAffected, lowLevelExecuteSql(connection, sqlStatement))
          delta <- Sys.time() - startQuery
          if (profile) {
            rlang::inform(paste("Statement ", i, "took", delta, attr(delta, "units")))
          }
        },
        error = function(err) {
          DatabaseConnector:::.createErrorReport(dbms, err$message, sqlStatement, errorReportFile, prependDate=TRUE, mode="a")
        }
      )
      if (progressBar) {
        setTxtProgressBar(pb, i / length(sqlStatements))
      }
    }
    if (progressBar) {
      close(pb)
    }
  }
  # Spark throws error 'Cannot use commit while Connection is in auto-commit mode.'. However, also throws error when trying to set autocommit on or off:
  if (dbms != "spark" && inherits(connection, "DatabaseConnectorJdbcConnection") && !rJava::.jcall(connection@jConnection, "Z", "getAutoCommit")) {
    rJava::.jcall(connection@jConnection, "V", "commit")
  }
  
  if (reportOverallTime) {
    delta <- Sys.time() - startTime
    rlang::inform(paste("Executing SQL took", signif(delta, 3), attr(delta, "units")))
  }
  invisible(rowsAffected)
}

create_crosswalks <- function(
    connectionDetails,
    sqlFilePath,
    cdmDatabaseSchema = "omop",
    resultsDatabaseSchema = "n3c",
    cohortTable = 'CLAD_COHORT',
    hash_fn = 'FNV_HASH',
    overlap_fn = 'OVERLAPS',
    startDate = '1970-01-01', 
    endDate = '2099-12-31',
    ...
) {
  # load source sql file
  src_sql <- SqlRender::readSql(sqlFilePath)
  
  # replace parameters with values
  src_sql <- SqlRender::render(
    sql = src_sql,
    warnOnMissingParameters = FALSE,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema,
    cohortTable = cohortTable,
    hash_fn = hash_fn,
    overlap_fn = overlap_fn,
    startDate = startDate,
    endDate = endDate,
    ...
  )
  nonDF <- c("MANIFEST.csv","DATA_COUNTS.csv","EXTRACT_VALIDATION.csv","DATA_COUNTS_APPEND.csv")
  # split script into chunks (to produce separate output files)
  allSQL <- parse_sql(src_sql)
  processDT <- data.table::transpose(data.table::as.data.table(allSQL))
  setnames(processDT,c("fileName","sql"))
  nfiles <- processDT[,.N]
  if (nfiles > 0) {
    conn <- DatabaseConnector::connect(connectionDetails)
    # setDF(processDT)
    returnedResults <- executeSql(conn, sql = processDT[,sql], noSplit=TRUE) # consider using errorReportFile parameter
    # documented at https://github.com/OHDSI/DatabaseConnector/blob/f0d479082b9f127d18eed65fa70adf6f17418e5c/R/Sql.R#L399
    # print(returnedResults)
  }
  return(returnedResults)
}

fileRecent <- function(filePath, daysToCheck) {
  fileDT <- file.mtime(filePath)
  retval <- ifelse(difftime(Sys.time(), fileDT, units = "days") <= daysToCheck,TRUE,FALSE)
  return(retval)
}

fileNoUpdate <- function(filenm, iDT, outdir) {
  fileDT <- as.double(file.mtime(paste0(outdir,filenm)))
  ftimes <- iDT[fileName==filenm,][][,ftime:=lapply(.SD,file.mtime),.SDcols=c("fullPathName")][,fileNewer:=(fileDT<as.double(ftime))]
  noUpdates <- ftimes[fileNewer==TRUE,.N]==0
  return(noUpdates)
}

identifyDateColumn <- function(sql, regexp='[A-Za-z_]*_DATE') {
  locstart <- regexpr(regexp, sql)
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
      if (de >= drEnd) de <- drEnd %m+% days(1)
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
is.defined <- function(sym) {
  sym <- deparse(substitute(sym))
  env <- parent.frame()
  exists(sym, env)
}

first.defined <- function(...) { # first parameter with a value (non-nully)
  # retval <- NA
  dotdotdots <- all.names(rlang::current_call())
  dotdotdots <- dotdotdots[2:length(dotdotdots)]
  namedlist <- Filter(function(x) {return(!(is.na(x) || is.null(x) || is.nan(x)))},mget(dotdotdots,parent.frame(),ifnotfound=NA,inherits=TRUE))
  return(unlist(namedlist[1],use.names=FALSE))
}

DBreconnect <- function(oldCnx) { # does not currently work as desired
    retval <- NULL
    if ("ConnectionDetails" %in% class(oldCnx)) {
        argList <- list()
        argList$dbms <- oldCnx$dbms
        argList$extraSettings <- oldCnx$extraSettings
        if (argList$dbms=="oracle") {
            argList$oracleDriver <- oldCnx$oracleDriver
        }
        argList$pathToDriver <- oldCnx$pathToDriver
        argList$user <- oldCnx$user()
        argList$password <- oldCnx$password()
        argList$server <- oldCnx$server()
        argList$port <- oldCnx$port()
        argList$connectionString <- oldCnx$connectionString()
        cdd <- do.call(DatabaseConnector::createConnectionDetails, argList)
        # cdd <- DatabaseConnector::createConnectionDetails(
        #    dbms=argList$dbms,
        #    user=argList$user,
        #    password=argList$password,
        #    server=argList$server, 
        #    port=argList$port, 
        #    extraSettings=argList$extraSettings,
        #    oracleDriver=argList$oracleDriver,
        #    pathToDriver=argList$pathToDriver
        # )
        print(paste("connection details re-created:", cdd$server(), labels(cdd)))
        retval <- DatabaseConnector::connect(cdd)
    }
    return(retval)
}

runExtraction  <- function(
  connectionDetails,
  sqlFilePath,
  cdmDatabaseSchema,
  resultsDatabaseSchema,
  outputFolder = paste0(getwd(), "/output/"),
  useAndromeda = FALSE,
  checkFileDates = FALSE,
  tempScipen = 999,
  cohortTable = "N3C_COHORT",
  dateBefore = TRUE,
  ...
) {
  # daysToCheck <- 0
  fileFails <- 0
  critError <- FALSE
  dotdotdots <- names(list(...))
  if (checkFileDates) {
    daysToCheck <- ifelse(("daysBetweenSubmissions" %in% dotdotdots) & (!is.na(as.numeric(daysBetweenSubmissions))),as.numeric(daysBetweenSubmissions),0)
    daysToCheck <- daysToCheck - ifelse(("dataLatencyNumDays" %in% dotdotdots) & (!is.na(as.numeric(dataLatencyNumDays))),as.numeric(dataLatencyNumDays),0) 
    if (daysToCheck < 0) daysToCheck <- 0
  }
  monthsToIncrement <- ifelse(('monthsToIncrement' %in% dotdotdots) & (is.integer(monthsToIncrement)),monthsToIncrement,NA)
  startDate <- first.defined(startDate, drStartShifted, drStart, dateRangeStart)
  endDate <- first.defined(endDate, drEndShifted, drEnd, dateRangeEnd) # TODO: consider checking LOCAL to see if endDate is in ...s, if not, don't check for global endDate until after drEndShifted
  # alternate: ifelse(('endDate' %in%  dotdotdots) & (!is.null(endDate)),endDate,ifelse(is.defined(drEndShifted),drEndShifted, ifelse(...))))
  
  # workaround to avoid scientific notation
  # save current scipen value
  scipen_val <- getOption("scipen")
  if (scipen_val != tempScipen) options(scipen.backup.n3c.phenotype = scipen_val)
  # temporarily change scipen setting (restored at end of f())
  options(scipen=999)

  # create output dir if it doesn't already exist
  # if (!file.exists(file.path(outputFolder)))
  #   dir.create(file.path(outputFolder), recursive = TRUE)

  if (!file.exists(paste0(outputFolder,"DATAFILES/toglob")))
    dir.create(paste0(outputFolder,"DATAFILES/toglob"), recursive = TRUE)

  # load source sql file
  src_sql <- SqlRender::readSql(sqlFilePath)

  # replace parameters with values
  src_sql <- SqlRender::render(
    sql = src_sql,
    warnOnMissingParameters = FALSE,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema,
    ...
  )
  nonDF <- c("MANIFEST.csv","DATA_COUNTS.csv","EXTRACT_VALIDATION.csv","DATA_COUNTS_APPEND.csv")
  # split script into chunks (to produce separate output files)
  allSQL <- parse_sql(src_sql)
  sqlsDT <- data.table::transpose(data.table::as.data.table(allSQL))
  # browser()
  setnames(sqlsDT,c("fileName","sql"))
  sqlsDT[,fileName:=gsub(pattern = "\r", x = fileName, replacement = "")]
  sqlsDT[,subFolder:=ifelse(fileName %in% nonDF,"","DATAFILES/")][,outputFolder:=paste0(outputFolder,subFolder)]
  # sqlsDT[,fullPathName:=paste0(outputFolder,"DATAFILES/",fileName)][fileName %in% nonDF,fullPathName:=paste0(outputFolder,fileName)]
  sqlsDT[,fullPathName:=paste0(outputFolder,fileName)]
  sqlsDT[,process:=TRUE][file.exists(fullPathName),process:=!fileRecent(fullPathName,daysToCheck)]
  sqlsDT[,dateColumn:=identifyDateColumn(sql)][,daterangeJoin:=grepl("{daterange_clause}",sql,fixed=TRUE)]
  sqlsDT[,startColumn:=identifyDateColumn(sql,regexp="[A-Za-z_]*_START_DATE")][,endColumn:=identifyDateColumn(sql,regexp="[A-Za-z_]*_END_DATE")]
  setindex(sqlsDT,fileName)
  # sqlsDT[,limitReplace:=paste0('AND ',overlap_fn,'(',paste(startDate,endDate,ifelse(is.na(startColumn),dateColumn,startColumn),ifelse(is.na(endColumn),'NULL',endColumn),sep=','),')')]
  # sqlsDT[,newSql:=mapply(function(x,y) {gsub(validDates,y,x,fixed=TRUE)}, sql, limitReplace)]
  # set incremental date-time substitutions
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
  mergedDT[is.na(newFileName),newFileName:=fileName]
  # check if each sub-file for e.g. Measurement exists
  mergedDT[,fullPathName:=paste0(outputFolder,newFileName)][,success:=FALSE][  
    file.exists(fullPathName),process:=!fileRecent(fullPathName,daysToCheck)]
  processDT <- mergedDT[process == TRUE, ]
  # print(processDT)
  # pause()
  # establish database connection
  conn <- DatabaseConnector::connect(connectionDetails)
  nfiles <- processDT[!is.na(newFileName),.N]
  if (nfiles > 0) {
    for (i in seq(1,nfiles)) { # go line-by-line
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
          fn <- processDT[i,newFileName]
          processDT[i,numResults:=executeChunk(conn = conn,
                                          sql = processDT[i,sql],
                                          fileName = fn,
                                          outputFolder = processDT[i,outputFolder])]
          # throw error if dup PKs found
          if(processDT[i,newFileName] == 'EXTRACT_VALIDATION.csv' && processDT[i,numResults] > 0){
            critError <- TRUE
            options(scipen=scipen_val)
            stop("Duplicate primary keys. See EXTRACT_VALIDATION.csv")
          }
          mergedDT[newFileName==fn,success:=TRUE]
        }
      }, error = function(cond) { 
        print(paste("Error encountered:",cond))
        try( {
          options(connectionObserver = NULL)
          DatabaseConnector::disconnect(conn)
        } )
        fileFails <- fileFails + 1
        if (critError == TRUE) { 
          options(scipen=scipen_val)
          stop("critical error; aborting loop")
        }
        print("***Resetting Database Connection!***")
        # browser()
        # disconnect(conn)
        conn <- DBreconnect(connectionDetails)
        # next # next gives "no loop for break/next, jumping to top level" which is probably not what we want.
        # stop() # don't quit - just go to next file, unless dup primary keys
      }, warning = function(cond) {
        print(paste("Warning only:",cond))
      }, finally = {
        print(paste("Finished processing file",processDT[i,newFileName]))
      })
    }
  } else {
    warning("No updated data to download (based on local file time alone)")
  }
  # Disconnect from database
  try(DatabaseConnector::disconnect(conn))
  #if (fileFails > 0) {
  fileSuc <- mergedDT[success==TRUE,.N]
  if (fileSuc < nfiles) {
    warning(paste(nfiles-fileSuc, "files processed were in error. Exiting without final cleanup."))
  } else {
    recombineDT <- mergedDT[daterangeJoin==TRUE,.(fileName,fullPathName)] # mergedDT rather than processDT as even if the data from "broken" files is stale, we still want to put it into the combined file
    dfd <- paste0(outputFolder,"DATAFILES/")
    # print(recombineDT)
    for (cfn in unique(recombineDT[,fileName])) {
      print(paste("Checking",cfn,"..."))
      if (file.exists(paste0(dfd,cfn)) & (fileNoUpdate(cfn, recombineDT, dfd) == TRUE)) {
        print(paste("    File",cfn,"does not need to be updated. All partial files are older than existing combined file."))
        next
      }
      print(paste("Combining file",cfn,"from bits"))
      cfp <- paste0(dfd,cfn)
      # file.create(cfp) # truncate existing combined file
      first <- TRUE
      for (fn in unique(recombineDT[fileName==cfn,fullPathName])) { # serially write to file
          print(paste("reading",fn))
          fwrite(x=fread(fn),file=cfp,append=!first) # first file written carries headers, rest do not.
          first <- FALSE
      }
      print(paste(cfn,"combined"))
    }
  }
  # restore original scipen value, but if we crashed out before, don't irrevocably clobber it
  options(scipen=getOption("scipen.backup.n3c.phenotype",scipen_val))
  # tidy up, delete temp setting
  options(scipen.backup.n3c.phenotype = NULL)
}

executeChunk <- function(conn, sql, fileName, outputFolder) {
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

executeChunkAndromeda <- function(conn, sql, fileName, outputFolder) {
  andr <- Andromeda::andromeda()
  DatabaseConnector::querySqlToAndromeda(
    connection = conn
    ,sql = sql
    ,andromeda = andr
    ,andromedaTableName = "tmp"
  )
  write.table(andr$tmp, file = paste0(outputFolder, fileName ), sep = "|", row.names = FALSE, na="")
  Andromeda::close(andr)
}


