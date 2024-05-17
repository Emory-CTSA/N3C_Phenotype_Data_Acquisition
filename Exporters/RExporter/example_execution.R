# Set variable redshift_password outside of this script before running
# --- Installation ---

install.packages("remotes")
library(remotes)
Sys.setenv(JAVA_HOME="")
projectRoot <- "C:/Users/mpagel/OneDrive - Emory University/Documents/ETL/N3C/"
pathToDriver <- paste0(projectRoot,"drivers")
downloadJdbcDrivers("redshift",pathToDriver=pathToDriver)
setwd(paste0(projectRoot,"Phenotype_Data_Acquisition"))

# Uncomment to Verify JAVA_HOME is set to jdk path
# Sys.getenv("JAVA_HOME")


remotes::install_github(repo = "National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition"
               ,ref = "master"
               ,subdir = "Exporters/RExporter"
               ,INSTALL_opts = "--no-multiarch"
)

# Uncomment to test for missing packages
# setdiff(c("rJava", "DatabaseConnector","SqlRender","zip","N3cOhdsi"), rownames(installed.packages()))

# load package
library(lubridate)
library(N3cOhdsi)
library(data.table)

# --- Local configuration ---
write.table <- data.table::fwrite # (showProgress=TRUE, ...)

# -- run config
condets <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                      server = "redshift-omop.csa2k2lfxhl7.us-east-1.redshift.amazonaws.com/omop_prod",
                                                      user = "mpagel",
                                                      pathToDriver = pathToDriver,
                                                      password = redshift_password
)
conn <- connect(condets)
disconnect(conn)
connectionDetails <- condets
cdmDatabaseSchema <- "omop_deid" # schema for your CDM instance -- e.g. TMC_OMOP.dbo
resultsDatabaseSchema <- "n3c_ncats" # schema with write privileges -- e.g. OHDSI.dbo
# tempDatabaseSchema <- "" # For Google BigQuery users only
dateRangePartition <- "{daterange_clause}"

outputFolder <-  paste0(getwd(), "/output/")  # directory where output will be stored. default provided
phenotypeSqlPath <- fs::path_abs(paste0(getwd(),"/PhenotypeScripts/N3C_phenotype_omop_redshift.sql")) # full path of phenotype sql file (.../Phenotype_Data_Acquisition/PhenotypeScripts/N3C_phenotype_omop_redshift.sql))
extractSqlPath <- fs::path_abs(paste0(getwd(), "/ExtractScripts/5.4_to_5.3/N3C_extract_omop5.4_to_outputAs5.3_redshift.sql"))  # full path of extract sql file (.../Phenotype_Data_Acquisition/ExtractScripts/your_file.sql))

# FOR NLP SITES ONLY:
nlpSqlPath <- "/NLPExtracts/N3C_extract_nlp_mssql.sql" # full path of NLP extract sql file (.../Phenotype_Data_Acquisition/NLPExtracts/N3C_extract_nlp_mssql.sql)

# FOR ADT/VISIT_DETAIL SITES ONLY:
adtSqlPath <- "" # full path of ADT extract sql file (.../Phenotype_Data_Acquisition/ADTExtracts/N3C_extract_adt_mssql.sql)

# -- manifest config
siteAbbrev <- "EmoryHC" #-- unique site identifier
siteName   <- "Emory Healthcare"
contactName <- "Matt Pagel"
contactEmail <- "mpagel@emory.edu"
cdmName <- "OMOP" #-- source data model. options: "OMOP", "ACT", "PCORNet", "TriNetX"
# cdmVersion <- "5.3.1"
cdmVersion <- "5.3.1x5_4" # used to represent 5.3 output from a 5.4 DB. 
dataLatencyNumDays <- "1"  #-- this integer will be used to calculate UPDATE_DATE dynamically
daysBetweenSubmissions <- "7"  #-- this integer will be used to calculate NEXT_SUBMISSION_DATE dynamically
shiftDateYN <- "Y" #-- Replace with either 'Y' or 'N' to indicate if your data is date shifted
maxNumShiftDays <- "185" #-- Maximum number of days shifted. 'NA' if NA, 'Unknown' if shifted but days unknown
dateRangeStart <- '2018-01-01'
dateRangeEnd <- '2023-12-31'
monthsToIncrement <- 2    #-- For large tables, how many months at a time to query
dateRangePartition <- TRUE
# --- Execution ---

if (shiftDateYN == "Y") {
  mnsd <- strtoi(maxNumShiftDays)
  if (is.na(mnsd)) mnsd <- 0
} else mnsd <- 0
drStart <- parse_date_time(dateRangeStart,'ymd') # format(..., '%Y-%m-%d')
drEnd <-   parse_date_time(dateRangeEnd,'ymd')   # format(..., '%Y-%m-%d')
drStartShifted <- drStart - days(mnsd)
drEndShifted <-   drEnd + days(mnsd)

# Generate cohort
N3cOhdsi::createCohort(connectionDetails = connectionDetails,
                        sqlFilePath = phenotypeSqlPath,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        resultsDatabaseSchema = resultsDatabaseSchema
                      # ,tempDatabaseSchema = tempDatabaseSchema
                        )

datePartitionReturn <- function(sqlFilePath, monthsInterval, table = "MEASUREMENT", dateColumn = NULL) {
  if (is.null(dateColumn)) dateColumn <- paste0(table, "_DATE")
  
  
}


# Extract data to pipe delimited files; uses custom implementation
runExtraction(connectionDetails = connectionDetails,
                        sqlFilePath = extractSqlPath,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        resultsDatabaseSchema = resultsDatabaseSchema,
                        outputFolder = outputFolder,
                        siteAbbrev = siteAbbrev,
                        siteName = siteName,
                        contactName = contactName,
                        contactEmail = contactEmail,
                        cdmName = cdmName,
                        cdmVersion = cdmVersion,
                        dataLatencyNumDays = dataLatencyNumDays,
                        daysBetweenSubmissions = daysBetweenSubmissions,
                        shiftDateYN = shiftDateYN,
                        maxNumShiftDays = maxNumShiftDays,
                        dateRangePartition = "{daterange_clause}",
                        checkFileDates = TRUE,
                        monthsToIncrement = 2
            )

# OPTIONAL EXTENSIONS
#------------------
# For those sites that have opted in for adding in NLP and/or ADT data, you must first run the main extraction code above before executing below as these functions append to tables generated during that process

#(1/2) NLP
# FOR NLP SITES ONLY
# Assumes OHNLP has already been run, reads from NOTE and NOTE_NLP tables, extracts NLP data to pipe delimited files
# references path var 'nlpSqlPath'
#N3cOhdsi::runExtraction(connectionDetails = connectionDetails,
#                        sqlFilePath = nlpSqlPath,
#                        cdmDatabaseSchema = cdmDatabaseSchema,
#                        resultsDatabaseSchema = resultsDatabaseSchema,
#                        outputFolder = outputFolder
#)

# (2/2) ADT
# FOR ADT/VISIT_DETAIL SITES ONLY
# Assumes main extraction has already been run and the DATA_COUNTS.csv file generated, extracts visit_detail table and appends row counts to DATA_COUNTS.csv
# references path var 'adtSqlPath'
#N3cOhdsi::runExtraction(connectionDetails = connectionDetails,
#                        sqlFilePath = adtSqlPath,
#                        cdmDatabaseSchema = cdmDatabaseSchema,
#                        resultsDatabaseSchema = resultsDatabaseSchema,
#                        outputFolder = outputFolder
#)
#------------------------

# Compress output
zip::zipr(zipfile = paste0(siteAbbrev, "_", cdmName, "_", format(Sys.Date(),"%Y%m%d"),".zip"),
          files = list.files(path=c(outputFolder,paste0(outputFolder,"DATAFILES/")), pattern="csv$", full.names=TRUE))
