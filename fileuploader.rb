# File Uploader
# encoding: UTF-8

# a new comment

require 'fileutils'
require 'logger'
require 'mysql2'
require 'sequel'
require_relative 'scrubcsv'
require_relative 'querybuilder'
require_relative 'filetotable'

# This script opens a database connection and imports text files placed in a
# specified directory if they are named following a specified pattern. All the
# specifications are stored file_to_table.yaml and accessed via the FileToTable class

# This is a different YAML file with db parameters per environment
# Change the key following db_env below to change environments
db_conn = YAML.load(File.open('.config/db_connections.yaml'))
db_params = db_conn[:lag_warehouse][:development]
# Open db connection for importing text files to target tables
@db = Sequel.connect db_params

@lgr = nil # For log file. File name and directory set below.

# Create an instance of FileToTable class to hold schema info about
# each target data and info about the files that can be imported to it
f2t = FileToTable.new

# TARGET TABLE LOOP - walk through the list of db tables from the YAML file
f2t.target_table_list.each do |target_table|
  # start log
  logfile = f2t.source_dir(target_table) + '/_upload_log.txt'
  @lgr = Logger.new(logfile, 'monthly')
  # @db.logger = @lgr
  @lgr.info "\n\nLOG for fileuploader.rb"
  @lgr.info('Got Next Table') { target_table.to_s }

  # SOURCE FILE LOOP
  # Walk through a list of files in the source directory that match the file name pattern
  # specified for the current target table
  path_and_pattern = f2t.source_dir(target_table) + f2t.file_pattern(target_table)
  Dir.glob(path_and_pattern).each do |source_file|
    begin
    @lgr.info('File Uploader') { "Importing file  #{source_file}" }

    # prepare the current CSV file for import
    scrubby = ScrubCSV.new
    scrubby.scrub(target_table, source_file)
    @lgr.info('FilePrep') { "#{File.basename(source_file)} prepared for import" }

    # get lib_service from the current source file name
    parse_file_name = File.basename(source_file).split('-')
    lib_service = parse_file_name[1]
    @lgr.info('File Import') { "Importing #{File.basename(source_file)} to #{target_table}" }

    # Create an instance of QueryBuilder to construct query string
    # for all the queries we need for the current target table and source file
    bldr = QueryBuilder.new(source_file, target_table, lib_service)

    @lgr.info 'Drop temp_import if it exists'
    @db.drop_table?('temp_import')

    @lgr.info "Create empty temp_import with same schema as #{target_table}"
    @db.run bldr.qry_create_temp

    @lgr.info "Upload the #{File.basename(source_file)} to temp_import"
    @db.run bldr.qry_infile

    # Need to handle possibility of duplicate records, so assume user wants to
    # update the db records to match the import file
    if f2t.updatable_fields('ga_search_string_raw').any?
      @lgr.info "Update existing records in #{target_table} that match records in temp_import"
      @db.run bldr.qry_update_existing

      # Append records from temp_import that do not match existing records in target table
      @lgr.info "Append unmatched records from temp_import to #{target_table}"
      @db.run bldr.qry_append_new
    else
      @lgr.info "Append all records from temp_import to #{target_table}"
      @db.run bldr.qry_append_all

    end

    # Delete the text file now that it has been successfully imported
    FileUtils.rm(source_file)
    rescue => e
      @lgr.error('File Uploader') { "Upload for #{source_file} failed" }
      @lgr.error('File Uploader') { e.message }
    end
    @lgr.info 'Table imported'
  end
end
