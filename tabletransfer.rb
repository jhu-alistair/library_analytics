# Table Transfer
# encoding: UTF-8
require 'logger'
require 'mysql2'
require 'sequel'
require_relative 'tabletotable'

# This script extracts data from source tables and writes them to
# target table_fields

# Load YAML file with database connection parameters
db_conn = YAML.load(File.open('.config/db_connections.yaml'))

# Open target table
trgt_params = db_conn[:lag_warehouse][:development]
DB = Sequel.connect trgt_params
trgt_ds = DB[:hz_sample]

# Open source table
src_params = db_conn[:horizon][:development]
SOURCE_DB = Sequel.connect src_params
src_ds = SOURCE_DB[:sample_sample]

# Read source table one row at a time
src_ds.each do |src_row|
    p src_row
  end


trgt_ds.insert(lib_service: 'fake', abc: 'test1')
