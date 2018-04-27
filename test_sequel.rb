# require 'mysql2'
# require 'tiny_tds'
# require 'sequel'
# require 'yaml'
# require 'awesome_print'
require_relative 'etl_sources'
require_relative 'common'

# db_conn = YAML.load(File.open('.config/db_connections.yaml'))
# env = db_conn[:active_env]
# db_params = db_conn[:horizon][env]
# ap db_params
# SOURCE_DB = Sequel.connect(db_params)

# ap db_params

db_conn = :horizon
sql = get_sql('circ_history')
# my_source = DbSource.new(db_conn, sql)

source DbSource, :horizon, sql
show_me!

# src_ds = SOURCE_DB[sql]
# src_ds.each{ |row| ap row }
