require 'sequel'
require 'mysql2'
require 'yaml'

@tbl = :lookup_lib_service
db_conn = :lag_warehouse
yfile = YAML.load(File.open('.config/db_connections.yaml'))
env = yfile[:active_env]
@db_params = yfile[db_conn][env]

trgt = Sequel.connect(@db_params)

trgt_ds = trgt[@tbl]

myrow = {:ID=>"test12", :descrip=>"Delete me, please"}
puts myrow

trgt_ds.insert(myrow)
