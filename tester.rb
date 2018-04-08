# require_relative 'scrubcsv'
# scrubby = ScrubCSV.new
# scrubby.scrub('bd_requests', '../data_transfer/import/bd_requests-bordir-short.csv')
#
# require_relative 'querybuilder'
# qb = QueryBuilder.new('lagwork/analytics/data_transfer/import/ga_search_string_raw-wshrdn-2017-07-01c','ga_search_string_raw','wshrdn')
# puts qb.qry_infile
#
# require_relative 'filetotable'
# f2t = FileToTable.new
# puts f2t.updatable_fields('ga_search_string_raw').any?


# require_relative 'filetotable'
# f2t = FileToTable.new
# puts f2t.source_columns_aliases('bd_requests')

# require 'yaml'
# db_conn = YAML.load(File.open('.config/db_connections.yaml'))
# db_params = db_conn[:lag_warehouse][:development]
# puts db_params
#
# require_relative 'tabletotable'
# t2t = TableToTable.new
# puts t2t.last_completed_date('import_hz_sample')

# require_relative 'getuserprofile'
# pfl = GetUserProfile.new('jkl123')
# puts pfl.retrieve_profile

# require 'yaml'
# attrs = YAML.load(File.open('.config/ldap_connection.yaml'))['development']
# puts attrs
#
# tidy = {'host'=>"win.johnshopkins.edu", 'port'=>636, 'encryption'=>{'method'=>:simple_tls}, 'auth'=>{'method'=>:simple, 'username'=>"cn=amorri63,ou=people,dc=win,dc=ad,dc=jhu,dc=edu", 'password'=>"CaptBiggles!"}}
# puts tidy
# puts tidy.to_yaml
require 'awesome_print'
# require_relative 'ldap_lookup'
require_relative 'getuserprofile'
looker = GetUserProfile.new
ap looker.ldap_lookup('uid', 'zkhan16')
