require 'mysql2'
require 'tiny_tds'
require 'sequel'
require 'yaml'
require 'awesome_print'

db_conn = YAML.load(File.open('.config/db_connections.yaml'))
db_params = db_conn[:horizon][:stage]
SOURCE_DB = Sequel.connect(db_params)

ap db_params

sql = "SELECT
circ.cko_location
, lookup_location.[name] as location_name
, itm.itype
, lookup_itype.descr as itype_descr
, lookup_collection.descr as collection
, itm.[call_reconst] as call_number
, itm.[call] as itm_sort_order
, YEAR(dateadd(dd, circ.cko_date, '01-01-1970')) *100 + MONTH(dateadd(dd, circ.cko_date, '01-01-1970')) as cko_month
, YEAR(dateadd(dd, circ.cki_date, '01-01-1970')) *100 + MONTH(dateadd(dd, circ.cki_date, '01-01-1970')) as cki_month
, itm.bib# as bib_id
, itm.ibarcode as barcode
, isnull(lookup_title.title, '') AS 'title'
FROM circ_history AS circ
, item AS itm
, title_inverted as lookup_title
, location as lookup_location
, itype as lookup_itype
, collection as lookup_collection
WHERE circ.item# = itm.item#
AND circ.cko_location = lookup_location.location
AND itm.itype = lookup_itype.itype
AND itm.bib# = lookup_title.bib#
AND itm.collection = lookup_collection.collection
AND YEAR(dateadd(dd, circ.cki_date, '01-01-1970')) *100 + MONTH(dateadd(dd, circ.cki_date, '01-01-1970')) >= 201804
AND itm.itype NOT IN ('elocker')"

src_ds = SOURCE_DB[sql]
src_ds.each{ |row| ap row }
