require 'mysql2'
require 'tiny_tds'
require 'sequel'
require 'yaml'
require 'awesome_print'

db_conn = YAML.load(File.open('.config/db_connections.yaml'))
db_params = db_conn[:horizon][:stage]
SOURCE_DB = Sequel.connect(db_params)

sql = "SELECT DISTINCT circ.borrower#
    , brw.second_id as hopkins_id
    , circ.cko_location
    , lookup_location.[name] as location_name
    , itm.itype
    , lookup_itype.descr as itype_descr
    , lookup_collection.descr as collection
    , itm.call_reconst as call_number
    , itm.[call] as itm_sort_order
    , itm.bib_id
    , itm.ibarcode
    , itm.title
    , YEAR(DATEADD(dd, cko_date, '1 Jan 1970')) *100 + MONTH(DATEADD(dd, cko_date, '1 Jan 1970')) as cko_month
    , YEAR(DATEADD(dd, cki_date, '1 Jan 1970')) *100 + MONTH(DATEADD(dd, cki_date, '1 Jan 1970')) as cki_month
FROM dbo.circ_history AS circ
    INNER JOIN borrower brw ON circ.borrower# = brw.borrower#
    INNER JOIN dbo.rv_item_with_title itm ON circ.item# = itm.item_id
    INNER JOIN dbo.location lookup_location ON circ.cko_location = lookup_location.location
    INNER JOIN dbo.itype lookup_itype ON itm.itype = lookup_itype.itype
    INNER JOIN dbo.collection lookup_collection ON itm.collection = lookup_collection.collection
WHERE
    brw.second_id IN ('A2367D', '17DAD3', '3F04BE', 'DBFC45', '604442', 'FCC0A6')
    AND itm.itype NOT IN ('borrdir', 'elocker')
    AND YEAR(DATEADD(dd, cki_date, '1 Jan 1970')) *100 + MONTH(DATEADD(dd, cki_date, '1 Jan 1970')) = 201801"

#  sql = "Select * from btype"
sql = "select * from rv_item_with_title WHERE bib_id = 421752"

src_ds = SOURCE_DB[sql]
src_ds.each{ |row| ap row }
