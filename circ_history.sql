SELECT
 brw.second_id as hopkins_id
, circ.cko_location
, lookup_location.[name] as cko_location_descr
, itm.itype
, lookup_itype.descr as itype_descr
, itm.collection as itm_collection
, lookup_collection.descr as itm_collection_descr
, itm.[call_reconstructed] as itm_call_number
, itm.[call] as itm_sort_order
, YEAR(dateadd(dd, circ.cko_date, '01-01-1970')) *100 + MONTH(dateadd(dd, circ.cko_date, '01-01-1970')) as cko_month
, YEAR(dateadd(dd, circ.cki_date, '01-01-1970')) *100 + MONTH(dateadd(dd, circ.cki_date, '01-01-1970')) as cki_month
, itm.bib# as itm_bib_id
, itm.ibarcode as itm_barcode
, isnull(lookup_title.processed, '') AS itm_title
 FROM circ_history AS circ
, borrower as brw
, item AS itm
, title as lookup_title
, location as lookup_location
, itype as lookup_itype
, collection as lookup_collection
 WHERE circ.borrower# = brw.borrower#
 AND circ.item# = itm.item#
 AND circ.cko_location = lookup_location.location
 AND itm.itype = lookup_itype.itype
 AND itm.bib# = lookup_title.bib#
 AND itm.collection = lookup_collection.collection
 AND YEAR(dateadd(dd, circ.cki_date, '01-01-1970')) *100 + MONTH(dateadd(dd, circ.cki_date, '01-01-1970')) > 201800
 AND itm.ibarcode = '31857001094867'
 AND itm.itype NOT IN ('elocker')
