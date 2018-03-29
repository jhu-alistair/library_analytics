# This class constructs query strings. It does not run them.
# It makes some assumptions about the database and the table,
# as noted in each method.

# EXAMPLE SCRIPT TO TEST METHODS IN THIS CLASS
# require_relative 'querybuilder'
# qb = QueryBuilder.new('src', 'ga_referrals', 'svc')
# puts qb.qry_append_new

class QueryBuilder
  require_relative 'filetotable'
  attr_reader :source_file, :target_table, :lib_service

  def initialize (src, tbl, svc)
    @source_file = src
    @target_table = tbl
    @f2t = FileToTable.new
    @lib_service = svc
  end

  def qry_create_temp
    # Use MySQL LIKE function to create empty temp table with same schema as target
    "CREATE TABLE temp_import LIKE #{@target_table};"
  end

  def qry_infile
    # create a list of SQL variables to hold the columns read in from the file
    # using list of target fields minus the lib_service field
    fld_vars = @f2t.source_columns_aliases(@target_table)
    str_fld_vars = fld_vars.join(', ')
    # create a list of all the SET statement components
    str_mapping = @f2t.import_values_list(@target_table).join(', ')
    # assemble query string
    qry_infl =  "LOAD DATA LOCAL INFILE '" + @source_file + "'"  + \
                " INTO TABLE temp_import #{@f2t.delimiters(@target_table)}" + \
                " IGNORE 1 LINES" + \
                " (#{str_fld_vars})" + \
                " SET lib_service = '#{@lib_service}', #{str_mapping};"
  end

  def qry_update_existing
    str_set = ''
    str_connector = ''
    @f2t.updatable_fields(@target_table).each do |fld|
      str_set += str_connector + "trgt.#{fld} = src.#{fld}"
      str_connector = ', '
    end

    str_where = ''
    str_connector = ''
    @f2t.unique_combo_fields(@target_table).each do |fld|
      str_where += str_connector + "trgt.#{fld} = src.#{fld}"
      str_connector = ' AND '
    end

    qry_updt =  "UPDATE #{@target_table} as trgt, temp_import as src" + \
                " SET #{str_set}" + \
                " WHERE #{str_where};"
  end

# Append to target table all records from temp_import that don't
# match records already in target table
  def qry_append_new
    str_trgt_flds = @f2t.target_field_list(@target_table).join(', ')
    src_fld_list = @f2t.target_field_list(@target_table).map { |fld1| "src.#{fld1}" }
    str_src_flds = src_fld_list.join(', ')
    where_list = @f2t.unique_combo_fields(@target_table).map { |fld2| "trgt.#{fld2} = src.#{fld2}" }
    str_where = where_list.join(' AND ')
    qry_appnd = "INSERT INTO #{@target_table} (#{str_trgt_flds})" + \
                " SELECT #{str_src_flds} FROM temp_import as src" + \
                " WHERE NOT EXISTS" + \
                " (SELECT 1 FROM #{@target_table} as trgt WHERE #{str_where});"
  end

  def qry_append_all
    str_trgt_flds = @f2t.target_field_list(@target_table).join(', ')
    src_fld_list = @f2t.target_field_list(@target_table).map { |fld1| "src.#{fld1}" }
    str_src_flds = src_fld_list.join(', ')
    qry_appnd = "INSERT INTO #{@target_table} (#{str_trgt_flds})" + \
                " SELECT #{str_src_flds} FROM temp_import as src;"
  end

# Not currently used. Removes records from temp_import
# that match the target table
##
##    DON'T FORGET TO REPLACE HARD-CODED TABLE NAME
##
  def qry_delete_dupes
    join_list = @f2t.unique_combo_fields(@target_table).map { |fld| "trgt.#{fld} = src.#{fld}" }
    str_join = join_list.join(' AND ')
    qry_del =  "DELETE src FROM temp_import as src " + \
                " JOIN ga_referrals as trgt" + \
                " ON #{str_join};"
  end

end
