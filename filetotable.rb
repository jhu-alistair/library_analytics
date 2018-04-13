# Reads file_to_table yaml
class FileToTable
  require 'yaml'
  attr_reader :file_to_table, :target_tables_params

  def initialize
    @file_to_table = YAML.load(File.open('file_to_table.yaml'))
    @target_tables_params = @file_to_table[:target_tables]
  end

  # TABLE LIST
  def target_table_list
    @file_to_table[:target_tables].keys
  end

  # TABLE PROPERTIES
  def source_dir(tbl)
    @target_tables_params[tbl][:source_file][:source_dir]
  end

  def file_pattern(tbl)
    @target_tables_params[tbl][:source_file][:file_pattern]
  end

  def file_format(tbl)
    @target_tables_params[tbl][:source_file][:format]
  end

  def delimiters(tbl)
    @target_tables_params[tbl][:source_file][:delimiters]
  end

  def comment_regex(tbl)
    Regexp.new(@target_tables_params[tbl][:source_file][:comment_regex]) unless @target_tables_params[tbl][:source_file][:comment_regex].nil?
  end

  def target_field_list(tbl)
    @target_tables_params[tbl][:target_fields].keys
  end

  def source_columns_list(tbl)
    cols = Array[]
    @target_tables_params[tbl][:target_fields].keys.each do |fld|
      fld_params = @target_tables_params[tbl][:target_fields][fld]
      cols.push fld_params[:map_to_col] unless fld_params.nil? || fld_params[:map_to_col].nil?
    end
    return cols
  end

# Alias equals the corresponding target field prefixed with @
# Fields that are not mapped to columns are omitted
  def source_columns_aliases(tbl)
    cols = Array[]
    @target_tables_params[tbl][:target_fields].keys.each do |fld|
      fld_params = @target_tables_params[tbl][:target_fields][fld]
      cols.push fld unless fld_params.nil? || fld_params[:map_to_col].nil?
    end
    return cols.map { |e| "@#{e}" }
  end

  def import_values_list(tbl)
    imprt = Array[]
    @target_tables_params[tbl][:target_fields].keys.each do |fld|
      fld_params = @target_tables_params[tbl][:target_fields][fld]
      imprt.push fld_params[:import_value] unless fld_params.nil? || fld_params[:import_value].nil?
    end
    return imprt
  end

  def unique_combo_fields(tbl)
    unique_combo = Array[]
    @target_tables_params[tbl][:target_fields].keys.each do |fld|
      fld_params = @target_tables_params[tbl][:target_fields][fld]
      unique_combo.push fld if fld_params[:part_of_unique] == 'Y'
    end
    return unique_combo
  end

  def updatable_fields(tbl)
    # Returns empty array if there are no updatable fields
    unique_combo = Array[]
    @target_tables_params[tbl][:target_fields].keys.each do |fld|
      fld_params = @target_tables_params[tbl][:target_fields][fld]
      unless fld_params.nil? || fld_params[:part_of_unique].nil?
        unique_combo.push fld if fld_params[:part_of_unique] == 'N'
      end
    end
    return unique_combo
  end

  # FIELD PROPERTIES
  def source_column(tbl, fld)
    @target_tables_params[tbl][:target_fields][fld][:map_to_col]
  end

  def import_value(tbl, fld)
    @target_tables_params[tbl][:target_fields][fld][:import_value]
  end
end
