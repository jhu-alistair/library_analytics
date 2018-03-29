# Rewrite CSV file in preparation for import to MySQL
# Called by fileuploader.rb
# Omits any csv columns not mapped to target table fields in file_to_table.yaml
class ScrubCSV
  require 'fileutils'
  require 'csv'
  require_relative 'filetotable'

  def scrub(tbl, import_file)
    f2t = FileToTable.new
    import_dir = f2t.source_dir(tbl)
    com_rgx = f2t.comment_regex(tbl)
    import_cols = f2t.source_columns_list(tbl)

    # make a copy of the import file
    temp_file = "#{import_dir}/_temp-" + File.basename(import_file)
    FileUtils.copy_file import_file, temp_file, :preserve=>true, :remove_destination=>true

    # open the import file as a CSV for writing and purge existing content
    trgt_file = CSV.open(import_file, 'w:utf-8', headers: import_cols, write_headers: true)
    src_file = CSV.open(temp_file, 'r:bom|utf-8', headers: true, skip_blanks: true, skip_lines: com_rgx)
    src_file.each do |src_row|
      #copy only the columns specified in file_to_table.yaml
      trgt_row = src_row.values_at(*import_cols)
      trgt_file << trgt_row
    end
    trgt_file.close
    src_file.close
    FileUtils.rm temp_file
  end
end
