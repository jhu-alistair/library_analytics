# A set of mixins for commonly used functions
module FieldPrep
  def validate_field (fld_value, fld_params)
    case fld_params[:data_type]
      when "integer_fld"
        return fld_value.to_i
      when "float_fld"
        return fld_value.to_f
      when "string_fld"
        return fld_value.to_s[0, fld_params[:max_length]]
      when "date_fld"
        return fld_value.to_date
      else
        # throw an error for unrecognized data type
    end
  end
end
