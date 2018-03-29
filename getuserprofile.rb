# Retrieve JHED user profile field values based on
# JHED ID
require 'yaml'
require_relative 'utilities'
class GetUserProfile
  include FieldPrep

  attr_accessor :jhed, :profile_fields
  attr_accessor :retrieved_profile

  def initialize(jhed)
    @profile_schema = YAML.load(File.open('jhed_profile_schema.yaml'))
    @retrieved_profile = YAML.load(File.open('sample_data/sample_jhed.yaml'))[jhed]
  end

  # Retrieves values from JHED system
  # to return a hash of field names + values; with the values
  # transformed as specified in jhed_profile_schema.yaml
  def retrieve_profile
    pfl = {}
    @profile_schema.each do |key, params|
      pfl[key] = validate_field(@retrieved_profile[key], params)
    end
    pfl
  end
end
