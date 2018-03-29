require 'sequel'
require 'mysql2'
require 'awesome_print'
#require 'tiny_tds'


params = {
  adapter: 'mysql2',
  host: 'db',
  port: 3306,
  database: "db_analytics",
  username: "root",
  password: "password"
}

db = Sequel.connect params

sessions = db[:ga_sessions].limit(10)
sessions.each do |sess|
  ap sess
end
