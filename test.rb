require_relative 'lib/clever_tap.rb'
irb -Ilib -rclever_tap

CleverTap.setup do |c|
  c.identity_field = 'ID'
  c.account_id = ENV['CT_ACC_ID']
  c.account_passcode = ENV['CT_ACC_PWD']
end

ct = CleverTap::Client.new

query = { common_profile_properties: { profile_fields: [ { name: "ID", operator: "less_than_equals", value: 50 }	] } }
query = { event_name: "Register", from: 20180101, to: 20180420 }

# define Faraday

@f = Faraday.new('https://api.clevertap.com/1') do |config|
  # configure.call(config)

  # NOTE: set adapter only if there isn't one set
  config.adapter :net_http if config.builder.handlers.empty?

  config.headers['Content-Type'] = 'application/json'
  config.headers[CleverTap::Client::ACCOUNT_HEADER] = ENV['CT_ACC_ID']
  config.headers[CleverTap::Client::PASSCODE_HEADER] = ENV['CT_ACC_PWD']
end

def post(*args, &block)
  @f.post(*args, &block)
end

def get(*args, &block)
  @f.get(*args, &block)
end

ct.fetch_profiles(query, batch_size: 100)
ct.fetch_events(query, batch_size: 100)
