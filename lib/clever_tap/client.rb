require 'faraday'
require 'pry'

class CleverTap
  class NotConsistentArrayError < RuntimeError
    def message
      'Some elements in the collection are of different type than the others'
    end
  end

  class Client
    DOMAIN = 'https://api.clevertap.com'.freeze
    API_VERSION = 1
    HTTP_PATH = 'upload'.freeze
    DEFAULT_SUCCESS = proc { |r| r.to_s }
    DEFAULT_FAILURE = proc { |r| r.to_s }
    DEFAULT_FETCH_BATCH_SIZE = 50

    ACCOUNT_HEADER = 'X-CleverTap-Account-Id'.freeze
    PASSCODE_HEADER = 'X-CleverTap-Passcode'.freeze

    attr_accessor :account_id, :passcode, :configure, :on_success, :on_failure

    def initialize(account_id = nil, passcode = nil, &configure)
      @account_id = assign_account_id(account_id)
      @passcode = assign_passcode(passcode)
      @configure = configure || proc {}
      @on_success = DEFAULT_SUCCESS
      @on_failure = DEFAULT_FAILURE
    end

    def connection
      # TODO: pass the config to a block
      @connection ||= Faraday.new("#{DOMAIN}/#{API_VERSION}") do |config|
        configure.call(config)

        # NOTE: set adapter only if there isn't one set
        config.adapter :net_http if config.builder.handlers.empty?

        config.headers['Content-Type'] = 'application/json'
        config.headers[ACCOUNT_HEADER] = account_id
        config.headers[PASSCODE_HEADER] = passcode
      end
    end

    def post(*args, &block)
      connection.post(*args, &block)
    end

    def get(*args, &block)
      connection.get(*args, &block)
    end

    def on_successful_upload(&block)
      @on_success = block
    end

    def on_failed_upload(&block)
      @on_failure = block
    end

    def upload(records, dry_run: 0)
      payload = ensure_array(records)
      entity = determine_type(payload)
      all_responses = []
      batched_upload(entity, payload, dry_run) do |response|
        all_responses << response
      end

      all_responses
    end

    def fetch_profiles(*args)
      fetch(Profile, *args)
    end

    def fetch_events(*args)
      fetch(Event, *args)
    end

    def fetch(entity, query, options = {})
      puts 'gets into fetch'
      raise 'Query must be a Hash' unless query.is_a?(Hash)
      batch_size = options[:batch_size] || DEFAULT_FETCH_BATCH_SIZE
      @uri = entity::FETCH_URI
      cursor = fetch_cursor(query.to_json, batch_size)

      return unless cursor
      records = fetch_all_records(cursor)

      records.map { |r| entity.parse(r) }
    end

    def batched_upload(entity, payload, dry_run)
      payload.each_slice(entity.upload_limit) do |group|
        response = post(HTTP_PATH, request_body(group)) do |request|
          request.params.merge!(dryRun: dry_run)
        end

        clevertap_response = Response.new(response)

        handle_callbacks(clevertap_response)

        yield(clevertap_response) if block_given?
      end
    end

    def request_body(records)
      { 'd' => records.map(&:to_h) }.to_json
    end

    def determine_type(records)
      types = records.map(&:class).uniq
      raise NotConsistentArrayError unless types.one?
      types.first
    end

    def ensure_array(records)
      Array(records)
    end

    def assign_account_id(account_id)
      account_id || CleverTap.account_id || raise('Clever Tap `account_id` missing')
    end

    def assign_passcode(passcode)
      passcode || CleverTap.account_passcode || raise('Clever Tap `passcode` missing')
    end

    def handle_callbacks(response)
      if response.success
        @on_success.call(response)
      else
        @on_failure.call(response)
      end
    end

    def fetch_all_records(cursor)
      current_cursor = cursor
      records = []

      while current_cursor do
        current_records, current_cursor = 
          fetch_records_and_next_cursor(current_cursor)
        records << current_records
      end

      records.flatten.compact
    end

    def fetch_cursor(query, batch_size)
      req = post(@uri, query) do |request|
        request.params.merge!(batch_size: batch_size)
      end
      res = Response.new(req)

      handle_callbacks(res)
      return unless res.success
      res.response['cursor']
    end

    def fetch_records_and_next_cursor(cursor)
      # NOTE: parameter interpolated because when passed in a block as
      # { |req| req.params.merge!(cursor: body['cursor']) }
      # it gets URL encoded which breaks it
      res = Response.new(get("#{@uri}?cursor=#{cursor}"))
      handle_callbacks(res)
      return [[], nil] unless res.success
      [res.response['records'], res.response['next_cursor']]
    end
  end
end
