module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 5288
# Hash 8618
# Hash 2303
# Hash 1181
# Hash 9574
# Hash 4938
# Hash 1218
# Hash 7975
# Hash 1147
# Hash 7638
# Hash 1171
# Hash 9146
# Hash 4682
# Hash 5040
# Hash 5197
# Hash 3393
# Hash 3746
# Hash 3664
# Hash 1212
# Hash 8112
# Hash 2166
# Hash 7729
# Hash 9846
# Hash 4534
# Hash 5882
# Hash 7360
# Hash 1769
# Hash 4480