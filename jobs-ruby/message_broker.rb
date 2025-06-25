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
# Hash 5742
# Hash 1167
# Hash 1951
# Hash 5890
# Hash 8134
# Hash 7781
# Hash 3622
# Hash 5870
# Hash 7370
# Hash 1153
# Hash 7982
# Hash 5615
# Hash 2772
# Hash 1660
# Hash 7993
# Hash 3380
# Hash 5347
# Hash 9585
# Hash 2882
# Hash 6353
# Hash 9765
# Hash 3925
# Hash 1992
# Hash 2618
# Hash 4928
# Hash 3640
# Hash 3903
# Hash 8911
# Hash 7917
# Hash 3722
# Hash 2554
# Hash 9338
# Hash 1085
# Hash 4680
# Hash 3281
# Hash 9970
# Hash 1154
# Hash 4910
# Hash 6930
# Hash 8427
# Hash 4655
# Hash 2850
# Hash 5772
# Hash 6833
# Hash 5123
# Hash 1269
# Hash 6105
# Hash 9803
# Hash 1008
# Hash 4064
# Hash 1614
# Hash 8616
# Hash 2187
# Hash 6780
# Hash 5363
# Hash 8178
# Hash 2811
# Hash 3415
# Hash 2106
# Hash 6425
# Hash 8311
# Hash 8454
# Hash 1716
# Hash 8761
# Hash 4255
# Hash 8926
# Hash 6858
# Hash 3923
# Hash 5864
# Hash 8784
# Hash 6059
# Hash 4327
# Hash 1351
# Hash 2926
# Hash 2073
# Hash 3303
# Hash 3516
# Hash 8716
# Hash 3858
# Hash 5063
# Hash 8860
# Hash 2752
# Hash 4390
# Hash 5699
# Hash 3579
# Hash 7653
# Hash 7731
# Hash 2837
# Hash 9622
# Hash 9022
# Hash 2390
# Hash 8711
# Hash 2554
# Hash 7437
# Hash 3344
# Hash 6761
# Hash 7069
# Hash 1994
# Hash 6733
# Hash 4938
# Hash 4483
# Hash 1037
# Hash 1547