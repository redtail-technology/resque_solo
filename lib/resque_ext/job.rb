# frozen_string_literal: true

module Resque
  class Job
    class << self
      # Mark an item as queued
      def create_solo(queue, klass, *args)
        item = { class: klass.to_s, args: args }
        if Resque.inline? || !ResqueSolo::Queue.is_unique?(item)
          return create_without_solo(queue, klass, *args)
        end

        data_args, metadata_args = parse_args(args)
        item = { class: klass.to_s, args: data_args }

        create_return_value = false

        Resque.redis.watch(ResqueSolo::Queue.unique_key(queue, item)) do
          if ResqueSolo::Queue.queued?(queue, item)
            Resque.redis.unwatch
            create_return_value = "EXISTED"
          else
            create_return_value = enqueue_job(queue, item, klass, data_args, metadata_args)
          end
        end

        create_return_value
      end

      def enqueue_job(queue, item, klass, data_args, metadata_args)
        Resque.redis.multi do
          return_value = create_without_solo(queue, klass, *data_args)
          ResqueSolo::Queue.mark_queued(queue, item, metadata_args)
          return_value
        end
      end

      def parse_args(args)
        if args.last.is_a?(Hash) && (args.last.key?(:metadata) || args.last.key?('metadata'))
          metadata = args.last.delete(:metadata) || args.last.delete('metadata')
          if args.length == 1
            [args, metadata]
          else
            [args[0..-2], metadata]
          end
        else
          [args, nil]
        end
      end

      # Mark an item as unqueued
      def reserve_solo(queue)
        item = reserve_without_solo(queue)
        return item unless item && !Resque.inline?

        metadata = ResqueSolo::Queue.mark_unqueued(queue, item)
        if metadata.is_a?(String)
          last_arg = item.payload["args"].last
          if last_arg.is_a?(Hash)
            last_arg["metadata"] = JSON.parse(metadata)
          else
            item.payload["args"] << { "metadata" => JSON.parse(metadata) }
          end
        end
        item
      end

      # Mark destroyed jobs as unqueued
      def destroy_solo(queue, klass, *args)
        ResqueSolo::Queue.destroy(queue, klass, *args) unless Resque.inline?
        destroy_without_solo(queue, klass, *args)
      end

      alias_method :create_without_solo, :create
      alias_method :create, :create_solo
      alias_method :reserve_without_solo, :reserve
      alias_method :reserve, :reserve_solo
      alias_method :destroy_without_solo, :destroy
      alias_method :destroy, :destroy_solo
    end
  end
end
