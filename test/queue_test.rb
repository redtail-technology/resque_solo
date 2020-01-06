# frozen_string_literal: true

require "test_helper"

class QueueTest < MiniTest::Spec
  describe ".is_unique?" do
    it "is false for non-unique job" do
      refute ResqueSolo::Queue.is_unique?(class: "FakeJob")
    end

    it "is false for invalid job class" do
      refute ResqueSolo::Queue.is_unique?(class: "InvalidJob")
    end

    it "is true for unique job" do
      assert ResqueSolo::Queue.is_unique?(class: "FakeUniqueJob")
    end
  end

  describe ".item_ttl" do
    it "is -1 for non-unique job" do
      assert_equal(-1, ResqueSolo::Queue.item_ttl(class: "FakeJob"))
    end

    it "is -1 for invalid job class" do
      assert_equal(-1, ResqueSolo::Queue.item_ttl(class: "InvalidJob"))
    end

    it "is -1 for unique job" do
      assert_equal(-1, ResqueSolo::Queue.item_ttl(class: "FakeUniqueJob"))
    end

    it "is job TTL" do
      assert_equal 300, UniqueJobWithTtl.ttl
      assert_equal 300, ResqueSolo::Queue.item_ttl(class: "UniqueJobWithTtl")
    end
  end

  describe ".release_lock_after_completion" do
    it "is false for non-unique job" do
      refute ResqueSolo::Queue.release_lock_after_completion(class: "FakeJob")
    end

    it "is false for invalid job class" do
      refute ResqueSolo::Queue.release_lock_after_completion(class: "InvalidJob")
    end

    it "is false for unique job" do
      refute ResqueSolo::Queue.release_lock_after_completion(class: "FakeUniqueJob")
    end

    it "is true for lock after completion job" do
      assert UniqueJobWithLock.release_lock_after_completion
      assert ResqueSolo::Queue.release_lock_after_completion(class: "UniqueJobWithLock")
    end
  end
end
