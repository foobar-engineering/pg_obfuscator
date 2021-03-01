# frozen_string_literal: true

require 'faker'
require 'securerandom'

module DataFaker
  def self.call_method(method)
    eval(method)
  end

  class PhoneNumber
    Faker::Config.locale = 'en-US'

    class << self
      def phone_number
        Faker::PhoneNumber.unique.cell_phone_in_e164
      end
    end
  end

  class General
    class << self
      def pattern(value, fields)
        value % fields
      end

      def uuid
        SecureRandom.uuid
      end
    end
  end
end
