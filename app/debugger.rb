# frozen_string_literal: true

module Debugger
  def handle_errors(stage, payload)
    yield
  rescue StandardError => e
    $log.error("Error on stage '#{stage}': #{e.full_message}")
    $log.error("Payload: #{payload.inspect}")
    raise e
  end
end
