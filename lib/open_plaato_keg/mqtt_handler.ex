defmodule OpenPlaatoKeg.MqttHandler do
  require Logger
  alias OpenPlaatoKeg.Models.AirlockData
  alias OpenPlaatoKeg.Models.KegData

  def init(args) do
    Logger.info("", data: inspect(args))
    {:ok, args}
  end

  def connection(status, state) do
    Logger.info("", data: inspect(%{status: status, state: state}))
    {:ok, state}
  end

  def handle_message(topic, payload, state) do
    Logger.debug(
      "Received message on topic #{inspect(topic)}, playload #{payload}, state: #{inspect(state)}"
    )

    {:ok, state}
  end

  def subscription(status, topic_filter, state) do
    Logger.info("", data: inspect(%{status: status, topic_filter: topic_filter, state: state}))
    {:ok, state}
  end

  def terminate(reason, state) do
    Logger.info("", data: inspect(%{reason: reason, state: state}))
    :ok
  end

  def publish(id, data) do
    config = OpenPlaatoKeg.mqtt_config()
    do_publish(config, config[:topic], id, data, fn -> KegData.get(id) end)
  end

  def publish_airlock(id, data) do
    config = OpenPlaatoKeg.mqtt_config()
    do_publish(config, config[:airlock_topic], id, data, fn -> AirlockData.get(id) end)
  end

  defp do_publish(config, base_topic, id, data, get_all_data) do
    if config[:json_output] do
      Tortoise.publish(
        config[:client],
        "#{base_topic}/#{id}",
        Poison.encode!(get_all_data.()),
        qos: 0,
        retain: true
      )
    end

    if config[:property_output] do
      Enum.each(data, fn {key, value} ->
        Tortoise.publish(
          config[:client],
          "#{base_topic}/#{id}/#{key}",
          encode_value(value),
          qos: 0,
          retain: true
        )
      end)
    end
  end

  defp encode_value(value) when is_binary(value), do: value
  defp encode_value(value) when is_map(value), do: Poison.encode!(value)
  defp encode_value(value), do: to_string(value)
end
