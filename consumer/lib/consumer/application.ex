defmodule Consumer.Application do
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    children = [
      Producer.Repo,
      {Task, fn -> :setup_jet_stream end},
      {PgProducer, args()},
      {Gnat.ConnectionSupervisor, gnat_supervisor_settings()},
      # JetStream pull consumer for CDC events
      {Consumer.Cdc, consumer_cdc_settings()},
      {Consumer.Schema, consumer_schema_settings()}
    ]

    opts = [strategy: :one_for_one, name: Consumer.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def setup_jet_stream do
    {:ok, %{created: _}} =
      Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{
        name: "CDC",
        subjects: ["cdc.>"]
      })

    {:ok, %{created: _}} = Gnat.Jetstream.API.Stream.info(:gnat, "CDC")

    {:ok, %{created: _}} =
      Gnat.Jetstream.API.Stream.create(:gnat, %Gnat.Jetstream.API.Stream{
        name: "SCHEMA",
        subjects: ["schema.>"]
      })

    {:ok, %{created: _}} = Gnat.Jetstream.API.Stream.info(:gnat, "SCHEMA")
  end

  defp get_tables do
    System.get_env("TABLES") |> String.split(",") |> Enum.map(&String.trim/1)
  end

  defp args do
    [
      hostname: System.get_env("PG_HOST") || "localhost",
      port: String.to_integer(System.get_env("PG_PORT") || "5432"),
      username: System.get_env("PG_USER") || "postgres",
      password: System.get_env("PG_PASSWORD") || "postgres",
      database: System.get_env("PG_DB") || "postgres",
      name: PgEx,
      tables: get_tables() || ["users", "orders"]
    ]
  end

  defp consumer_cdc_settings do
    %Gnat.Jetstream.API.Consumer{
      # consumer position tracking is persisted
      durable_name: "ex_cdc_consumer",
      stream_name: "CDC",
      ack_policy: :explicit,
      # 60 seconds in nanoseconds
      ack_wait: 60_000_000_000,
      max_deliver: 3,
      filter_subject: "cdc.>"
    }
  end

  defp consumer_schema_settings do
    %Gnat.Jetstream.API.Consumer{
      # consumer position tracking is persisted
      durable_name: "ex_schema_consumer",
      stream_name: "SCHEMA",
      ack_policy: :explicit,
      # 60 seconds in nanoseconds
      ack_wait: 60_000_000_000,
      max_deliver: 3,
      filter_subject: "schema.>"
    }
  end

  defp gnat_supervisor_settings do
    %{
      name: :gnat,
      backoff_period: 4_000,
      connection_settings: [
        %{host: "127.0.0.1", port: 4222}
      ]
    }
  end
end
