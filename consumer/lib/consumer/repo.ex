defmodule Producer.Repo do
  use Ecto.Repo,
    otp_app: :consumer,
    adapter: Ecto.Adapters.Postgres,
    database: "postgres",
    pool_size: 10,
    password: "postgres",
    username: "postgres",
    hostname: "localhost"
end
