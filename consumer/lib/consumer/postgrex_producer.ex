defmodule PgProducer do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def check_table(name \\ "users") do
    GenServer.call(__MODULE__, {:check_table, name})
  end

  def drop_table(name \\ "users") do
    GenServer.call(__MODULE__, {:drop_table, name})
  end

  def new_table(name \\ "users") do
    GenServer.call(__MODULE__, {:create_new_table, name})
  end

  def run(nb, name \\ "users") do
    GenServer.call(__MODULE__, {:run, nb, name})
  end

  def run_test() do
    GenServer.call(__MODULE__, :run_test)
  end

  @impl GenServer
  def init(opts) do
    tables = Keyword.get(opts, :tables, ["users"])
    {:ok, pid} = Postgrex.start_link(opts)
    Enum.each(tables, fn table -> create_table(table, pid) end)
    {:ok, {pid, 0}}
  end

  @impl GenServer

  def handle_call({:create_new_table, name}, _, {pid, _} = state) do
    :ok = create_table(name, pid)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:run, nb, name}, _, {pid, i} = _state) do
    # Logger.info("Running PG statements...")

    # Generate list of maps for bulk insert

    values =
      for idx <- 1..nb do
        %{
          name: "User-#{i}-#{idx}",
          email: "user-#{i}-#{idx}@example.com"
        }
      end

    Producer.Repo.insert_all(name, values)

    # %Postgrex.Result{} =
    # Postgrex.query!(pid, """
    # INSERT INTO #{name} (name, email)
    # VALUES ('#{user_name}', 'user#{i}@example.com');
    # """)

    # cond do
    #   rem(i, 5) == 0 ->
    #     user_name = "User #{i}"

    #     %Postgrex.Result{} =
    #       Postgrex.query!(state, "DELETE FROM #{name} WHERE name = $1", [user_name])

    #   rem(i, 2) == 0 ->
    #     %Postgrex.Result{} =
    #       Postgrex.query!(state, """
    #       UPDATE #{name} SET name = '#{user_name}', email = 'user#{i}@example.com'
    #       WHERE id = #{i};
    #       """)

    #   true ->
    #     :ok
    # end
    # end

    # Logger.info("PG job done")

    {:reply, :ok, {pid, i + 1}}
  end

  @impl GenServer
  def handle_call({:check_table, name}, _, {pid, _} = state) do
    try do
      query =
        Postgrex.prepare!(pid, "", "SELECT * FROM #{name};", [])

      len =
        Postgrex.execute!(pid, query, [])
        |> Map.get(:num_rows)

      {:reply, len, state}
    rescue
      e in Postgrex.Error ->
        {:reply, e.postgres.message, state}
    end
  end

  @impl GenServer
  def handle_call({:drop_table, name}, _, {pid, _} = state) do
    Postgrex.query!(pid, "DROP TABLE IF EXISTS #{name};")
    {:reply, :ok, state}
  end

  def handle_call(:run_test, _, {pid, _} = state) do
    test(pid)
    {:reply, :ok, state}
  end

  defp create_table("test_types" = name, pid) do
    %Postgrex.Result{} =
      Postgrex.query!(pid, """
      CREATE TABLE IF NOT EXISTS #{name} (
        id SERIAL PRIMARY KEY,
        age INT,
        temperature FLOAT8,
        price NUMERIC(20,8),
        is_true BOOLEAN,
        some_text TEXT,
        tags TEXT[],
        matrix INT[][],
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT now()
      );
      """)

    :ok
  end

  defp create_table(name, pid) do
    %Postgrex.Result{} =
      Postgrex.query!(pid, """
      CREATE TABLE IF NOT EXISTS #{name} (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        create_at TIMESTAMPTZ DEFAULT now()
      );
      """)

    :ok
  end

  defp test(pid) do
    %Postgrex.Result{} =
      Postgrex.query!(pid, """
      INSERT INTO test_types (age, temperature, is_true, some_text, tags, matrix, metadata, price)
      VALUES (30, 36.6, true, 'Sample text', ARRAY['tag1', 'tag2'], ARRAY[[1,2],[3,4]], '{"key_1": "value_1",  "key_2": [1,2], "key_3": {"key_4": "value_4", "key_5": "value_5"}}'::jsonb, 123.45);
      """)
  end
end

# Stream.interval(1) |> Stream.take(1_000) |> Task.async_stream(fn _ -> PgProducer.run(100, Enum.random(["users", "orders"])) end, ordered: false) |> Stream.run()
# Stream.interval(1) |> Stream.take(10_000) |> Task.async_stream(fn _ -> PgProducer.run(140, "users") end, ordered: false) |> Stream.run()
