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

  @impl GenServer
  def init(opts) do
    table = Keyword.get(opts, :table, "users")
    {:ok, pid} = Postgrex.start_link(opts)
    :ok = create_table(table, pid) |> dbg()
    {:ok, {pid, 0}}
  end

  @impl GenServer

  def handle_call({:create_new_table, name}, _, {pid, _} = state) do
    :ok = create_table(name, pid)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:run, nb, _name}, _, {pid, i} = _state) do
    # Logger.info("Running PG statements...")

    # Generate list of maps for bulk insert
    values =
      for idx <- 1..nb do
        user_name = "User #{idx}"

        %{
          name: user_name,
          email: "user#{idx}@example.com"
        }
      end

    Producer.Repo.insert_all("users", values)

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

    {:reply, :ok, {pid, i}}
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

  defp create_table(name, pid) do
    %Postgrex.Result{} =
      Postgrex.query!(pid, """
      CREATE TABLE IF NOT EXISTS #{name} (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
      );
      """)

    :ok
  end
end
