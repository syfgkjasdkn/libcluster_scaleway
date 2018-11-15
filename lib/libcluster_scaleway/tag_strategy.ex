defmodule ClusterScaleway.TagStrategy do
  @moduledoc """
  This clustering strategy works by querying the API for droplets with a provided tag name
  and attempts to connect them.
  (see: https://developer.scaleway.com/#servers-servers-get)

  It assumes that all nodes share a base name and are using longnames of the form
  `<basename>@<ip_address>` where the `<ip_address>` is unique for each node.

  An example configuration is below:
      config :libcluster,
        topologies: [
          scaleway_example: [
            strategy: #{__MODULE__},
            config: [
              node_basename: "myapp",
              region: "par1", # "par1" | "ams1"
              ip_addr_type: {:public, :ipv4}, # {:public | :private, :ipv4 | :ipv6}
              tag_name: "awesome",
              token: "654c95b0-2cf5-41a3-b3cc-733ffba4b4b7",
              polling_interval: 10_000 # in ms
            ]
          ]
        ]
  """

  use GenServer
  use Cluster.Strategy

  alias Cluster.Strategy.State

  @default_polling_interval 5_000

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc false
  @impl true
  def init([%State{meta: nil} = state]) do
    init([%State{state | :meta => MapSet.new()}])
  end

  def init([%State{} = state]) do
    ensure_opts_valid!(state, [:tag_name, :token, :node_basename, :region, :ip_addr_type])
    {:ok, load(state)}
  end

  # TODO
  defp ensure_opts_valid!(%State{topology: topology, config: config}, keys) do
    Enum.each(keys, fn key ->
      value =
        config[key] || raise(ArgumentError, "libcluster:#{topology} missing #{inspect(key)}")

      ensure_valid_option!(topology, key, value)
    end)
  end

  defp ensure_valid_option!(topology, key, value) do
    (is_binary(value) and value != "") ||
      raise(
        ArgumentError,
        "libcluster:#{topology} invalid option for #{inspect(key)}: #{inspect(value)}"
      )
  end

  @doc false
  @impl true
  def handle_info(:timeout, state) do
    handle_info(:load, state)
  end

  def handle_info(:load, %State{} = state) do
    {:noreply, load(state)}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  defp load(
         %State{
           topology: topology,
           connect: connect,
           disconnect: disconnect,
           list_nodes: list_nodes,
           config: config,
           meta: meta
         } = state
       ) do
    new_nodelist = topology |> ClusterScaleway.get_nodes(config) |> MapSet.new()
    added = MapSet.difference(new_nodelist, meta)
    removed = MapSet.difference(meta, new_nodelist)

    new_nodelist =
      case Cluster.Strategy.disconnect_nodes(
             topology,
             disconnect,
             list_nodes,
             MapSet.to_list(removed)
           ) do
        :ok ->
          new_nodelist

        {:error, bad_nodes} ->
          # Add back the nodes which should have been removed, but which couldn't be for some reason
          Enum.reduce(bad_nodes, new_nodelist, fn {n, _}, acc ->
            MapSet.put(acc, n)
          end)
      end

    new_nodelist =
      case Cluster.Strategy.connect_nodes(topology, connect, list_nodes, MapSet.to_list(added)) do
        :ok ->
          new_nodelist

        {:error, bad_nodes} ->
          # Remove the nodes which should have been added, but couldn't be for some reason
          Enum.reduce(bad_nodes, new_nodelist, fn {n, _}, acc ->
            MapSet.delete(acc, n)
          end)
      end

    Process.send_after(self(), :load, config[:polling_interval] || @default_polling_interval)

    %{state | :meta => new_nodelist}
  end
end
