defmodule ClusterScaleway do
  @moduledoc false
  import Cluster.Logger

  @doc false
  def base_url(region) when region in ["par1", "ams1"] do
    "https://cp-#{region}.scaleway.com"
  end

  @doc false
  @spec get_nodes(module, Keyword.t()) :: [atom()]
  def get_nodes(topology, config) do
    headers = [{'accept', 'application/json'}, {'x-auth-token', '#{config[:token]}'}]

    case follow_link(base_url(config[:region]), "servers", headers) do
      {:ok, resps} when is_list(resps) ->
        resps
        |> Enum.flat_map(fn %{"servers" => servers} -> servers end)
        |> Enum.filter(fn %{"tags" => tags} -> config[:tag_name] in tags end)
        |> extract_nodes(config[:node_basename], config[:ip_addr_type])

      {:ok, {{_version, code, status}, headers, body}} ->
        warn(
          topology,
          """
          cannot query Scaleway API (#{code} #{status})
          headers: #{inspect(headers)}
          body: #{inspect(body)}
          """
        )

        []

      {:error, reason} ->
        error(topology, "request to Scaleway API failed!: #{inspect(reason)}")
        []
    end
  end

  @doc false
  def follow_link(base_url, path, req_headers, acc \\ []) do
    case :httpc.request(:get, {'#{base_url}/#{path}', req_headers}, [], []) do
      {:ok, {{_version, 200, _status}, resp_headers, body}} ->
        resp = Jason.decode!(body)
        acc = [resp | acc]

        if next_path = extract_next_path(resp_headers) do
          follow_link(base_url, next_path, req_headers, acc)
        else
          {:ok, acc}
        end

      other ->
        other
    end
  end

  @doc false
  def extract_next_path(headers) do
    if link = :proplists.get_value('link', headers, nil) do
      link
      |> to_string()
      |> Linkex.decode()
      |> case do
        {:ok, %Linkex.LinkHeader{next: %Linkex.Entry{target: %URI{path: path, query: query}}}} ->
          "#{path}?#{query}"

        _other ->
          nil
      end
    end
  end

  # TODO
  @doc false
  def extract_nodes(servers, app_name, {:public, :ipv4}) do
    Enum.map(servers, fn %{"public_ip" => %{"address" => public_ip_address}} ->
      :"#{app_name}@#{public_ip_address}"
    end)
  end

  def extract_nodes(servers, app_name, {:private, :ipv4}) do
    Enum.map(servers, fn %{"private_ip" => %{"address" => private_ip_address}} ->
      :"#{app_name}@#{private_ip_address}"
    end)
  end

  def extract_nodes(_servers, _app_name, {:public, :ipv6}) do
  end
end
