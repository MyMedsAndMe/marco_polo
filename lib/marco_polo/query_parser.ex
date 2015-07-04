defmodule MarcoPolo.QueryParser do
  @moduledoc """
  Facilities for parsing OrientDB queries.
  """

  @doc """
  Returns the query type of the given `query`.

  The possible query types are:

    * `:sql_query`: an idempotent SQL query like `SELECT`; it's the equivalent
      of `com.orientechnologies.orient.core.sql.query.OSQLSynchQuery`.
    * `:sql_command`: a non-idempotent command (`INSERT`, `UPDATE`, etc.); it's
      the equivalent of `com.orientechnologies.orient.core.sql.OCommandSQL`.

  """
  @spec query_type(binary) :: :sql_query | :sql_command
  def query_type(query) do
    case parse(query) do
      "select" -> :sql_query
      _        -> :sql_command
    end
  end

  defp parse(query) do
    regex               = ~r/^\s*(?<cmd>\w+)/
    %{"cmd" => command} = Regex.named_captures(regex, query)

    String.downcase(command)
  end
end
