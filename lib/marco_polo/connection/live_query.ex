defmodule MarcoPolo.Connection.LiveQuery do
  @moduledoc false

  alias MarcoPolo.Protocol
  alias MarcoPolo.Document

  @doc """
  Extracts the Live Query token from a response to a `REQUEST_COMMAND`.
  """
  @spec extract_token(%{response: [Document.t]}) :: integer
  def extract_token(%{response: [%Document{fields: %{"token" => token}}]}) do
    token
  end

  @doc """
  Parses the push data in `data` and forwards the result to the receiver.

  The receiver is found based on the token in `data` in the `:live_query_tokens`
  field of the state `s`.
  """
  @spec forward_live_query_data(binary, MarcoPolo.Connection.state) ::
    MarcoPolo.Connection.state
  def forward_live_query_data(data, s) when is_binary(data) do
    case Protocol.parse_push_data(data, s.schema) do
      :incomplete ->
        %{s | tail: data}
      {:ok, {token, resp}, rest} ->
        send_live_query_data_resp(s, token, resp)
        %{s | tail: rest}
    end
  end

  defp send_live_query_data_resp(%{live_query_tokens: tokens}, token, resp) do
    receiver = Dict.fetch!(tokens, token)
    send receiver, {:orientdb_live_query, token, resp}
  end
end
