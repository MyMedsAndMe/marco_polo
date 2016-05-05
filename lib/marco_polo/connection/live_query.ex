defmodule MarcoPolo.Connection.LiveQuery do
  @moduledoc false

  alias MarcoPolo.Document

  def extract_token(%{response: [%Document{fields: %{"token" => token}}]}) do
    token
  end

  def forward_live_query_data({token, :unsubscribed}, s) do
    send_live_query_data_resp(s, token, :unsubscribed)
    update_in(s.live_query_tokens, &Dict.delete(&1, token))
  end

  def forward_live_query_data({token, resp}, s) do
    send_live_query_data_resp(s, token, resp)
    s
  end

  defp send_live_query_data_resp(%{live_query_tokens: tokens}, token, resp) do
    receiver = Dict.fetch!(tokens, token)
    send receiver, {:orientdb_live_query, token, resp}
  end
end
