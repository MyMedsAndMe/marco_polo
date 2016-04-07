defmodule MarcoPolo.REST do
  @moduledoc """
  This module provides an interface to the functionalities exposed by OrientDB's
  REST API.

  Not all the functionalities that OrientDB exposes are available through the
  binary protocol that `MarcoPolo` uses: some of them are only available through
  the HTTP REST API. These functionalities are exposed by this module.

  The functions in this module are "stateless", so they can be just run without
  any connection setup (e.g., like what needs to be done with
  `MarcoPolo.start_link/1` for the binary protocol).

  ## Options

  The following is a list of options that functions in this module accept;
  they're used to handle the connection (e.g., where to connect to, what's the
  user and the password, and so on).

    * `:host` - (binary) the host where the OrientDB server is running. Defaults
      to `"localhost"`.
    * `:port` - (integer) the port where the OrientDB server is exposing the
      HTTP REST API. Defaults to `2480`.
    * `:user` - (binary) the user to use. Mandatory.
    * `:password` - (binary) the password to use. Mandatory.
    * `:scheme` - (binary) the scheme to use to connect to the server. Defaults
      to `"http"`.

  """

  require Logger

  @default_headers [{"Accept-Encoding", "gzip,deflate"}]

  @doc """
  Imports the database dumped at `db_path` into `destination_database`.

  This function takes the path to a JSON dump of a database (`db_path`), and
  imports that dump to the `destination_database`. `opts` is a list of options
  used for connecting to the REST API of the OrientDB server. You can read more
  about these options in the documentation for this module.

  Returns just `:ok` if the import is successful, `{:error, reason}` if anything
  goes wrong (e.g., the connection to the OrientDB server is unsuccessful or the
  database cannot be imported).

  ## Examples

      opts = [user: "root", password: "root"]
      MarcoPolo.REST.import("DestDb", "/path/to/db.json", opts)
      #=> :ok

  """
  @spec import(binary, Path.t, Keyword.t) :: :ok | {:error, term}
  def import(destination_database, db_path, opts)
      when is_binary(destination_database) and is_binary(db_path) and is_list(opts) do
    url = URI.to_string(%URI{
      scheme: Keyword.get(opts, :scheme, "http"),
      host: Keyword.get(opts, :host, "localhost"),
      port: Keyword.get(opts, :port, 2480),
      path: "/import/#{destination_database}",
    })

    headers = [{"Content-Type", "application/json"} | @default_headers]
    body = File.read!(db_path)
    options = [
      basic_auth: {Keyword.fetch!(opts, :user), Keyword.fetch!(opts, :password)},
    ]

    case :hackney.request(:post, url, headers, body, options) do
      {:ok, 200, _headers, body_ref} ->
        :hackney.skip_body(body_ref)
        :ok
      {:ok, status, _headers, body_ref} ->
        {:ok, body} = :hackney.body(body_ref)
        {:error, "error while importing (status #{status}): #{body}"}
      {:error, _} = error ->
        error
    end
  end
end
