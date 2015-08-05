# Contributing to MarcoPolo

First of all, thanks for willing to contribute! :heart:

You can contribute by contributing to the code or by opening an issue on
GitHub. Feel free to open issues if you find anything wrong with the code (e.g.,
bugs!) or with the documentation (e.g., errors in the documentation or unclear
parts of it).

If you to contribute to the code or the documentation, first clone the
repository, then create a new branch and commit your work in it (add tests if
you add features!), and then open a pull request on this repository. Be sure all
tests pass before submitting a pull request. Have a look at the "Testing
locally" section below for more information on running tests.

## Testing locally

By default, running `mix test` runs both unit and integrations tests. For this
reason, you need an OrientDB server running on `localhost` (with the binary
inferface listening on port `2424`, which is the default one for OrientDB).

The test setup (which happens in `test/test_helper.exs` also relies on the
`orientdb-console` command, so make sure such command is available in your
`$PATH`.

After you have the server running, you can run the test suite:

    $ mix test

All the tests will be run inside a `MarcoPoloTest` database on the server, so be
sure not to save any important data on it as it will be overwritten.

### Environment variables

The MarcoPolo tests rely on some environment variables to be set in order to
work properly. For example, the OrientDB user and password used to connect to
the server are read from the `$ORIENTDB_USER` and `$ORIENTDB_PASS` environment
variables.

To make working with environment variables easier, MarcoPolo uses
[dotenv for Elixir][dotenv_elixir], a library that picks up environment
variables from a `.env` file in the root of the project. This library is only
used in the `test` Mix environment. You can copy [`.env.example`](.env.example)
to `.env` (which is not version-controlled) and mofify the values of the
variables in it.

### Scripting

OrientDB does not allow server-side scripting by default, so scripring related
tests (tagged in ExUnit with the `:scripting` tag) are not run by default. If
you enable server-side scripting (for at least SQL, Javascript and Groovy) in
the OrienDB server's XML configuration, you can include the `:scripting` tag
when running `mix test`:

    $ mix test --include scripting


[dotenv_elixir]: https://github.com/avdi/dotenv_elixir
