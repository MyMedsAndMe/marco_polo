# MarcoPolo

Marco Polo is a binary OrientDB driver for Elixir.

Documentation is available at [http://hexdocs.pm/marco_polo][docs].

## Usage

Add MarcoPolo as a dependency of your application inside your `mix.exs` file:

```elixir
def deps do
  [{:marco_polo, "~> 0.1"}]
end
```

Now run `mix deps.get` in your shell to fetch and compile MarcoPolo. To play with MarcoPolo, run `iex -S mix` in your project:

```elixir
{:ok, conn} = MarcoPolo.start_link(user: "admin",
                                   password: "admin",
                                   connection: {:db, "GratefulDeadConcerts", :document})

MarcoPolo.command(conn, "CREATE CLASS ProgrammingLanguage")
#=> {:ok, 15}

MarcoPolo.command(conn, "INSERT INTO ProgrammingLanguage(name) VALUES ('Elixir')")
#=> {:ok, {%MarcoPolo.RID{cluster_id: 15, position: 0}, 1}}

MarcoPolo.command(conn, "SELECT FROM ProgrammingLanguage WHERE name = 'Elixir'")
#=> {:ok, [%MarcoPolo.Document{class: "ProgrammingLanguage", version: 1, fields: %{"name" => "Elixir"}}]}
```

## Types

Some OrientDB types are "more specific" than their Elixir counterparts. For
example, OrientDB can represent integers as ints (4 bytes), longs (8 bytes), or
shorts (2 bytes). Elixir only knows about integers. For this reason, the OrientDB type can be forced (similarly to a type cast) from Elixir. For example, Elixir integers are encoded as ints (4 bytes) by default, but they can be forced to be encoded as shorts or longs by using a *tagged tuple*:

```elixir
332          #=> gets encoded with 4 bytes as the int 332
{:long, 332} #=> gets encoded with 8 bytes as the long 332
```

The same can be done for other types as well.

The following table shows how Elixir types map to OrientDB types and viceversa. The t


| Elixir                                                                                 | Encoded as OrientDB type | Decoded in Elixir as                             |
| :------                                                                                | :---------               | :-----------                                     |
| `true`, `false`                                                                        | boolean                  | same as original                                 |
| `83` or `{:int, 83}`                                                                   | integer                  | `83`                                             |
| `{:short, 21}`                                                                         | short                    | `21`                                             |
| `{:long, 944}`                                                                         | long                     | `944`                                            |
| `{:float, 3.14}`                                                                       | float                    | `3.14`                                           |
| `2.71` or `{:double, 2.71}`                                                            | double                   | `2.71`                                           |
| `Decimal.new(3.14)` (using [Decimal][decimal])                                         | decimal                  | same as original                                 |
| `"foo"`, `<<1, 2, 3>>`                                                                 | string                   | `"foo"`, `<<1, 2, 3>>`                           |
| `{:binary, <<7, 2>>}`                                                                  | binary                   | `<<7, 2>>`                                       |
| `%MarcoPolo.Date{year: 2015 month: 7, day: 1}`                                         | date                     | same as original                                 |
| `%MarcoPolo.DateTime{year: 2015 month: 7, day: 1, hour: 0, min: 37, sec: 14, msec: 0}` | datetime                 | same as original                                 |
| `%MarcoPolo.Document{}`                                                                | embedded                 | same as original                                 |
| `[1, "foo", {:float, 3.14}]`                                                           | embedded list            | `[1, "foo", 3.14]`                               |
| `#HashSet<[2, 1]>`                                                                     | embedded set             | `#HashSet<[2, 1]>`                               |
| `%{"foo" => true}`                                                                     | embedded map             | `%{"foo" => true}`                               |
| `%MarcoPolo.RID{cluster_id: 21, position: 3}`                                          | link                     | `%MarcoPolo.RID{cluster_id: 21, position: 3}`    |
| `{:link_list, [%MarcoPolo.RID{}, ...]}`                                                | link list                | `{:link_list, [%MarcoPolo.RID{}, ...]}`          |
| `{:link_set, #HashSet<%MarcoPolo.RID{}, ...>}`                                         | link set                 | `{:link_set, #HashSet<%MarcoPolo.RID{}, ...>}`   |
| `{:link_map, %{"foo" => %MarcoPolo.RID{}, ...}}`                                       | link set                 | `{:link_map, %{"foo" => %MarcoPolo.RID{}, ...}}` |


Caveats:

* embedded maps and link maps only support strings as keys. During encoding,
  MarcoPolo tries to convert all keys to strings using the `to_string/1`
  function and the information about the original type is lost (so that at
  decoding, all map keys are strings).
* encoding and decoding of RidBags is described in the "RidBags" section below.

### RidBags

As of version 0.1, MarcoPolo doesn't support tree RidBags. It only supports embedded RidBags.

Embedded RidBags are represented in Elixir like this:

```elixir
{:link_bag, [%MarcoPolo.RID, ...]}
```

Tree-based RidBags will likely be supported in the upcoming versions. In the
meantime, if you need to, you can configure the OrientDB server so that it uses
only embedded RidBags (up to a number of links). To do this, set the value of
the `ridBag.embeddedToSbtreeBonsaiThreshold` option in the server's XML config
to a very high value (e.g. 1 billion), so that embedded RidBags will be used up
to that number of links. For example:


```xml
<properties>
  ...
  <entry name="ridBag.embeddedToSbtreeBonsaiThreshold" value="1000000000" />
</properties>
```

## Contributing

For more information on how to contribute to MarcoPolo (including how to clone
the repository and run tests), have a look at the
[CONTRIBUTING](CONTRIBUTING.md) file.

[docs]: http://hexdocs.pm/marco_polo
[decimal]: https://github.com/ericmj/decimal
