> :warning: **THIS REPOSITORY IS NO LONGER MAINTAINED. IF YOU WOULD LIKE TO VOLUNTEER TO BE THE MAINTAINER, PLEASE CONTACT US** :warning:

# MarcoPolo [![Build Status](https://travis-ci.org/MyMedsAndMe/marco_polo.svg?branch=master)](https://travis-ci.org/MyMedsAndMe/marco_polo)

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
                                   connection: {:db, "GratefulDeadConcerts"})

{:ok, %{response: cluster_id}} = MarcoPolo.command(conn, "CREATE CLASS ProgrammingLanguage")
cluster_id #=> 15

query = "INSERT INTO ProgrammingLanguage(name) VALUES (?)"
{:ok, %{response: doc}} = MarcoPolo.command(conn, query, params: ["Elixir"])
doc.rid     #=> #MarcoPolo.RID<#15:0>
doc.version #=> 1
doc.fields  #=> %{"name" => "Elixir"}

query = "SELECT FROM ProgrammingLanguage WHERE name = :name"
{:ok, %{response: [language]}} = MarcoPolo.command(conn, query, params: %{name: "Elixir"})
language == doc #=> true
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

As of version 0.1, MarcoPolo doesn't support tree RidBags. It only supports
embedded RidBags.

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

## Fetch plans

MarcoPolo supports OrientDB [fetch plans][odb-fetching-strategies]. Starting with these data:

```elixir
{:ok, conn} = MarcoPolo.start_link(user: "root",
                                   password: "root",
                                   connection: {:db, "GratefulDeadConcerts"})

{:ok, %{response: country}} = MarcoPolo.command(conn, "INSERT INTO Country(name) VALUES ('USA')")

query = "INSERT INTO City(name, country) VALUES ('New York', ?)"
{:ok, %{response: city}} = MarcoPolo.command(conn, query, params: [country.rid])

query = "INSERT INTO Street(name, city) VALUES ('5th avenue', ?)"
{:ok, %{response: street}} = MarcoPolo.command(conn, query, params: [city.rid])
```

we can fetch the city and the country when we fetch the street:

```elixir
query = "SELECT FROM Street WHERE name = '5th avenue'"
{:ok, %{response: [street], linked_records: linked}} = MarcoPolo.command(conn, query, fetch_plan: "*:-1")

ny = MarcoPolo.FetchPlan.resolve_links!(street.fields["city"], linked)
usa = MarcoPolo.FetchPlan.resolve_links!(ny.fields["country"], linked)
```

## Working with graphs

MarcoPolo supports working with graphs using the same `MarcoPolo.command/3`
function showed above:

```elixir
{:ok, conn} = MarcoPolo.start_link(user: "root",
                                   password: "root",
                                   connection: {:db, "GratefulDeadConcerts"})

query = "CREATE VERTEX V SET name = 'Pizza place'"
{:ok, %{response: pizza_place}} = MarcoPolo.command(conn, query)
query = "CREATE VERTEX V SET name = 'Jane'"
{:ok, %{response: jane}} = MarcoPolo.command(conn, query)

query = "CREATE EDGE HasEatenIn FROM ? to ?"
params = [jane.rid, pizza_place.rid]
{:ok, %{response: edge}} = MarcoPolo.command(conn, query, params: params)

edge.fields["in"]  #=> {:link_list, [pizza_place.rid]}
edge.fields["out"] #=> {:link_list, [jane.rid]}
```

The `:graph` atom in the `MarcoPolo.start_link/1` function shuld reflect how the
database was created. If it was created as a graph database, then we use
`:graph`, otherwise we use `:document`. The differences between graph and
document databases are differences in the implementation on the server side; the
API is exactly the same between the two types.

## Scripting

OrientDB supports server-side scripting (for example,
[JavaScript][odb-javascript] and [SQL-batch][odb-sql-batch]). MarcoPolo supports
this feature through the `MarcoPolo.script/4` function:

```elixir
{:ok, conn} = MarcoPolo.start_link(user: "root",
                                   password: "root",
                                   connection: {:db, "GratefulDeadConcerts"})

{:ok, _} = MarcoPolo.script(conn, "Javascript", """
db.command('CREATE CLASS Number);

for (var i = 1; i <= 10; i++) {
  db.command('INSERT INTO Number(value) VALUES (' + i + ')');
}
""")
```

## Transactions

OrientDB supports server-side transactions, meaning transactions that happen
only on the server. The clients sends all the operations it wants to perform in
the transactions, and the server either performs them all atomically or reverts
all of them if there's an error in one of them. To perform a transaction in
MarcoPolo:

```elixir
{:ok, conn} = MarcoPolo.start_link(user: "root",
                                   password: "root",
                                   connection: {:db, "GratefulDeadConcerts"})

{:ok, resp} = MarcoPolo.transaction(conn, [
  {:create, %MarcoPolo.Document{class: "Foo", fields: %{"foo" => "bar"}}},
  {:delete, %MarcoPolo.Document{rid: %MarcoPolo.RID{cluster_id: 10, position: 39}}},
])

resp.created
#=> %MarcoPolo.Document{class: "Foo", fields: %{"foo" => "bar"}, rid: %MarcoPolo.RID{...}}

resp.updated
#=> []
```

To perform transactions with manual rollback (similar to the ones in most
relational databases), you have to use server-side scripting. For example, you
can perform a transaction by using a SQL script:

```elixir
{:ok, conn} = MarcoPolo.start_link(user: "root",
                                   password: "root",
                                   connection: {:db, "GratefulDeadConcerts"})

script = """
begin
let account = create vertex Account set name = 'Luke'
let city = select from City where name = 'London' lock record
let edge = create edge Lives from $account to $city
commit
return $edge
"""

MarcoPolo.script(conn, "SQL", script)
```

## Live query

**Note**: this is an *experimental feature in OrientDB*, and thus subject to
frequent changes. It should be considered experimental in MarcoPolo as well.

[Live Queries][odb-live-query] are OrientDB's take on PubSub. A client starts
"watching" a given query, and OrientDB sends messages to that client every time
something happens that changes the result of the query. For example, a `LIVE
SELECT FROM Person` query subscribes to the `SELECT FROM Person` query. If a
client adds record to `Person`, then all clients subscribed to that query get a
message that says a new `Person` has been created. Subscriptions are identified
by tokens.

All of this is pretty straightforward in MarcoPolo:

```elixir
{:ok, conn} = MarcoPolo.start_link(user: "root",
                                   password: "root",
                                   connection: {:db, "GratefulDeadConcerts"})

# Let's keep the token around so that we can unsubscribe later
{:ok, token} = MarcoPolo.live_query(conn, "LIVE SELECT FROM Person", self())

# The third argument to live_query/3 is the pid that will receive messages from
# the live query
MarcoPolo.command(conn, "INSERT INTO Person(name) VALUES ('Olivia Dunham')")
receive do msg -> msg end
#=> {:orientdb_live_query, token, {:create, %MarcoPolo.Document{class: "Person", ...}}}

MarcoPolo.command(conn, "UPDATE Person SET name = 'Fauxlivia Dunham' WHERE name = 'Olivia Dunham'")
receive do msg -> msg end
#=> {:orientdb_live_query, token, {:update, %MarcoPolo.Document{class: "Person", ...}}}

# Ok, enough with Fringe references
:ok = MarcoPolo.live_query_unsubscribe(conn, token)
```

## Contributing

For more information on how to contribute to MarcoPolo (including how to clone
the repository and run tests), have a look at the
[CONTRIBUTING](CONTRIBUTING.md) file.

## License

See the [LICENSE](LICENSE) file.


[docs]: http://hexdocs.pm/marco_polo
[decimal]: https://github.com/ericmj/decimal
[odb-javascript]: http://orientdb.com/docs/last/Javascript-Command.html
[odb-sql-batch]: http://orientdb.com/docs/last/SQL-batch.html
[odb-fetching-strategies]: http://orientdb.com/docs/last/Fetching-Strategies.html
[odb-live-query]: https://orientdb.com/docs/last/Live-Query.html
