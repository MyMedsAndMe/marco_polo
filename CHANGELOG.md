# Changelog

## v0.2.1

* Fixed a nasty bug in the serialized record parser. It should all be
  transparent to the users.

## v0.2.0

#### Features and improvements

* Support for [Live Query](https://orientdb.com/docs/last/Live-Query.html) (a
  feature introduced in OrientDB 2.1) through `MarcoPolo.live_query/4` and
  `MarcoPolo.live_query_unsubscribe/2`

#### Breaking changes

* `MarcoPolo.transaction/2` will now raise a `MarcoPolo.Error` if records passed
  in `:update` or `:delete` operations have a `nil` `:version` field

## v0.1.0

Initial release.
