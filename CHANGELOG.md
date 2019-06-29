## 2.0.2
* `legacy_fields` setting will make handlers ignore certain fields that are not available anymore.
    Works with: `filter`, `sort`, `group`, `join`, `joinf`, `aggregate`.
* `method_decorator` has had a few improvements that no one would notice

## 2.0.0
* Version 2.0 is released!
* Complete redesign
* Query Object format is the same: backwards-compatible
* `outerjoin` is renamed to `join`, old buggy `join` is now `joinf`
* `join` is not handled by a tweaked `selectin` loader, which is a lot easier and faster!
* Overall 1.5x-2.5x performance improvement
* `MongoQuery` settings lets you configure everything
* `StrictCrudHelper` is much more powerful
* `@saves_relations` helps with saving related entities
* `MongoQuery.end_count()` counts and selects at the same time
