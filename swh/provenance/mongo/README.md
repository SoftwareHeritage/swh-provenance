mongo backend
=============

Provenance storage implementation using MongoDB

initial data-model
------------------

```json
content
{
    id: sha1
    ts: int // optional
    revision: {<ref revision str>: [<ref path>]}
    directory: {<ref directory str>: [<ref path>]}
}

directory
{
    id: sha1
    ts: int  // optional
    revision: {<ref revision str>: [<ref path>]}
}

revision
{
    id: sha1
    ts: int // optional
    preferred  <ref origin>  // optional
    origin  [<ref origin>]
    revision [<ref revisions>]
}

origin
{
    id: sha1
    url: str
}

path
{
    path: str
}
```
